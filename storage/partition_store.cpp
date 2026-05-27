#include "storage/partition_store.hpp"
#include "storage/log_recovery.hpp"
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>

namespace fell::storage {

  PartitionStore::PartitionStore(const std::filesystem::path &partition_dir) : dir_(partition_dir) {
    std::filesystem::create_directories(partition_dir);

    // Truncate any partial write from an unclean shutdown before opening the writer.
    recover_partition(partition_dir);

    std::vector<std::filesystem::path> log_paths;
    for (const auto &entry : std::filesystem::directory_iterator(partition_dir)) {
      if (entry.path().extension() == ".log")
        log_paths.push_back(entry.path());
    }
    std::sort(log_paths.begin(), log_paths.end());

    // All segments except the last are sealed and immutable.
    for (size_t i = 0; i + 1 < log_paths.size(); ++i) {
      uint64_t base = std::stoull(log_paths[i].stem().string());
      sealed_segments_.push_back({base, log_paths[i]});
    }

    uint64_t active_base = log_paths.empty() ? 0 : std::stoull(log_paths.back().stem().string());

    // No rotation callback — PartitionStore::append detects rotation by
    // comparing base_offset() before and after writer_->append().
    writer_ = std::make_unique<SegmentWriter>(partition_dir, active_base, nullptr);
  }

  uint64_t PartitionStore::append(const uint8_t *payload, uint32_t size) {
    // ── Bottleneck 2: Coarse timestamp ─────────────────────────────────────
    // Refresh system_clock::now() only every kTimestampRefreshEvery writes
    // instead of on every single append. Millisecond granularity makes this
    // safe: adjacent messages may share a timestamp but ordering is still
    // guaranteed by their offsets.
    uint64_t ts;
    if (ts_counter_ == 0) {
      ts = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count());
      last_timestamp_ms_ = ts;
    } else {
      ts = last_timestamp_ms_;
    }
    if (++ts_counter_ >= kTimestampRefreshEvery)
      ts_counter_ = 0;

    std::unique_lock<DebugMutex> lk(mu_);
    const uint64_t prev_base = writer_->base_offset();
    const uint64_t result    = writer_->append_no_flush(ts, payload, size);
    const uint64_t new_base  = writer_->base_offset();

    // If base changed, rotation just happened — add the sealed segment and
    // evict its cache entry (it will be re-built from the new file on read).
    if (new_base != prev_base) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(prev_base));
      auto sealed_path = dir_ / (std::string(buf) + ".log");
      auto pos =
          std::lower_bound(sealed_segments_.begin(), sealed_segments_.end(), prev_base,
                           [](const SegmentMeta &m, uint64_t v) { return m.base_offset < v; });
      if (pos == sealed_segments_.end() || pos->base_offset != prev_base)
        sealed_segments_.insert(pos, {prev_base, sealed_path});

      // Evict the cache for the segment that just rotated so the reader
      // and index objects are rebuilt from the finalized file on next fetch.
      segment_cache_.erase(sealed_path.string());
    }

    // ── Bottleneck 3: Flush gating ─────────────────────────────────────────
    // Check whether a flush is due while still holding the lock, then use an
    // atomic CAS to elect exactly one thread per flush window.  The elected
    // thread performs the slow fdatasync *outside* the lock so it never
    // blocks concurrent appends.
    bool need_flush = writer_->flush_due();
    // Volunteer to flush: only if no other thread already grabbed the job.
    bool i_will_flush = false;
    if (need_flush) {
      i_will_flush = !flush_in_flight_.exchange(true, std::memory_order_acq_rel);
    }
    lk.unlock();

    if (i_will_flush) {
      writer_->flush();
      flush_in_flight_.store(false, std::memory_order_release);
    }

    return result;
  }

  std::filesystem::path PartitionStore::find_segment_for(uint64_t offset) const {
    // Binary search for the last sealed segment with base_offset <= offset.
    auto it =
        std::upper_bound(sealed_segments_.begin(), sealed_segments_.end(), offset,
                         [](uint64_t val, const SegmentMeta &m) { return val < m.base_offset; });

    if (it != sealed_segments_.begin())
      return (--it)->path;

    if (offset >= writer_->base_offset()) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%020llu",
               static_cast<unsigned long long>(writer_->base_offset()));
      return dir_ / (std::string(buf) + ".log");
    }

    return {}; // offset predates all known segments
  }

  std::vector<StoredMessage> PartitionStore::fetch(uint64_t offset, uint16_t max_count) const {
    // ── Bottleneck 4 & 5: Cached OffsetIndex + SegmentReader ──────────────
    // We resolve the segment path and retrieve (or build) the per-segment
    // cache *under the lock*, then do all disk I/O *outside* the lock using
    // the shared_ptr copies.  This keeps the hot lock section O(1) (just a
    // hash-map lookup) while reads never block concurrent writes.
    std::shared_ptr<OffsetIndex>   idx;
    std::shared_ptr<SegmentReader> rdr;

    {
      std::unique_lock<DebugMutex> lk(mu_);
      const auto seg_path = find_segment_for(offset);
      if (seg_path.empty())
        return {};

      const std::string key = seg_path.string();
      auto &cache = segment_cache_[key];

      // Build index lazily — one full read of the .idx file, then cached.
      if (!cache.index) {
        auto idx_path = seg_path;
        idx_path.replace_extension(".idx");
        cache.index = std::make_shared<OffsetIndex>(idx_path);
      }

      // Keep the .log fd open across calls — SegmentReader holds it.
      if (!cache.reader) {
        cache.reader = std::make_shared<SegmentReader>(seg_path);
      }

      idx = cache.index;
      rdr = cache.reader;
    } // lock released here — disk I/O below holds no lock

    const uint64_t file_pos   = idx->lookup(offset);
    auto           disk_records = rdr->read(file_pos, offset, max_count);

    std::vector<StoredMessage> results;
    results.reserve(disk_records.size());
    for (auto &r : disk_records)
      results.push_back({r.offset, r.timestamp_ms, std::move(r.payload)});
    return results;
  }

  uint64_t PartitionStore::next_offset() const {
    std::unique_lock<DebugMutex> lk(mu_);
    return writer_->next_offset();
  }

} // namespace fell::storage
