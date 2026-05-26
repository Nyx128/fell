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

    // on_rotate scans the dir to find the newly sealed segment (any .log != new_stem
    // not yet tracked). O(segments), called at most once per 64 MB written.
    writer_ = std::make_unique<SegmentWriter>(
        partition_dir, active_base, [this, partition_dir](uint64_t new_base) {
          char buf[32];
          snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(new_base));
          const std::string new_stem = buf;

          std::lock_guard<std::mutex> lk(mu_);
          for (const auto &entry : std::filesystem::directory_iterator(partition_dir)) {
            if (entry.path().extension() != ".log") continue;
            if (entry.path().stem().string() == new_stem) continue;

            uint64_t base = std::stoull(entry.path().stem().string());
            bool already_sealed = std::any_of(
                sealed_segments_.begin(), sealed_segments_.end(),
                [base](const SegmentMeta &m) { return m.base_offset == base; });

            if (!already_sealed) {
              auto pos = std::lower_bound(
                  sealed_segments_.begin(), sealed_segments_.end(), base,
                  [](const SegmentMeta &m, uint64_t v) { return m.base_offset < v; });
              sealed_segments_.insert(pos, {base, entry.path()});
            }
          }
        });
  }

  uint64_t PartitionStore::append(const uint8_t *payload, uint32_t size) {
    auto ts = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    std::unique_lock<std::mutex> lk(mu_);
    return writer_->append(ts, payload, size);
  }

  std::filesystem::path PartitionStore::find_segment_for(uint64_t offset) const {
    // Binary search for the last sealed segment with base_offset <= offset.
    auto it = std::upper_bound(
        sealed_segments_.begin(), sealed_segments_.end(), offset,
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
    std::unique_lock<std::mutex> lk(mu_);
    const auto seg_path = find_segment_for(offset);
    lk.unlock(); // release before disk I/O — sealed segments are immutable

    if (seg_path.empty())
      return {};

    auto idx_path = seg_path;
    idx_path.replace_extension(".idx");

    const uint64_t file_pos = OffsetIndex(idx_path).lookup(offset);
    auto disk_records = SegmentReader(seg_path).read(file_pos, offset, max_count);

    std::vector<StoredMessage> results;
    results.reserve(disk_records.size());
    for (auto &r : disk_records)
      results.push_back({r.offset, r.timestamp_ms, std::move(r.payload)});
    return results;
  }

  uint64_t PartitionStore::next_offset() const {
    std::unique_lock<std::mutex> lk(mu_);
    return writer_->next_offset();
  }

} // namespace fell::storage
