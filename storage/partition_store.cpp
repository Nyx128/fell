#include "storage/partition_store.hpp"
#include "include/storage/storage_options.hpp"
#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_recovery.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>

namespace fell::storage {

  PartitionStore::PartitionStore(const std::filesystem::path &partition_dir, StorageOptions options)
      : dir_(partition_dir), options_(options) {
    std::filesystem::create_directories(partition_dir);

    // Truncate any partial write from an unclean shutdown before opening the writer.
    auto rr = recover_partition(partition_dir);

    std::vector<std::filesystem::path> log_paths;
    for (const auto &entry : std::filesystem::directory_iterator(partition_dir)) {
      if (entry.path().extension() == ".log") {
        log_paths.push_back(entry.path());
      }
    }
    std::sort(log_paths.begin(), log_paths.end());

    // All segments except the last are sealed and immutable.
    for (size_t i = 0; i + 1 < log_paths.size(); ++i) {
      uint64_t base = std::stoull(log_paths[i].stem().string());
      sealed_segments_.push_back({base, log_paths[i]});
    }

    uint64_t active_base = log_paths.empty() ? 0 : std::stoull(log_paths.back().stem().string());
    writer_ = std::make_unique<SegmentWriter>(partition_dir, active_base);

    if (!log_paths.empty()) {
      writer_->set_next_offset(rr.next_offset);
      writer_->set_bytes_written(rr.log_end_position);
    }

    // Initialize offset tracker states from the recovered writer tail
    next_offset_ = writer_->next_offset();
    committed_offset_.store(next_offset_, std::memory_order_release);

    // Start Partition I/O Thread
    last_flush_time_ = std::chrono::steady_clock::now();
    io_thread_ = std::thread(&PartitionStore::io_loop, this);
  }

  PartitionStore::~PartitionStore() {
    {
      std::lock_guard<std::mutex> lk(queue_mu_);
      shutdown_ = true;
    }
    queue_cv_.notify_all();
    if (io_thread_.joinable()) {
      io_thread_.join();
    }
  }

  uint64_t PartitionStore::get_cached_timestamp_locked() {
    // Cache timestamps in millisecond buckets to avoid a clock read per append.
    if (ts_counter_ == 0) {
      last_timestamp_ms_ =
          static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count());
    }

    const uint64_t ts = last_timestamp_ms_;
    if (++ts_counter_ >= kTimestampRefreshEvery) {
      ts_counter_ = 0;
    }
    return ts;
  }

  AppendResult PartitionStore::append(const uint8_t *payload, uint32_t size) {
    AppendCommand cmd;
    cmd.payload.assign(payload, payload + size);

    std::unique_lock<std::mutex> lk(queue_mu_);

    if (shutdown_) {
      return AppendResult::closed();
    }

    // Check bounded queue backpressure limits
    if (queue_.size() >= options_.queue_capacity ||
        pending_bytes_ + size > options_.max_pending_bytes) {
      busy_count_.fetch_add(1, std::memory_order_relaxed);
      return AppendResult::busy();
    }

    // Protect clock VDSO overhead using our coarse timestamp cache
    cmd.timestamp_ms = get_cached_timestamp_locked();

    cmd.offset = next_offset_++;
    pending_bytes_ += cmd.payload.size();

    queue_.push_back(std::move(cmd));
    lk.unlock();

    queue_cv_.notify_one();
    return AppendResult::ok(cmd.offset);
  }

  std::filesystem::path PartitionStore::find_segment_for(uint64_t offset) const {
    auto it =
        std::upper_bound(sealed_segments_.begin(), sealed_segments_.end(), offset,
                         [](uint64_t val, const SegmentMeta &m) { return val < m.base_offset; });

    if (it != sealed_segments_.begin()) {
      return (--it)->path;
    }

    if (offset >= writer_->base_offset()) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%020llu",
               static_cast<unsigned long long>(writer_->base_offset()));
      return dir_ / (std::string(buf) + ".log");
    }

    return {};
  }

  std::vector<StoredMessage> PartitionStore::fetch(uint64_t offset, uint16_t max_count) const {
    std::shared_ptr<OffsetIndex> idx;
    std::shared_ptr<SegmentReader> rdr;

    {
      std::unique_lock<DebugMutex> lk(mu_);
      const auto seg_path = find_segment_for(offset);
      if (seg_path.empty()) {
        return {};
      }

      const std::string key = seg_path.string();
      auto &cache = segment_cache_[key];

      if (!cache.index) {
        auto idx_path = seg_path;
        idx_path.replace_extension(".idx");
        cache.index = std::make_shared<OffsetIndex>(idx_path);
      }

      if (!cache.reader) {
        cache.reader = std::make_shared<SegmentReader>(seg_path);
      }

      idx = cache.index;
      rdr = cache.reader;
    }

    const uint64_t file_pos = idx->lookup(offset);
    auto disk_records = rdr->read(file_pos, offset, max_count);

    std::vector<StoredMessage> results;
    results.reserve(disk_records.size());
    for (auto &r : disk_records) {
      results.push_back({r.offset, r.timestamp_ms, std::move(r.payload)});
    }
    return results;
  }

  uint64_t PartitionStore::next_offset() const {
    std::unique_lock<DebugMutex> lk(mu_);
    return writer_->next_offset();
  }

  std::vector<AppendCommand> PartitionStore::take_batch() {
    std::vector<AppendCommand> batch;
    size_t batch_bytes = 0;

    std::unique_lock<std::mutex> lk(queue_mu_);

    // Wait until there's something in the queue or we are shutting down
    queue_cv_.wait(lk, [&] { return shutdown_ || !queue_.empty(); });

    // If we've shut down and processed everything, exit
    if (shutdown_ && queue_.empty()) {
      return batch;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::microseconds(options_.batch_wait_us);

    while (!queue_.empty()) {
      AppendCommand cmd = std::move(queue_.front());
      queue_.pop_front();
      pending_bytes_ -= cmd.payload.size();

      batch_bytes += LOG_RECORD_HEADER_SIZE + cmd.payload.size();
      batch.push_back(std::move(cmd));

      // Check if batch limits are met
      if (batch.size() >= options_.max_batch_records || batch_bytes >= options_.max_batch_bytes) {
        break;
      }

      // Check if time deadline is exceeded
      if (std::chrono::steady_clock::now() >= deadline) {
        break;
      }

      // If queue is empty, do a short wait_until to gather any fast-following publishes
      if (queue_.empty()) {
        queue_cv_.wait_until(lk, deadline, [&] { return shutdown_ || !queue_.empty(); });
      }
    }

    return batch;
  }

  void PartitionStore::write_batch(const std::vector<AppendCommand> &batch) {
    if (batch.empty())
      return;

    size_t total_batch_bytes = 0;
    for (const auto &cmd : batch) {
      total_batch_bytes += LOG_RECORD_HEADER_SIZE + cmd.payload.size();
    }

    std::vector<uint8_t> buffer;
    buffer.reserve(total_batch_bytes);

    uint64_t current_file_pos = writer_->bytes_written();

    for (const auto &cmd : batch) {
      LogRecordHeader header{
          platform::host_to_be64(cmd.offset),
          platform::host_to_be64(cmd.timestamp_ms),
          platform::host_to_be32(static_cast<uint32_t>(cmd.payload.size())),
      };

      // If sparse index entry is due (every INDEX_INTERVAL messages)
      if ((cmd.offset - writer_->base_offset()) % INDEX_INTERVAL == 0) {
        IndexEntry entry{
            platform::host_to_be64(cmd.offset),
            platform::host_to_be64(current_file_pos),
        };
        platform::IOBuffer idx_buf{&entry, sizeof(entry)};
        if (!platform::write_file_vec(writer_->idx_fd(), &idx_buf, 1)) {
          throw std::system_error(errno, std::system_category(), "Failed to write index entry");
        }
      }

      // Append header and payload to batch buffer
      const uint8_t *h_ptr = reinterpret_cast<const uint8_t *>(&header);
      buffer.insert(buffer.end(), h_ptr, h_ptr + LOG_RECORD_HEADER_SIZE);
      buffer.insert(buffer.end(), cmd.payload.begin(), cmd.payload.end());

      current_file_pos += LOG_RECORD_HEADER_SIZE + cmd.payload.size();
    }

    // Write full batch buffer to active log segment
    platform::IOBuffer log_buf{buffer.data(), buffer.size()};
    if (!platform::write_file_vec(writer_->log_fd(), &log_buf, 1)) {
      throw std::system_error(errno, std::system_category(), "Failed to write batch to log file");
    }

    // Update SegmentWriter internal tracking
    writer_->add_bytes_written(buffer.size());
    writer_->advance_next_offset(batch.size());

    // Advance committed offset release semantics so Fetch threads see it
    const uint64_t last_offset = batch.back().offset;
    committed_offset_.store(last_offset + 1, std::memory_order_release);
  }

  void PartitionStore::io_loop() {
    while (true) {
      std::vector<AppendCommand> batch = take_batch();

      if (batch.empty()) {
        if (shutdown_)
          break;
        continue;
      }

      write_batch(batch);

      // 1. Rotation policy (happens on batch boundaries)
      if (writer_->bytes_written() >= LOG_SEGMENT_MAX_BYTES) {
        const uint64_t old_base = writer_->base_offset();
        writer_->flush();

        char buf[32];
        snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(old_base));
        auto old_log_path = dir_ / (std::string(buf) + ".log");

        // Publish sealed segment metadata to readable fetches
        {
          std::unique_lock<DebugMutex> lk(mu_);
          sealed_segments_.push_back({old_base, old_log_path});
          segment_cache_.erase(old_log_path.string());
        }

        // Open new active segment starting at next offset.
        // C++ RAII: writer_.reset() automatically flushes and closes old file handles.
        const uint64_t new_base = writer_->next_offset();
        writer_ = std::make_unique<SegmentWriter>(dir_, new_base);

        // Reset flush counts on rotation
        records_since_flush_ = 0;
        last_flush_time_ = std::chrono::steady_clock::now();
      } else {
        // 2. Flush policy checks
        records_since_flush_ += batch.size();

        const auto now = std::chrono::steady_clock::now();
        const auto elapsed_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(now - last_flush_time_).count();

        if (records_since_flush_ >= options_.flush_every_records ||
            elapsed_ms >= options_.flush_every_ms) {
          writer_->flush();
          records_since_flush_ = 0;
          last_flush_time_ = now;
        }
      }
    }

    // Final flush of active files on shutdown
    if (writer_) {
      writer_->flush();
    }
  }

} // namespace fell::storage
