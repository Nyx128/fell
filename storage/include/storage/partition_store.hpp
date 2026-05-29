#pragma once

#include "debug_mutex.hpp"
#include "log_format.hpp"
#include "offset_index.hpp"
#include "segment_reader.hpp"
#include "segment_writer.hpp"
#include "storage_options.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fell::storage {

  /**
   * @brief Struct representing a message retrieved from the storage subsystem.
   * 
   * Design Insight:
   * Mirrors broker/topic_registry.hpp's Message struct to prevent the storage package
   * from depending directly on broker internals.
   */
  struct StoredMessage {
    uint64_t offset;       ///< Assigned absolute offset
    uint64_t timestamp_ms; ///< Log ingest timestamp in milliseconds
    std::vector<uint8_t> payload; ///< Binary payload
  };

  /**
   * @brief Internal command queued for background I/O serialization.
   */
  struct AppendCommand {
    uint64_t offset = 0;
    uint64_t timestamp_ms = 0;
    std::vector<uint8_t> payload;
  };

  /**
   * @brief Reason for append rejection.
   */
  enum class AppendError {
    Busy,   ///< Storage bounded queue or memory limits exceeded (backpressure)
    Closed, ///< Partition store has shut down
  };

  /**
   * @brief Result of a fast append enqueue operation.
   */
  struct AppendResult {
    bool accepted = false;         ///< True if successfully enqueued
    uint64_t offset = 0;           ///< Assigned offset (monotonic, before write completion)
    AppendError error = AppendError::Busy;

    static AppendResult ok(uint64_t value) {
      return {.accepted = true, .offset = value};
    }

    static AppendResult busy() {
      return {.accepted = false, .error = AppendError::Busy};
    }

    static AppendResult closed() {
      return {.accepted = false, .error = AppendError::Closed};
    }
  };

  /**
   * @class PartitionStore
   * @brief Thread-safe async storage partition engine.
   * 
   * ### Architectural Overview
   * 1. **Fast Ingestion (Append Path)**:
   *    - Acquires immediate monotonic offsets.
   *    - Enqueues records to an internal bounded lock-free/low-lock deque.
   *    - Non-blocking unless the queue bounds (records or memory capacity) are reached, 
   *      which immediately triggers backpressure with `AppendResult::busy()`.
   * 
   * 2. **Async Commit (I/O Path)**:
   *    - A dedicated single background thread pulls batches from the queue.
   *    - Serializes multiple records in a single sequential I/O system call.
   *    - Releases `committed_offset_` atomically, making written records visible to consumers.
   *    - Manages file segment rotations and fsync flushing asynchronously.
   * 
   * 3. **Consistent Reads (Fetch Path)**:
   *    - Consumers query log segments up to `committed_offset_` only.
   *    - Protects readers from accessing uncommitted/dirty tail records.
   *    - Fast segment-index lookups bypass queue lock synchronization.
   * 
   * ### Concurrency Assumptions
   * - **Producers**: Multiple parallel writer threads allowed (`append()` is thread-safe).
   * - **Consumers**: Multiple parallel readers allowed (`fetch()` is thread-safe, locks metadata only on cache miss).
   * - **Log Recovery**: Reconstructed at startup. Partial logs from crashes are trimmed to the last clean checksum state.
   */
  class PartitionStore {
  public:
    /**
     * @brief Opens or recovers a partition store directory.
     * @param partition_dir Absolute directory path.
     * @param options Tuning limits for caching, batches, and backpressure.
     */
    explicit PartitionStore(const std::filesystem::path &partition_dir,
                            StorageOptions options = {});
    ~PartitionStore();

    // Disable copy
    PartitionStore(const PartitionStore &) = delete;
    PartitionStore &operator=(const PartitionStore &) = delete;

    /**
     * @brief Fast-path append. Enqueues the message to the background thread.
     * @return AppendResult containing the assigned monotonic offset or backpressure rejection.
     */
    AppendResult append(const uint8_t *payload, uint32_t size);

    /**
     * @brief Fetch up to max_count committed messages starting at offset.
     * @return List of committed messages (excludes dirty pending queue tail).
     */
    std::vector<StoredMessage> fetch(uint64_t offset, uint16_t max_count) const;

    /**
     * @brief Returns the next offset to be assigned.
     */
    uint64_t next_offset() const;

    /**
     * @brief Returns the highest successfully flushed and indexable offset.
     */
    uint64_t committed_offset() const {
      return committed_offset_.load(std::memory_order_acquire);
    }

  private:
    uint64_t get_cached_timestamp_locked();
    std::filesystem::path find_segment_for(uint64_t offset) const;
    void io_loop();
    std::vector<AppendCommand> take_batch();
    void write_batch(const std::vector<AppendCommand> &batch);

    struct SegmentCache {
      std::shared_ptr<OffsetIndex> index;
      std::shared_ptr<SegmentReader> reader;
    };

    mutable std::unordered_map<std::string, SegmentCache> segment_cache_;
    std::filesystem::path dir_;
    std::unique_ptr<SegmentWriter> writer_;
    mutable DebugMutex mu_; // Protects active and sealed segment metadata caches

    struct SegmentMeta {
      uint64_t base_offset;
      std::filesystem::path path;
    };

    std::vector<SegmentMeta> sealed_segments_;
    static constexpr uint32_t kTimestampRefreshEvery = 16;
    uint64_t last_timestamp_ms_ = 0;
    uint32_t ts_counter_ = 0;
    StorageOptions options_;

    // Bounded queue state
    std::deque<AppendCommand> queue_;
    size_t pending_bytes_ = 0;
    mutable std::mutex queue_mu_;
    std::condition_variable queue_cv_;

    // Worker thread state
    std::thread io_thread_;
    bool shutdown_ = false;

    // Offset tracking
    uint64_t next_offset_ = 0;
    std::atomic<uint64_t> committed_offset_{0};
    std::atomic<uint64_t> busy_count_{0};

    // Async flush tracking
    uint32_t records_since_flush_ = 0;
    std::chrono::steady_clock::time_point last_flush_time_;
  };

} // namespace fell::storage
