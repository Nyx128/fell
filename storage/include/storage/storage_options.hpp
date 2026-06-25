#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace fell::storage {

  /**
   * @struct StorageOptions
   * @brief System-wide tuning options for async batch storage ingestion and flushes.
   */
  struct StorageOptions {
    // --- Backpressure Limits (Memory & Queue Capacity) ---
    size_t max_pending_bytes = 64 * 1024 * 1024; ///< Max pending queue size in bytes (default 64MB)
    size_t queue_capacity = 65536; ///< Max pending commands queued (default 64k entries)

    // --- Batch Ingestion Limits ---
    size_t max_batch_bytes = 1024 * 1024; ///< Form batch if payload sizes cross this (default 1MB)
    size_t max_batch_records = 4096; ///< Form batch if record count crosses this (default 4096)
    uint32_t batch_wait_us =
        1000; ///< Queue latency/gather deadline window in microseconds (default 1ms)

    // --- Flushing / Durability Policies ---
    uint32_t flush_every_records = 8192; ///< Force fsync/flush if un-fsynced count is met
    uint32_t flush_every_ms =
        25; ///< Force fsync/flush if age of oldest batch reaches this in milliseconds
  };

} // namespace fell::storage
