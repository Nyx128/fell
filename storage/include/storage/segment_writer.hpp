#pragma once

#include "log_format.hpp"
#include "platform/file.hpp"
#include <cstdint>
#include <filesystem>

namespace fell::storage {

  /**
   * @class SegmentWriter
   * @brief Low-level, thread-hostile sequential segment and index file writer.
   * 
   * Design Insight:
   * Encapsulates log file appending, offset index generation, and raw OS file descriptor flushing.
   * By keeping this component passive and single-threaded, concurrency controls and batching decisions 
   * are elevated to the PartitionStore layer.
   * 
   * Assumptions:
   * - Callers guarantee mutually exclusive access to instances of SegmentWriter.
   */
  class SegmentWriter {
  public:
    /**
     * @brief Creates or resumes a log segment file.
     * @param partition_dir Partition subdirectory.
     * @param base_offset The base offset at which this segment starts.
     */
    SegmentWriter(const std::filesystem::path &partition_dir, uint64_t base_offset);
    ~SegmentWriter();

    // Disable copy
    SegmentWriter(const SegmentWriter &) = delete;
    SegmentWriter &operator=(const SegmentWriter &) = delete;

    /// @brief Gets the active log OS file handle.
    file_t log_fd() const { return log_fd_; }

    /// @brief Gets the active offset index OS file handle.
    file_t idx_fd() const { return idx_fd_; }

    /// @brief The base (start) offset of the segment.
    uint64_t base_offset() const { return base_offset_; }

    /// @brief The next sequential unwritten offset.
    uint64_t next_offset() const { return next_offset_; }

    /// @brief Cumulative bytes written into the active log file.
    uint64_t bytes_written() const { return bytes_written_; }

    /// @brief Advances physical byte count tracking.
    void add_bytes_written(uint64_t bytes) { bytes_written_ += bytes; }

    /// @brief Advances active sequential logical offset tracking.
    void advance_next_offset(uint64_t count) { next_offset_ += count; }

    /// @brief Restores writer offset tracking state upon recovery.
    void set_next_offset(uint64_t offset) { next_offset_ = offset; }

    /// @brief Restores writer byte offset tracking state upon recovery.
    void set_bytes_written(uint64_t bytes) { bytes_written_ = bytes; }

    /// @brief Syncs write buffers down to OS storage caches.
    void flush();

  private:
    void open_files();

    std::filesystem::path partition_dir_;
    uint64_t base_offset_;
    uint64_t next_offset_;
    uint64_t bytes_written_ = 0;

    file_t log_fd_ = INVALID_FILE;
    file_t idx_fd_ = INVALID_FILE;
  };

} // namespace fell::storage
