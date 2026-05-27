#pragma once
#include "log_format.hpp"
#include <cstdint>
#include <filesystem>
#include <functional>
#include <vector>

#include "platform/file.hpp"

namespace fell::storage {

  class SegmentWriter {
  public:
    // Opens or creates the segment for base_offset in partition_dir.
    // on_rotate is called with the new base offset when rotation occurs.
    SegmentWriter(const std::filesystem::path &partition_dir, uint64_t base_offset,
                  std::function<void(uint64_t new_base)> on_rotate = {}, uint32_t sync_every = 16);

    ~SegmentWriter();

    // Appends one message. Returns the assigned offset.
    // Rotates segment if size threshold reached after this write.
    uint64_t append(uint64_t timestamp_ms, const uint8_t *payload, uint32_t payload_size);

    // Appends one message without flushing. Returns the assigned offset.
    // Rotates segment if size threshold reached after this write.
    uint64_t append_no_flush(uint64_t timestamp_ms, const uint8_t *payload, uint32_t payload_size);

    bool flush_due() const {
      return writes_since_flush_ >= sync_every_;
    }

    void flush();

    uint64_t next_offset() const {
      return next_offset_;
    }
    uint64_t base_offset() const {
      return base_offset_;
    }
    uint64_t bytes_written() const {
      return bytes_written_;
    }

  private:
    void write_index_entry(uint64_t offset, uint64_t file_pos);
    void rotate();
    void open_files();

    std::filesystem::path partition_dir_;
    uint64_t base_offset_;
    uint64_t next_offset_;
    uint64_t bytes_written_ = 0;
    uint32_t writes_since_flush_ = 0;
    uint32_t sync_every_ = 16;

    // Using intptr_t for handles to stay platform agnostic in the header
    file_t log_fd_ = INVALID_FILE;
    file_t idx_fd_ = INVALID_FILE;

    std::function<void(uint64_t)> on_rotate_;
  };

} // namespace fell::storage
