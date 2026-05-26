#pragma once
#include "log_format.hpp"
#include "platform/file.hpp"
#include <cstdint>
#include <filesystem>
#include <vector>

namespace fell::storage {

  // A single decoded record from disk.
  struct DiskRecord {
    uint64_t offset;
    uint64_t timestamp_ms;
    std::vector<uint8_t> payload;
  };

  class SegmentReader {
  public:
    explicit SegmentReader(const std::filesystem::path &log_path);
    ~SegmentReader();

    // Seek to file_position, then read up to max_count records
    // whose offset >= start_offset.
    // Returns records in offset order.
    std::vector<DiskRecord> read(uint64_t file_position, uint64_t start_offset,
                                 uint16_t max_count) const;

    // Used by recovery: scan all records from file_position.
    // Stops at first partial/corrupt record.
    // Returns the file position of the byte after the last valid record.
    [[nodiscard]] uint64_t scan_valid(uint64_t from_position, std::vector<DiskRecord> &out) const;

  private:
    std::filesystem::path path_;
    file_t fd_ = INVALID_FILE;
  };

} // namespace fell::storage
