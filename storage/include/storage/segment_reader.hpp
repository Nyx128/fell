#pragma once

#include "log_format.hpp"
#include "platform/file.hpp"
#include <cstdint>
#include <filesystem>
#include <vector>

namespace fell::storage {

  /**
   * @struct DiskRecord
   * @brief Deserialized record read back from a log segment.
   */
  struct DiskRecord {
    uint64_t offset;              ///< Assigned monotonic offset
    uint64_t timestamp_ms;        ///< Ingest timestamp in milliseconds
    std::vector<uint8_t> payload; ///< Binary payload
  };

  /**
   * @class SegmentReader
   * @brief Thread-safe passive reader for immutable or active log segment files.
   * 
   * Design Insight:
   * Decoupled from writing threads. Reads leverage random access lookups via raw file 
   * descriptors, allowing concurrent readers to bypass active batch serialization locks.
   */
  class SegmentReader {
  public:
    /**
     * @brief Opens a segment log file for reading.
     * @param log_path Path to the `.log` segment file.
     */
    explicit SegmentReader(const std::filesystem::path &log_path);
    ~SegmentReader();

    // Disable copy
    SegmentReader(const SegmentReader &) = delete;
    SegmentReader &operator=(const SegmentReader &) = delete;

    /**
     * @brief Seeks to `file_position` and reads up to `max_count` records with offset >= `start_offset`.
     * @param file_position Physical byte position to start scanning from (obtained via index lookup).
     * @param start_offset Lower bound logical offset filter.
     * @param max_count Maximum records to fetch.
     * @return List of sequential valid records.
     */
    std::vector<DiskRecord> read(uint64_t file_position, uint64_t start_offset,
                                 uint16_t max_count) const;

    /**
     * @brief Scans all records from `from_position` to validate checksums and bounds.
     * 
     * Mainly used during partition startup recovery. Scanning stops immediately at 
     * the first malformed record, torn write, or CRC mismatch.
     * 
     * @param from_position Physical byte position to start scanning from.
     * @param out Collected list of sequential valid records.
     * @return Byte position immediately following the last fully valid record.
     */
    [[nodiscard]] uint64_t scan_valid(uint64_t from_position, std::vector<DiskRecord> &out) const;

  private:
    std::filesystem::path path_;
    file_t fd_ = INVALID_FILE;
  };

} // namespace fell::storage
