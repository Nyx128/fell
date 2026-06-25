#pragma once
#include <cstdint>

namespace fell::storage {

  static constexpr uint32_t LOG_RECORD_HEADER_SIZE = 20;
  static constexpr uint32_t LOG_SEGMENT_MAX_BYTES = 64 * 1024 * 1024; // 64 MB
  static constexpr uint32_t INDEX_INTERVAL = 128;                     // index every Nth message

#pragma pack(push, 1)
  struct LogRecordHeader {
    uint64_t offset;
    uint64_t timestamp_ms;
    uint32_t payload_size;
  };
#pragma pack(pop)

  static_assert(sizeof(LogRecordHeader) == LOG_RECORD_HEADER_SIZE);

#pragma pack(push, 1)
  struct IndexEntry {
    uint64_t offset;
    uint64_t file_position;
  };
#pragma pack(pop)

  static_assert(sizeof(IndexEntry) == 16);

} // namespace fell::storage
