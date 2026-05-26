#include "storage/segment_reader.hpp"
#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_format.hpp"
#include <iostream>
#include <system_error>

namespace fell::storage {

  SegmentReader::SegmentReader(const std::filesystem::path &log_path) : path_(log_path) {
    fd_ = platform::open_file_read(log_path);
  }

  SegmentReader::~SegmentReader() {
    platform::close_file(fd_);
  }

  std::vector<DiskRecord> SegmentReader::read(uint64_t file_position, uint64_t start_offset,
                                              uint16_t max_count) const {
    std::vector<DiskRecord> records;
    records.reserve(max_count);

    uint64_t current_pos = file_position;
    while (records.size() < max_count) {
      LogRecordHeader log_header;
      ssize_t bytes_read =
          platform::pread_file(fd_, &log_header, sizeof(LogRecordHeader), current_pos);
      if (bytes_read == -1) {
        throw std::system_error(errno, std::system_category(), "failed to read log header");
      }

      if (static_cast<size_t>(bytes_read) < sizeof(LogRecordHeader)) {
        break; // EOF or partial header
      }

      current_pos += sizeof(LogRecordHeader);
      log_header.offset = platform::be64_to_host(log_header.offset);
      log_header.timestamp_ms = platform::be64_to_host(log_header.timestamp_ms);
      log_header.payload_size = platform::be32_to_host(log_header.payload_size);

      if (log_header.offset < start_offset) {
        // Skip the payload read entirely if we don't need this record
        // This is efficient because the sparse index often lands us up to 127 messages early.
        current_pos += log_header.payload_size;
        continue;
      }

      std::vector<uint8_t> payload(log_header.payload_size);

      bytes_read = platform::pread_file(fd_, payload.data(), log_header.payload_size, current_pos);
      if (bytes_read == -1) {
        throw std::system_error(errno, std::system_category(), "failed to read log payload");
      }

      if (static_cast<size_t>(bytes_read) < log_header.payload_size) {
        break; // EOF or partial payload
      }

      current_pos += log_header.payload_size;

      DiskRecord dr;
      dr.offset = log_header.offset;
      dr.payload = std::move(payload);
      dr.timestamp_ms = log_header.timestamp_ms;
      records.emplace_back(std::move(dr));
    }
    return records;
  }

  uint64_t SegmentReader::scan_valid(uint64_t from_position, std::vector<DiskRecord> &out) const {
    uint64_t current_pos = from_position;
    while (true) {
      LogRecordHeader log_header;
      ssize_t bytes_read =
          platform::pread_file(fd_, &log_header, sizeof(LogRecordHeader), current_pos);
      if (bytes_read == -1 || static_cast<size_t>(bytes_read) < sizeof(LogRecordHeader)) {
        break; // EOF or partial header
      }

      log_header.offset = platform::be64_to_host(log_header.offset);
      log_header.timestamp_ms = platform::be64_to_host(log_header.timestamp_ms);
      log_header.payload_size = platform::be32_to_host(log_header.payload_size);

      std::vector<uint8_t> payload(log_header.payload_size);
      bytes_read = platform::pread_file(fd_, payload.data(), log_header.payload_size,
                                        current_pos + sizeof(LogRecordHeader));
      if (bytes_read == -1 || static_cast<size_t>(bytes_read) < log_header.payload_size) {
        break; // EOF or partial payload
      }

      DiskRecord dr;
      dr.offset = log_header.offset;
      dr.timestamp_ms = log_header.timestamp_ms;
      dr.payload = std::move(payload);
      out.emplace_back(std::move(dr));

      current_pos += sizeof(LogRecordHeader) + log_header.payload_size;
    }
    return current_pos;
  }

} // namespace fell::storage
