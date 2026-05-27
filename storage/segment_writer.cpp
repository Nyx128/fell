#include "storage/segment_writer.hpp"
#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_format.hpp"
#include <array>
#include <system_error>
#include <cassert>

namespace fell::storage {

  SegmentWriter::SegmentWriter(const std::filesystem::path &partition_dir, uint64_t base_offset,
                               std::function<void(uint64_t)> on_rotate, uint32_t sync_every)
      : partition_dir_(partition_dir), base_offset_(base_offset), next_offset_(base_offset),
        sync_every_(sync_every), on_rotate_(std::move(on_rotate)) {
    assert(sync_every_ > 0);
    open_files();
  }

  SegmentWriter::~SegmentWriter() {
    flush();
    platform::flush_file(idx_fd_);
    platform::close_file(log_fd_);
    platform::close_file(idx_fd_);
  }

  uint64_t SegmentWriter::append(uint64_t timestamp_ms, const uint8_t *payload,
                                 uint32_t payload_size) {
    uint64_t assigned_offset = append_no_flush(timestamp_ms, payload, payload_size);
    if (flush_due()) {
      flush();
    }
    return assigned_offset;
  }

  uint64_t SegmentWriter::append_no_flush(uint64_t timestamp_ms, const uint8_t *payload,
                                          uint32_t payload_size) {
    uint64_t assigned_offset = next_offset_;
    size_t file_pos = bytes_written_;

    if ((assigned_offset - base_offset_) % INDEX_INTERVAL == 0) {
      write_index_entry(assigned_offset, file_pos);
    }

    LogRecordHeader header = {platform::host_to_be64(assigned_offset),
                              platform::host_to_be64(timestamp_ms),
                              platform::host_to_be32(payload_size)};

    std::array<platform::IOBuffer, 2> buffers = {
        {{&header, LOG_RECORD_HEADER_SIZE}, {payload, payload_size}}};

    if(!platform::write_file_vec(log_fd_, buffers.data(), 2)){
      throw std::system_error(errno, std::system_category(), "Failed to write to log entry");
    }

    bytes_written_ += sizeof(LogRecordHeader) + payload_size;
    next_offset_++;
    writes_since_flush_++;

    if(bytes_written_ >= LOG_SEGMENT_MAX_BYTES){
      rotate();
    }

    return assigned_offset;
  }

  void SegmentWriter::write_index_entry(uint64_t offset, uint64_t file_pos) {
    IndexEntry entry = {platform::host_to_be64(offset), platform::host_to_be64(file_pos)};
    platform::IOBuffer buf{&entry, sizeof(entry)};
    if (!platform::write_file_vec(idx_fd_, &buf, 1)) {
      throw std::system_error(errno, std::system_category(), "Failed to write index entry");
    }
  }

  void SegmentWriter::flush() {
    if(writes_since_flush_ > 0){
      if(!platform::flush_file(log_fd_)){
        throw std::system_error(errno, std::system_category(), "Failed to flush log file");
      }
      writes_since_flush_ = 0;
    }
  }

  void SegmentWriter::rotate() {
    flush();
    platform::flush_file(idx_fd_);
    platform::close_file(log_fd_);
    platform::close_file(idx_fd_);

    base_offset_ = next_offset_;
    bytes_written_ = 0;

    open_files();

    if(on_rotate_){
      on_rotate_(base_offset_);
    }
  }

  void SegmentWriter::open_files() {
    char buf[32];
    snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(base_offset_));
    std::string stem = buf;
    auto log_path = partition_dir_ / (stem + ".log");
    auto idx_path = partition_dir_ / (stem + ".idx");
    log_fd_ = platform::open_file_append(log_path);
    if (log_fd_ == INVALID_FILE) {
      throw std::system_error(errno, std::system_category(),
                              "Failed to open log file: " + log_path.string());
    }
    idx_fd_ = platform::open_file_append(idx_path);
    if (idx_fd_ == INVALID_FILE) {
      platform::close_file(log_fd_);
      throw std::system_error(errno, std::system_category(),
                              "Failed to open idx file: " + idx_path.string());
    }
  }

} // namespace fell::storage
