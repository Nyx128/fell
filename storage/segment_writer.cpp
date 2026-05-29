#include "storage/segment_writer.hpp"
#include "platform/file.hpp"
#include <cstdio>
#include <system_error>

namespace fell::storage {

  SegmentWriter::SegmentWriter(const std::filesystem::path &partition_dir, uint64_t base_offset)
      : partition_dir_(partition_dir), base_offset_(base_offset), next_offset_(base_offset) {
    open_files();
  }

  SegmentWriter::~SegmentWriter() {
    flush();
    if (log_fd_ != INVALID_FILE) {
      platform::close_file(log_fd_);
    }
    if (idx_fd_ != INVALID_FILE) {
      platform::close_file(idx_fd_);
    }
  }

  void SegmentWriter::flush() {
    if (log_fd_ != INVALID_FILE) {
      platform::flush_file(log_fd_);
    }
    if (idx_fd_ != INVALID_FILE) {
      platform::flush_file(idx_fd_);
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
      log_fd_ = INVALID_FILE;
      throw std::system_error(errno, std::system_category(),
                              "Failed to open idx file: " + idx_path.string());
    }
  }

} // namespace fell::storage
