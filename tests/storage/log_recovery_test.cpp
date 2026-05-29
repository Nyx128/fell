#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_format.hpp"
#include "storage/log_recovery.hpp"
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

using namespace fell::storage;

class LogRecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
    std::filesystem::create_directories("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }

  void write_record_to_segment(const std::filesystem::path &dir, uint64_t offset,
                               uint64_t timestamp_ms, const std::vector<uint8_t> &payload) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%020llu", 0ULL);
    auto log_path = dir / (std::string(buf) + ".log");
    auto idx_path = dir / (std::string(buf) + ".idx");

    file_t log_fd = fell::platform::open_file_append(log_path);
    file_t idx_fd = fell::platform::open_file_append(idx_path);

    uint64_t file_pos =
        std::filesystem::exists(log_path) ? std::filesystem::file_size(log_path) : 0;

    LogRecordHeader header{fell::platform::host_to_be64(offset),
                           fell::platform::host_to_be64(timestamp_ms),
                           fell::platform::host_to_be32(static_cast<uint32_t>(payload.size()))};

    fell::platform::IOBuffer log_buf{&header, sizeof(header)};
    fell::platform::IOBuffer pay_buf{payload.data(), payload.size()};

    std::array<fell::platform::IOBuffer, 2> bufs = {log_buf, pay_buf};
    fell::platform::write_file_vec(log_fd, bufs.data(), bufs.size());

    IndexEntry entry{fell::platform::host_to_be64(offset), fell::platform::host_to_be64(file_pos)};
    fell::platform::IOBuffer idx_buf{&entry, sizeof(entry)};
    fell::platform::write_file_vec(idx_fd, &idx_buf, 1);

    fell::platform::close_file(log_fd);
    fell::platform::close_file(idx_fd);
  }
};

TEST_F(LogRecoveryTest, RecoverIntactSegment) {
  auto dir = std::filesystem::path("test-data");

  std::vector<uint8_t> m1 = {0x01};
  write_record_to_segment(dir, 0, 1000, m1);

  auto log_path = dir / "00000000000000000000.log";
  auto idx_path = dir / "00000000000000000000.idx";

  RecoveryResult result = recover_partition(dir);

  EXPECT_EQ(result.next_offset, 1); // Appended 1 message at offset 0, so next is 1

  // File size should not have been truncated as it was intact
  EXPECT_EQ(std::filesystem::file_size(log_path), 20 + 1); // 20 byte header + 1 byte payload
}

TEST_F(LogRecoveryTest, RecoverCorruptedSegment) {
  auto dir = std::filesystem::path("test-data");

  std::vector<uint8_t> m1 = {0x01};
  std::vector<uint8_t> m2 = {0x02};
  write_record_to_segment(dir, 0, 1000, m1);
  write_record_to_segment(dir, 1, 1001, m2);

  auto log_path = dir / "00000000000000000000.log";
  auto idx_path = dir / "00000000000000000000.idx";

  // Corrupt the file by truncating the last byte of the second message
  uint64_t original_size = std::filesystem::file_size(log_path);
  std::filesystem::resize_file(log_path, original_size - 1);

  // Recover should truncate the partial second message
  RecoveryResult result = recover_partition(dir);

  EXPECT_EQ(result.next_offset, 1); // Second message was corrupted and dropped!
  EXPECT_EQ(std::filesystem::file_size(log_path), 20 + 1); // Only first message remains
}
