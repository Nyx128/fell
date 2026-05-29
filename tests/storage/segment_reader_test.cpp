#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_format.hpp"
#include "storage/segment_reader.hpp"
#include <array>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

using namespace fell::storage;

class SegmentReaderTest : public ::testing::Test {
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

TEST_F(SegmentReaderTest, ReadValidRecords) {
  auto dir = std::filesystem::path("test-data");

  std::vector<uint8_t> m1 = {0xAA};
  std::vector<uint8_t> m2 = {0xBB, 0xCC};

  write_record_to_segment(dir, 0, 1000, m1);
  write_record_to_segment(dir, 1, 1001, m2);

  auto log_path = dir / "00000000000000000000.log";
  ASSERT_TRUE(std::filesystem::exists(log_path));

  SegmentReader reader(log_path);

  // Read starting from file position 0, target offset 0
  auto records = reader.read(0, 0, 10);

  ASSERT_EQ(records.size(), 2);

  EXPECT_EQ(records[0].offset, 0);
  EXPECT_EQ(records[0].payload.size(), 1);
  EXPECT_EQ(records[0].payload[0], 0xAA);

  EXPECT_EQ(records[1].offset, 1);
  EXPECT_EQ(records[1].payload.size(), 2);
  EXPECT_EQ(records[1].payload[0], 0xBB);
  EXPECT_EQ(records[1].payload[1], 0xCC);
}

TEST_F(SegmentReaderTest, ReadWithStartOffsetSkipping) {
  auto dir = std::filesystem::path("test-data");

  std::vector<uint8_t> m1 = {0xAA};
  std::vector<uint8_t> m2 = {0xBB};
  std::vector<uint8_t> m3 = {0xCC};

  write_record_to_segment(dir, 0, 1000, m1);
  write_record_to_segment(dir, 1, 1001, m2);
  write_record_to_segment(dir, 2, 1002, m3);

  auto log_path = dir / "00000000000000000000.log";
  SegmentReader reader(log_path);

  // Scan from file position 0, but only want offsets >= 1
  auto records = reader.read(0, 1, 10);

  ASSERT_EQ(records.size(), 2);
  EXPECT_EQ(records[0].offset, 1);
  EXPECT_EQ(records[1].offset, 2);
}

TEST_F(SegmentReaderTest, ScanValid) {
  auto dir = std::filesystem::path("test-data");

  std::vector<uint8_t> m1 = {0x11};
  write_record_to_segment(dir, 0, 1000, m1);

  auto log_path = dir / "00000000000000000000.log";
  SegmentReader reader(log_path);

  std::vector<DiskRecord> out;
  uint64_t valid_bytes = reader.scan_valid(0, out);

  EXPECT_GT(valid_bytes, 0);
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].offset, 0);
  EXPECT_EQ(out[0].payload[0], 0x11);
}
