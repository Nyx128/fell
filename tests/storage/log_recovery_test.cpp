#include "storage/log_recovery.hpp"
#include "storage/segment_writer.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

using namespace fell::storage;
using namespace fell::platform;

class LogRecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
    std::filesystem::create_directories("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }
};

TEST_F(LogRecoveryTest, RecoverIntactSegment) {
  auto dir = std::filesystem::path("test-data");

  {
    SegmentWriter writer(dir, 0, nullptr, 1);
    std::vector<uint8_t> m1 = {0x01};
    writer.append(1000, m1.data(), m1.size());
  }

  auto log_path = dir / "00000000000000000000.log";
  auto idx_path = dir / "00000000000000000000.idx";

  RecoveryResult result = recover_partition(dir);

  EXPECT_EQ(result.next_offset, 1); // Appended 1 message at offset 0, so next is 1

  // File size should not have been truncated as it was intact
  EXPECT_EQ(std::filesystem::file_size(log_path), 20 + 1); // 20 byte header + 1 byte payload
}

TEST_F(LogRecoveryTest, RecoverCorruptedSegment) {
  auto dir = std::filesystem::path("test-data");

  {
    SegmentWriter writer(dir, 0, nullptr, 1);
    std::vector<uint8_t> m1 = {0x01};
    std::vector<uint8_t> m2 = {0x02};
    writer.append(1000, m1.data(), m1.size());
    writer.append(1001, m2.data(), m2.size());
  }

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
