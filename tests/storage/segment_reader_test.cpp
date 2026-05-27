#include <gtest/gtest.h>
#include "storage/segment_reader.hpp"
#include "storage/segment_writer.hpp"
#include <filesystem>
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
};

TEST_F(SegmentReaderTest, ReadValidRecords) {
  auto dir = std::filesystem::path("test-data");
  
  // Create a segment using the writer
  {
    SegmentWriter writer(dir, 0, nullptr, 1); // flush every write
    std::vector<uint8_t> m1 = {0xAA};
    std::vector<uint8_t> m2 = {0xBB, 0xCC};
    
    writer.append(1000, m1.data(), m1.size());
    writer.append(1001, m2.data(), m2.size());
  }

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
  
  {
    SegmentWriter writer(dir, 0, nullptr, 1);
    std::vector<uint8_t> m1 = {0xAA};
    std::vector<uint8_t> m2 = {0xBB};
    std::vector<uint8_t> m3 = {0xCC};
    
    writer.append(1000, m1.data(), m1.size());
    writer.append(1001, m2.data(), m2.size());
    writer.append(1002, m3.data(), m3.size());
  }

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
  
  {
    SegmentWriter writer(dir, 0, nullptr, 1);
    std::vector<uint8_t> m1 = {0x11};
    writer.append(1000, m1.data(), m1.size());
  }

  auto log_path = dir / "00000000000000000000.log";
  SegmentReader reader(log_path);

  std::vector<DiskRecord> out;
  uint64_t valid_bytes = reader.scan_valid(0, out);
  
  EXPECT_GT(valid_bytes, 0);
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].offset, 0);
  EXPECT_EQ(out[0].payload[0], 0x11);
}
