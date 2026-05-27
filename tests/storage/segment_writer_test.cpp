#include "storage/segment_writer.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

using namespace fell::storage;

class SegmentWriterTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
    std::filesystem::create_directories("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }
};

TEST_F(SegmentWriterTest, AppendAndRotate) {
  auto dir = std::filesystem::path("test-data");

  uint64_t last_rotated_base = 0;
  auto on_rotate = [&](uint64_t new_base) { last_rotated_base = new_base; };

  {
    SegmentWriter writer(dir, 0, on_rotate, 100);

    std::vector<uint8_t> payload = {0xAA, 0xBB};

    // Append a few messages
    EXPECT_EQ(writer.append(1000, payload.data(), payload.size()), 0);
    EXPECT_EQ(writer.append(1001, payload.data(), payload.size()), 1);

    // File "00000000000000000000.log" should exist
    EXPECT_TRUE(std::filesystem::exists(dir / "00000000000000000000.log"));
  }

  // Writer is destroyed, files are flushed.
  EXPECT_TRUE(std::filesystem::exists(dir / "00000000000000000000.idx"));
  EXPECT_GT(std::filesystem::file_size(dir / "00000000000000000000.idx"), 0);

  EXPECT_EQ(last_rotated_base, 0); // Didn't rotate
}
