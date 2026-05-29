#include "storage/segment_writer.hpp"
#include <filesystem>
#include <gtest/gtest.h>

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

TEST_F(SegmentWriterTest, SegmentWriterInitialization) {
  auto dir = std::filesystem::path("test-data");

  {
    SegmentWriter writer(dir, 0);

    // Initial files should exist
    EXPECT_TRUE(std::filesystem::exists(dir / "00000000000000000000.log"));
    EXPECT_TRUE(std::filesystem::exists(dir / "00000000000000000000.idx"));

    EXPECT_EQ(writer.base_offset(), 0);
    EXPECT_EQ(writer.next_offset(), 0);
    EXPECT_EQ(writer.bytes_written(), 0);
  }
}
