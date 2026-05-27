#include <gtest/gtest.h>
#include "storage/offset_index.hpp"
#include "platform/endian.hpp"
#include "platform/file.hpp"
#include <filesystem>
#include <vector>

using namespace fell::storage;
using namespace fell::platform;

class OffsetIndexTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
    std::filesystem::create_directories("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }
};

TEST_F(OffsetIndexTest, LookupReturnsCorrectPosition) {
  auto idx_path = std::filesystem::path("test-data/test.idx");
  
  // Write a dummy index file
  file_t fd = open_file_append(idx_path);
  ASSERT_NE(fd, INVALID_FILE);

  std::vector<IndexEntry> entries = {
    {host_to_be64(10), host_to_be64(100)},
    {host_to_be64(20), host_to_be64(200)},
    {host_to_be64(30), host_to_be64(300)}
  };

  std::vector<IOBuffer> bufs = {
    {entries.data(), entries.size() * sizeof(IndexEntry)}
  };
  ASSERT_TRUE(write_file_vec(fd, bufs.data(), 1));
  flush_file(fd);
  close_file(fd);

  // Test OffsetIndex lookup
  OffsetIndex index(idx_path);

  // Exact matches
  EXPECT_EQ(index.lookup(10), 100);
  EXPECT_EQ(index.lookup(20), 200);
  EXPECT_EQ(index.lookup(30), 300);

  // In between matches (should return lower bound)
  EXPECT_EQ(index.lookup(15), 100);
  EXPECT_EQ(index.lookup(29), 200);
  EXPECT_EQ(index.lookup(100), 300);

  // Before first entry (should return 0 to scan from start of file)
  EXPECT_EQ(index.lookup(5), 0);
}

TEST_F(OffsetIndexTest, EmptyFileReturnsZero) {
  auto idx_path = std::filesystem::path("test-data/empty.idx");
  file_t fd = open_file_append(idx_path);
  close_file(fd);

  OffsetIndex index(idx_path);
  EXPECT_EQ(index.lookup(100), 0);
}
