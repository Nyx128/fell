#include <gtest/gtest.h>
#include "storage/partition_store.hpp"
#include <filesystem>
#include <vector>

using namespace fell::storage;

class PartitionStoreTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
    std::filesystem::create_directories("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }
};

TEST_F(PartitionStoreTest, AppendAndFetch) {
  auto dir = std::filesystem::path("test-data");
  
  PartitionStore store(dir);

  std::vector<uint8_t> m1 = {0x11, 0x22};
  std::vector<uint8_t> m2 = {0x33, 0x44, 0x55};

  EXPECT_EQ(store.append(m1.data(), m1.size()), 0);
  EXPECT_EQ(store.append(m2.data(), m2.size()), 1);

  auto fetched = store.fetch(0, 10);
  
  ASSERT_EQ(fetched.size(), 2);
  
  EXPECT_EQ(fetched[0].offset, 0);
  EXPECT_EQ(fetched[0].payload, m1);

  EXPECT_EQ(fetched[1].offset, 1);
  EXPECT_EQ(fetched[1].payload, m2);
}

TEST_F(PartitionStoreTest, FetchOutOfBounds) {
  auto dir = std::filesystem::path("test-data");
  PartitionStore store(dir);

  std::vector<uint8_t> m1 = {0x11};
  EXPECT_EQ(store.append(m1.data(), m1.size()), 0);

  // Offset 1 doesn't exist yet
  auto fetched = store.fetch(1, 10);
  EXPECT_TRUE(fetched.empty());
}
