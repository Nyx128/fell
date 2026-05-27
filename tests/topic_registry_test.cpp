#include <gtest/gtest.h>
#include "broker/topic_registry.hpp"
#include <filesystem>
#include <vector>

using namespace fell;

class TopicRegistryTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }
};

TEST_F(TopicRegistryTest, CreateTopic) {
  TopicRegistry registry("test-data");

  EXPECT_TRUE(registry.create_topic("sales", 3));
  EXPECT_FALSE(registry.create_topic("sales", 3)); // duplicate

  EXPECT_EQ(registry.num_partitions("sales"), 3);
  EXPECT_EQ(registry.num_partitions("non-existent"), 0);
}

TEST_F(TopicRegistryTest, AppendAndFetch) {
  TopicRegistry registry("test-data");
  ASSERT_TRUE(registry.create_topic("sales", 3));

  Partition* p0 = registry.get_partition("sales", 0);
  Partition* p1 = registry.get_partition("sales", 1);
  Partition* p2 = registry.get_partition("sales", 2);
  Partition* p3 = registry.get_partition("sales", 3); // out of bounds

  ASSERT_NE(p0, nullptr);
  ASSERT_NE(p1, nullptr);
  ASSERT_NE(p2, nullptr);
  EXPECT_EQ(p3, nullptr);

  std::vector<uint8_t> m1 = {0xAA};
  std::vector<uint8_t> m2 = {0xBB, 0xCC};

  EXPECT_EQ(p0->append(m1.data(), m1.size()), 0);
  EXPECT_EQ(p0->append(m2.data(), m2.size()), 1);

  std::vector<Message> fetched = p0->fetch(0, 10);
  
  ASSERT_EQ(fetched.size(), 2);
  EXPECT_EQ(fetched[0].offset, 0);
  EXPECT_EQ(fetched[0].payload, m1);
  EXPECT_EQ(fetched[1].offset, 1);
  EXPECT_EQ(fetched[1].payload, m2);
}
