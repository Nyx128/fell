#define NOMINMAX
#include "broker/topic_registry.hpp"
#include "platform/endian.hpp"
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include "replication/replication_client.hpp"
#include <filesystem>
#include <gtest/gtest.h>

namespace {

  class ReplicationClientTest : public ::testing::Test {
  protected:
    void SetUp() override {
      std::filesystem::remove_all("test-repl-client");
    }

    void TearDown() override {
      std::filesystem::remove_all("test-repl-client");
    }
  };

} // namespace

TEST_F(ReplicationClientTest, FetchLogStartsFromCommittedOffset) {
  fell::storage::StorageOptions opts;
  opts.batch_wait_us = 500000;

  fell::TopicRegistry registry("test-repl-client", opts);
  ASSERT_TRUE(registry.create_topic("orders", 1));

  auto *partition = registry.get_partition("orders", 0);
  ASSERT_NE(partition, nullptr);

  const uint8_t payload[] = {0x42};
  auto append = partition->append(payload, 1);
  ASSERT_TRUE(append.accepted);
  ASSERT_EQ(append.offset, 0u);
  ASSERT_EQ(partition->committed_offset(), 0u);

  fell::repl::ClusterConfig cfg;
  cfg.broker_id = 1;
  cfg.peers.push_back({0, "127.0.0.1", 8700, 7700});

  fell::repl::PartitionMetaRegistry meta_reg;
  auto &meta = meta_reg.get("orders", 0);
  meta.leader_id = 0;

  fell::repl::ReplicationClient client("orders", 0, cfg, meta_reg, registry);
  const auto req = client.build_fetch_log_request();

  EXPECT_EQ(fell::platform::be64_to_host(req.start_offset), partition->committed_offset());
  EXPECT_EQ(fell::platform::be64_to_host(req.start_offset), 0u);
}
