#define NOMINMAX
#include "broker/broker.hpp"
#include "broker/protocol.hpp"
#include "broker/topic_registry.hpp"
#include "platform/endian.hpp"
#include "platform/socket.hpp"
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include "replication/repl_protocol.hpp"
#include "replication/replication_server.hpp"
#include <chrono>
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

namespace {

  using namespace std::chrono_literals;

  void wait_for_commit(fell::Partition *partition, uint64_t offset) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (partition->committed_offset() > offset) {
        return;
      }
      std::this_thread::sleep_for(5ms);
    }
    FAIL() << "Timed out waiting for committed offset " << offset;
  }

  bool recv_exact(socket_t fd, uint8_t *buf, size_t len, int timeout_ms) {
    fell::platform::set_nonblocking(fd);
    size_t received = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (received < len && std::chrono::steady_clock::now() < deadline) {
      const int n = fell::platform::recv_data(fd, buf + received, len - received);
      if (n > 0) {
        received += static_cast<size_t>(n);
        continue;
      }
      if (n < 0 && !fell::platform::would_block()) {
        return false;
      }
      std::this_thread::sleep_for(5ms);
    }
    return received == len;
  }

  bool recv_frame(socket_t fd, uint8_t &opcode, std::vector<uint8_t> &payload, int timeout_ms) {
    uint32_t net_len = 0;
    if (!recv_exact(fd, reinterpret_cast<uint8_t *>(&net_len), sizeof(net_len), timeout_ms)) {
      return false;
    }

    const uint32_t frame_len = fell::platform::be32_to_host(net_len);
    if (frame_len == 0) {
      return false;
    }

    std::vector<uint8_t> frame(frame_len);
    if (!recv_exact(fd, frame.data(), frame.size(), timeout_ms)) {
      return false;
    }

    opcode = frame[0];
    payload.assign(frame.begin() + 1, frame.end());
    return true;
  }

  socket_t connect_with_retry(const char *host, uint16_t port, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
      socket_t fd = fell::platform::connect_socket(host, port);
      if (fd != INVALID_SOCKET_T) {
        return fd;
      }
      std::this_thread::sleep_for(20ms);
    }
    return INVALID_SOCKET_T;
  }

  std::vector<uint8_t> make_publish_frame(const std::string &topic, uint16_t partition,
                                          const std::vector<uint8_t> &payload) {
    fell::proto::PublishReq req{};
    req.topic_len = static_cast<uint8_t>(topic.size());
    std::memcpy(req.topic, topic.data(), topic.size());
    req.partition = fell::platform::host_to_be16(partition);
    req.payload_size = fell::platform::host_to_be32(static_cast<uint32_t>(payload.size()));

    std::vector<uint8_t> frame_payload(sizeof(req) + payload.size());
    std::memcpy(frame_payload.data(), &req, sizeof(req));
    std::memcpy(frame_payload.data() + sizeof(req), payload.data(), payload.size());

    const uint32_t frame_len = 1 + static_cast<uint32_t>(frame_payload.size());
    std::vector<uint8_t> frame(4 + frame_len);
    const uint32_t net_len = fell::platform::host_to_be32(frame_len);
    std::memcpy(frame.data(), &net_len, sizeof(net_len));
    frame[4] = static_cast<uint8_t>(fell::Op::PUBLISH);
    std::memcpy(frame.data() + 5, frame_payload.data(), frame_payload.size());
    return frame;
  }

  class ReplicationIntegrationTest : public ::testing::Test {
  protected:
    void SetUp() override {
      fell::platform::platform_net_init();
      std::filesystem::remove_all("test-repl-server");
      std::filesystem::remove_all("test-broker-repl");
    }

    void TearDown() override {
      std::filesystem::remove_all("test-repl-server");
      std::filesystem::remove_all("test-broker-repl");
      fell::platform::platform_net_cleanup();
    }
  };

} // namespace

TEST_F(ReplicationIntegrationTest, ReplicationServerDecodesFetchPartitionInHostOrder) {
  fell::TopicRegistry registry("test-repl-server");
  ASSERT_TRUE(registry.create_topic("orders", 2));
  auto *partition = registry.get_partition("orders", 1);
  ASSERT_NE(partition, nullptr);

  const uint8_t payload[] = {0xAA, 0xBB};
  ASSERT_TRUE(partition->append(payload, 2).accepted);
  wait_for_commit(partition, 0);

  fell::repl::ClusterConfig cfg;
  cfg.repl_port = 8710;

  fell::repl::PartitionMetaRegistry meta_reg;
  auto &meta = meta_reg.get("orders", 1);
  meta.epoch = 7;

  fell::repl::ReplicationServer server(cfg, meta_reg, registry);
  server.start();

  socket_t fd = connect_with_retry("127.0.0.1", cfg.repl_port, 2000);
  ASSERT_NE(fd, INVALID_SOCKET_T);

  fell::repl::FetchLogReq req{};
  req.topic_len = 6;
  std::memcpy(req.topic, "orders", 6);
  req.partition = fell::platform::host_to_be16(1);
  req.start_offset = fell::platform::host_to_be64(0);
  req.follower_id = fell::platform::host_to_be32(1);

  const uint32_t frame_len = 1 + sizeof(req);
  const uint32_t net_len = fell::platform::host_to_be32(frame_len);
  const uint8_t opcode = static_cast<uint8_t>(fell::repl::ReplOp::FETCH_LOG);
  ASSERT_GT(fell::platform::send_data(fd, &net_len, sizeof(net_len)), 0);
  ASSERT_GT(fell::platform::send_data(fd, &opcode, 1), 0);
  ASSERT_GT(fell::platform::send_data(fd, &req, sizeof(req)), 0);

  uint8_t resp_opcode = 0;
  std::vector<uint8_t> resp_payload;
  ASSERT_TRUE(recv_frame(fd, resp_opcode, resp_payload, 2000));
  EXPECT_EQ(resp_opcode, static_cast<uint8_t>(fell::repl::ReplOp::REPLICA_SYNC));
  ASSERT_GE(resp_payload.size(), sizeof(fell::repl::ReplicaSyncHeader));
  auto *hdr = reinterpret_cast<const fell::repl::ReplicaSyncHeader *>(resp_payload.data());
  EXPECT_EQ(fell::platform::be16_to_host(hdr->partition), 1);

  fell::platform::close_socket(fd);
  server.stop();
}

TEST_F(ReplicationIntegrationTest, BrokerReplicatesCommittedBatchesForAcksOne) {
  {
    fell::TopicRegistry registry("test-broker-repl");
    ASSERT_TRUE(registry.create_topic("orders", 1));
  }

  fell::Broker broker("test-broker-repl");
  std::thread broker_thread([&broker]() { broker.run(7715); });
  std::this_thread::sleep_for(200ms);

  socket_t follower_fd = connect_with_retry("127.0.0.1", 8700, 2000);
  ASSERT_NE(follower_fd, INVALID_SOCKET_T);

  fell::repl::FetchLogReq fetch_req{};
  fetch_req.topic_len = 6;
  std::memcpy(fetch_req.topic, "orders", 6);
  fetch_req.partition = fell::platform::host_to_be16(0);
  fetch_req.start_offset = fell::platform::host_to_be64(0);
  fetch_req.follower_id = fell::platform::host_to_be32(1);

  const uint32_t fetch_frame_len = 1 + sizeof(fetch_req);
  const uint32_t fetch_net_len = fell::platform::host_to_be32(fetch_frame_len);
  const uint8_t fetch_opcode = static_cast<uint8_t>(fell::repl::ReplOp::FETCH_LOG);
  ASSERT_GT(fell::platform::send_data(follower_fd, &fetch_net_len, sizeof(fetch_net_len)), 0);
  ASSERT_GT(fell::platform::send_data(follower_fd, &fetch_opcode, 1), 0);
  ASSERT_GT(fell::platform::send_data(follower_fd, &fetch_req, sizeof(fetch_req)), 0);

  uint8_t initial_opcode = 0;
  std::vector<uint8_t> initial_payload;
  ASSERT_TRUE(recv_frame(follower_fd, initial_opcode, initial_payload, 2000));
  EXPECT_EQ(initial_opcode, static_cast<uint8_t>(fell::repl::ReplOp::REPLICA_SYNC_END));

  socket_t producer_fd = connect_with_retry("127.0.0.1", 7715, 2000);
  ASSERT_NE(producer_fd, INVALID_SOCKET_T);
  const auto publish = make_publish_frame("orders", 0, {0x10, 0x20, 0x30});
  ASSERT_GT(fell::platform::send_data(producer_fd, publish.data(), publish.size()), 0);

  uint8_t producer_opcode = 0;
  std::vector<uint8_t> producer_payload;
  ASSERT_TRUE(recv_frame(producer_fd, producer_opcode, producer_payload, 2000));
  EXPECT_EQ(producer_opcode, static_cast<uint8_t>(fell::Op::ACK));

  uint8_t repl_opcode = 0;
  std::vector<uint8_t> repl_payload;
  ASSERT_TRUE(recv_frame(follower_fd, repl_opcode, repl_payload, 2000));
  EXPECT_EQ(repl_opcode, static_cast<uint8_t>(fell::repl::ReplOp::REPLICA_SYNC));
  ASSERT_GE(repl_payload.size(), sizeof(fell::repl::ReplicaSyncHeader));
  const auto *hdr = reinterpret_cast<const fell::repl::ReplicaSyncHeader *>(repl_payload.data());
  EXPECT_EQ(fell::platform::be16_to_host(hdr->partition), 0);
  EXPECT_EQ(fell::platform::be64_to_host(hdr->offset), 0u);

  fell::platform::close_socket(producer_fd);
  fell::platform::close_socket(follower_fd);
  broker.stop();
  broker_thread.join();
}
