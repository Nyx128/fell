#include <gtest/gtest.h>
#include "broker/request_handler.hpp"
#include "broker/topic_registry.hpp"
#include "broker/protocol.hpp"
#include <filesystem>
#include <vector>
#include <cstring>
#include <thread>
#include <chrono>

using namespace fell;

class RequestHandlerV2Test : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data-v2");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data-v2");
  }

  void wait_for_commit(Partition *p, uint64_t offset) {
    auto start = std::chrono::steady_clock::now();
    while (p->fetch(offset, 1).empty()) {
      if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) {
        FAIL() << "Timeout waiting for commit";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
};

static inline uint16_t swap_be16(uint16_t val) {
  return (val >> 8) | (val << 8);
}

static inline uint32_t swap_be32(uint32_t val) {
  return ((val >> 24) & 0x000000FF) | ((val >> 8) & 0x0000FF00) | ((val << 8) & 0x00FF0000) |
         ((val << 24) & 0xFF000000);
}

static uint64_t test_fnv1a(const char *data, size_t len) {
  uint64_t hash = 14695981039346656037ULL;
  for (size_t i = 0; i < len; ++i) {
    hash ^= static_cast<uint8_t>(data[i]);
    hash *= 1099511628211ULL;
  }
  return hash;
}

TEST_F(RequestHandlerV2Test, SuccessfulPublishV2WithKeys) {
  storage::StorageOptions opts;
  opts.batch_wait_us = 0; // instant processing

  TopicRegistry registry("test-data-v2", opts);
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  // Create topic
  proto::CreateTopicReq create_req = {};
  create_req.name_len = 5;
  std::memcpy(create_req.name, "sales", 5);
  create_req.num_partitions = swap_be16(3);
  Frame create_frame;
  create_frame.op = Op::CREATE_TOPIC;
  create_frame.payload.assign(reinterpret_cast<const uint8_t *>(&create_req),
                              reinterpret_cast<const uint8_t *>(&create_req) + sizeof(proto::CreateTopicReq));
  handler.handle(create_frame, conn);

  // Publish V2 requests with keys
  std::vector<std::string> keys = {"user-1", "user-2", "user-3", "user-12345", "billing-key-alpha"};
  std::string msg_val = "hello routing";

  for (const auto &key : keys) {
    proto::PublishV2Req pub_req = {};
    pub_req.topic_len = 5;
    std::memcpy(pub_req.topic, "sales", 5);
    pub_req.partition = swap_be16(0xFFFF); // Use routing key
    pub_req.key_len = static_cast<uint8_t>(key.size());
    std::memcpy(pub_req.key, key.data(), key.size());
    pub_req.payload_size = swap_be32(static_cast<uint32_t>(msg_val.size()));

    std::vector<uint8_t> payload(sizeof(proto::PublishV2Req) + msg_val.size());
    std::memcpy(payload.data(), &pub_req, sizeof(proto::PublishV2Req));
    std::memcpy(payload.data() + sizeof(proto::PublishV2Req), msg_val.data(), msg_val.size());

    Frame f;
    f.op = Op::PUBLISH_V2;
    f.payload.assign(payload.begin(), payload.end());

    uint32_t expected_partition = static_cast<uint32_t>(test_fnv1a(key.data(), key.size()) % 3);

    // Capture baseline next offset for expected partition
    Partition *p = registry.get_partition("sales", expected_partition);
    ASSERT_NE(p, nullptr);
    uint64_t initial_next = p->next_offset();

    std::vector<uint8_t> resp = handler.handle(f, conn);
    ASSERT_GE(resp.size(), 5);
    EXPECT_EQ(resp[4], static_cast<uint8_t>(Op::ACK));

    // Wait for async commit thread to flush
    wait_for_commit(p, initial_next);

    // Next offset should have incremented by 1
    EXPECT_EQ(p->next_offset(), initial_next + 1);
  }
}

TEST_F(RequestHandlerV2Test, PublishV2RoundRobinFallback) {
  storage::StorageOptions opts;
  opts.batch_wait_us = 0; // instant processing

  TopicRegistry registry("test-data-v2", opts);
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;
  conn.rr_counter = 0;

  // Create topic
  proto::CreateTopicReq create_req = {};
  create_req.name_len = 5;
  std::memcpy(create_req.name, "sales", 5);
  create_req.num_partitions = swap_be16(3);
  Frame create_frame;
  create_frame.op = Op::CREATE_TOPIC;
  create_frame.payload.assign(reinterpret_cast<const uint8_t *>(&create_req),
                              reinterpret_cast<const uint8_t *>(&create_req) + sizeof(proto::CreateTopicReq));
  handler.handle(create_frame, conn);

  // Send 6 messages without keys (partition = 0xFFFF, key_len = 0)
  // Expecting perfectly balanced round-robin distribution: 2 messages per partition
  std::string msg_val = "no key msg";
  for (int i = 0; i < 6; ++i) {
    proto::PublishV2Req pub_req = {};
    pub_req.topic_len = 5;
    std::memcpy(pub_req.topic, "sales", 5);
    pub_req.partition = swap_be16(0xFFFF);
    pub_req.key_len = 0;
    pub_req.payload_size = swap_be32(static_cast<uint32_t>(msg_val.size()));

    std::vector<uint8_t> payload(sizeof(proto::PublishV2Req) + msg_val.size());
    std::memcpy(payload.data(), &pub_req, sizeof(proto::PublishV2Req));
    std::memcpy(payload.data() + sizeof(proto::PublishV2Req), msg_val.data(), msg_val.size());

    Frame f;
    f.op = Op::PUBLISH_V2;
    f.payload.assign(payload.begin(), payload.end());

    std::vector<uint8_t> resp = handler.handle(f, conn);
    ASSERT_GE(resp.size(), 5);
    EXPECT_EQ(resp[4], static_cast<uint8_t>(Op::ACK));
  }

  // Wait for commits
  wait_for_commit(registry.get_partition("sales", 0), 1);
  wait_for_commit(registry.get_partition("sales", 1), 1);
  wait_for_commit(registry.get_partition("sales", 2), 1);

  EXPECT_EQ(registry.get_partition("sales", 0)->next_offset(), 2);
  EXPECT_EQ(registry.get_partition("sales", 1)->next_offset(), 2);
  EXPECT_EQ(registry.get_partition("sales", 2)->next_offset(), 2);
}

TEST_F(RequestHandlerV2Test, PublishV2BusyErrorHandling) {
  // Use a topic registry with StorageOptions optimized for triggering BUSY
  storage::StorageOptions opts;
  opts.queue_capacity = 1;
  opts.max_pending_bytes = 1024 * 1024;
  opts.batch_wait_us = 1000000; // Large batch wait (1s) to prevent processing the queue
  TopicRegistry registry("test-data-v2", opts);
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  // Create topic
  proto::CreateTopicReq create_req = {};
  create_req.name_len = 5;
  std::memcpy(create_req.name, "sales", 5);
  create_req.num_partitions = swap_be16(1);
  Frame create_frame;
  create_frame.op = Op::CREATE_TOPIC;
  create_frame.payload.assign(reinterpret_cast<const uint8_t *>(&create_req),
                              reinterpret_cast<const uint8_t *>(&create_req) + sizeof(proto::CreateTopicReq));
  handler.handle(create_frame, conn);

  // Send first message (should fit in the queue or be currently processed)
  std::string msg_val = "msg";
  proto::PublishV2Req pub_req = {};
  pub_req.topic_len = 5;
  std::memcpy(pub_req.topic, "sales", 5);
  pub_req.partition = swap_be16(0);
  pub_req.key_len = 0;
  pub_req.payload_size = swap_be32(static_cast<uint32_t>(msg_val.size()));

  std::vector<uint8_t> payload(sizeof(proto::PublishV2Req) + msg_val.size());
  std::memcpy(payload.data(), &pub_req, sizeof(proto::PublishV2Req));
  std::memcpy(payload.data() + sizeof(proto::PublishV2Req), msg_val.data(), msg_val.size());

  Frame f;
  f.op = Op::PUBLISH_V2;
  f.payload.assign(payload.begin(), payload.end());

  auto resp1 = handler.handle(f, conn);
  // Send second message immediately (should saturate/exceed the queue_capacity = 1 and return BUSY)
  // We try a few times in a loop since the I/O thread might be extremely fast, but with 1s batch wait it should hit BUSY.
  bool hit_busy = false;
  for (int i = 0; i < 5; ++i) {
    auto resp = handler.handle(f, conn);
    ASSERT_GE(resp.size(), 5);
    if (resp[4] == static_cast<uint8_t>(Op::ERR)) {
      const auto *err = reinterpret_cast<const proto::ErrorResp *>(resp.data() + 5);
      if (err->code == static_cast<uint8_t>(ErrCode::BUSY)) {
        hit_busy = true;
        break;
      }
    }
  }
  EXPECT_TRUE(hit_busy);
}
