#include "broker/protocol.hpp"
#include "broker/request_handler.hpp"
#include "broker/topic_registry.hpp"
#include <cstring>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

using namespace fell;

class RequestHandlerTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }
};

static inline uint16_t swap_be16(uint16_t val) {
  return (val >> 8) | (val << 8);
}

static inline uint32_t swap_be32(uint32_t val) {
  return ((val >> 24) & 0x000000FF) | ((val >> 8) & 0x0000FF00) | ((val << 8) & 0x00FF0000) |
         ((val << 24) & 0xFF000000);
}

static inline uint64_t swap_be64(uint64_t val) {
  return ((val >> 56) & 0x00000000000000FFULL) | ((val >> 40) & 0x000000000000FF00ULL) |
         ((val >> 24) & 0x0000000000FF0000ULL) | ((val >> 8) & 0x00000000FF000000ULL) |
         ((val << 8) & 0x000000FF00000000ULL) | ((val << 24) & 0x0000FF0000000000ULL) |
         ((val << 40) & 0x00FF000000000000ULL) | ((val << 56) & 0xFF00000000000000ULL);
}

TEST_F(RequestHandlerTest, TooSmallPayloadBoundaryValidation) {
  TopicRegistry registry("test-data");
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  Frame invalid_frame;
  invalid_frame.op = Op::CREATE_TOPIC;
  std::vector<uint8_t> dummy(2, 0);
  invalid_frame.payload.assign(dummy.begin(), dummy.end()); // CreateTopicReq requires 258 bytes

  std::vector<uint8_t> err_resp = handler.handle(invalid_frame, conn);

  ASSERT_GE(err_resp.size(), 5);
  EXPECT_EQ(err_resp[4], static_cast<uint8_t>(Op::ERR));
  const auto *err = reinterpret_cast<const proto::ErrorResp *>(err_resp.data() + 5);
  EXPECT_EQ(err->code, static_cast<uint8_t>(ErrCode::MALFORMED_REQUEST));
}

TEST_F(RequestHandlerTest, SuccessfulTopicCreation) {
  TopicRegistry registry("test-data");
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  proto::CreateTopicReq create_req = {};
  create_req.name_len = 7;
  std::memcpy(create_req.name, "billing", 7);
  create_req.num_partitions = swap_be16(2);

  Frame create_frame;
  create_frame.op = Op::CREATE_TOPIC;
  create_frame.payload.assign(reinterpret_cast<const uint8_t *>(&create_req),
                              reinterpret_cast<const uint8_t *>(&create_req) +
                                  sizeof(proto::CreateTopicReq));

  std::vector<uint8_t> create_resp = handler.handle(create_frame, conn);

  ASSERT_GE(create_resp.size(), 5);
  EXPECT_EQ(create_resp[4], static_cast<uint8_t>(Op::ACK));
  EXPECT_EQ(registry.num_partitions("billing"), 2);
}

TEST_F(RequestHandlerTest, TopicDuplicateCreationFails) {
  TopicRegistry registry("test-data");
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  proto::CreateTopicReq create_req = {};
  create_req.name_len = 7;
  std::memcpy(create_req.name, "billing", 7);
  create_req.num_partitions = swap_be16(2);

  Frame create_frame;
  create_frame.op = Op::CREATE_TOPIC;
  create_frame.payload.assign(reinterpret_cast<const uint8_t *>(&create_req),
                              reinterpret_cast<const uint8_t *>(&create_req) +
                                  sizeof(proto::CreateTopicReq));

  handler.handle(create_frame, conn); // first creation

  std::vector<uint8_t> create_dup_resp = handler.handle(create_frame, conn);

  EXPECT_EQ(create_dup_resp[4], static_cast<uint8_t>(Op::ERR));
  const auto *err_dup = reinterpret_cast<const proto::ErrorResp *>(create_dup_resp.data() + 5);
  EXPECT_EQ(err_dup->code, static_cast<uint8_t>(ErrCode::MALFORMED_REQUEST));
}

TEST_F(RequestHandlerTest, PublishToTopic) {
  TopicRegistry registry("test-data");
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  // First create the topic
  proto::CreateTopicReq create_req = {};
  create_req.name_len = 7;
  std::memcpy(create_req.name, "billing", 7);
  create_req.num_partitions = swap_be16(2);
  Frame create_frame;
  create_frame.op = Op::CREATE_TOPIC;
  create_frame.payload.assign(reinterpret_cast<const uint8_t *>(&create_req),
                              reinterpret_cast<const uint8_t *>(&create_req) +
                                  sizeof(proto::CreateTopicReq));
  handler.handle(create_frame, conn);

  // Publish
  proto::PublishReq pub_req = {};
  pub_req.topic_len = 7;
  std::memcpy(pub_req.topic, "billing", 7);
  pub_req.partition = swap_be16(0); // target partition 0
  std::string test_msg = "test payload";
  pub_req.payload_size = swap_be32(static_cast<uint32_t>(test_msg.size()));

  std::vector<uint8_t> pub_payload(sizeof(proto::PublishReq) + test_msg.size());
  std::memcpy(pub_payload.data(), &pub_req, sizeof(proto::PublishReq));
  std::memcpy(pub_payload.data() + sizeof(proto::PublishReq), test_msg.data(), test_msg.size());

  Frame pub_frame;
  pub_frame.op = Op::PUBLISH;
  pub_frame.payload.assign(pub_payload.begin(), pub_payload.end());

  std::vector<uint8_t> pub_resp = handler.handle(pub_frame, conn);

  ASSERT_GE(pub_resp.size(), 5);
  EXPECT_EQ(pub_resp[4], static_cast<uint8_t>(Op::ACK));
  const auto *ack = reinterpret_cast<const proto::AckResp *>(pub_resp.data() + 5);
  EXPECT_EQ(swap_be64(ack->value), 0); // First message offset should be 0
}
