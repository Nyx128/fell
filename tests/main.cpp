#ifdef NDEBUG
#undef NDEBUG
#endif
#include "broker/connection_state.hpp"
#include "broker/frame_decoder.hpp"
#include "broker/protocol.hpp"
#include "broker/request_handler.hpp"
#include "broker/topic_registry.hpp"
#include <cassert>
#include <cstring>
#include <iostream>
#include <vector>

using namespace fell;

// Endianness serialization/deserialization helpers for verifying headers in tests
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

void test_frame_decoder() {
  std::cout << "[Test] Running FrameDecoder tests..." << std::endl;
  FrameDecoder decoder;
  std::vector<Frame> out;

  // 1. Parse a complete valid frame
  // Length: 2 (1-byte Op + 1-byte Payload) -> big-endian: {0x00, 0x00, 0x00, 0x02}
  // Opcode: 0x01, Payload: {0xAA}
  uint8_t complete_frame[] = {0x00, 0x00, 0x00, 0x02, 0x01, 0xAA};
  int count = decoder.push(complete_frame, sizeof(complete_frame), out);
  (void)count;
  assert(count == 1);
  assert(out.size() == 1);
  assert(out[0].op == static_cast<Op>(0x01));
  assert(out[0].payload.size() == 1);
  assert(out[0].payload[0] == 0xAA);

  out.clear();
  decoder.reset();

  // 2. Parse fragmented/partial frame inputs
  uint8_t chunk1[] = {0x00, 0x00};
  assert(decoder.push(chunk1, sizeof(chunk1), out) == 0); // Not enough for size

  uint8_t chunk2[] = {0x00, 0x03, 0x02};
  assert(decoder.push(chunk2, sizeof(chunk2), out) ==
         0); // size read as 3, but only 1 byte of payload present

  uint8_t chunk3[] = {0xBB, 0xCC};
  assert(decoder.push(chunk3, sizeof(chunk3), out) == 1); // Completed!
  assert(out.size() == 1);
  assert(out[0].op == static_cast<Op>(0x02));
  assert(out[0].payload.size() == 2);
  assert(out[0].payload[0] == 0xBB);
  assert(out[0].payload[1] == 0xCC);

  out.clear();
  decoder.reset();

  // 3. Parse multiple frames in a single buffer
  // Frame A: Len = 1, Op = 0x03
  // Frame B: Len = 2, Op = 0x04, Payload = {0xDD}
  uint8_t multi_frame[] = {0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0x04, 0xDD};
  assert(decoder.push(multi_frame, sizeof(multi_frame), out) == 2);
  assert(out.size() == 2);
  assert(out[0].op == static_cast<Op>(0x03));
  assert(out[0].payload.empty());
  assert(out[1].op == static_cast<Op>(0x04));
  assert(out[1].payload.size() == 1);
  assert(out[1].payload[0] == 0xDD);

  std::cout << "[Test] FrameDecoder tests passed!" << std::endl;
}

void test_topic_registry() {
  std::cout << "[Test] Running TopicRegistry tests..." << std::endl;
  TopicRegistry registry;

  // Create topic with 3 partitions
  assert(registry.create_topic("sales", 3) == true);
  // Topic duplicate creation should fail
  assert(registry.create_topic("sales", 3) == false);

  assert(registry.num_partitions("sales") == 3);
  assert(registry.num_partitions("non-existent") == 0);

  Partition *p0 = registry.get_partition("sales", 0);
  Partition *p1 = registry.get_partition("sales", 1);
  Partition *p2 = registry.get_partition("sales", 2);
  Partition *p3 = registry.get_partition("sales", 3); // out of bounds

  (void)p1;
  (void)p2;
  (void)p3;
  assert(p0 != nullptr);
  assert(p1 != nullptr);
  assert(p2 != nullptr);
  assert(p3 == nullptr);

  // Append to partition 0
  std::vector<uint8_t> m1 = {0x10, 0x20};
  std::vector<uint8_t> m2 = {0x30};

  assert(p0->append(m1) == 0);
  assert(p0->append(m2) == 1);

  // Fetch from partition 0
  std::vector<Message> fetched = p0->fetch(0, 10);
  (void)fetched;
  assert(fetched.size() == 2);
  assert(fetched[0].offset == 0);
  assert(fetched[0].payload == m1);
  assert(fetched[1].offset == 1);
  assert(fetched[1].payload == m2);

  std::cout << "[Test] TopicRegistry tests passed!" << std::endl;
}

void test_request_handler() {
  std::cout << "[Test] Running RequestHandler tests..." << std::endl;
  TopicRegistry registry;
  RequestHandler handler(registry);
  ConnectionState conn;
  conn.fd = 99;

  // 1. Test too-small payload boundary validation
  Frame invalid_frame;
  invalid_frame.op = Op::CREATE_TOPIC;
  invalid_frame.payload.resize(2); // CreateTopicReq requires 258 bytes
  std::vector<uint8_t> err_resp = handler.handle(invalid_frame, conn);

  // Convert error response bytes back into frame structure
  // (We know frame header is 5 bytes: 4 len, 1 op)
  (void)err_resp;
  assert(err_resp.size() >= 5);
  assert(err_resp[4] == static_cast<uint8_t>(Op::ERROR));
  const auto *err = reinterpret_cast<const proto::ErrorResp *>(err_resp.data() + 5);
  (void)err;
  assert(err->code == static_cast<uint8_t>(ErrCode::MALFORMED_REQUEST));

  // 2. Test successful topic creation
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
  (void)create_resp;
  assert(create_resp.size() >= 5);
  assert(create_resp[4] == static_cast<uint8_t>(Op::ACK));
  assert(registry.num_partitions("billing") == 2);

  // 3. Test topic duplicate creation fails
  std::vector<uint8_t> create_dup_resp = handler.handle(create_frame, conn);
  (void)create_dup_resp;
  assert(create_dup_resp[4] == static_cast<uint8_t>(Op::ERROR));
  const auto *err_dup = reinterpret_cast<const proto::ErrorResp *>(create_dup_resp.data() + 5);
  (void)err_dup;
  assert(err_dup->code == static_cast<uint8_t>(ErrCode::MALFORMED_REQUEST));

  // 4. Test publish
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
  pub_frame.payload = pub_payload;

  std::vector<uint8_t> pub_resp = handler.handle(pub_frame, conn);
  (void)pub_resp;
  assert(pub_resp.size() >= 5);
  assert(pub_resp[4] == static_cast<uint8_t>(Op::ACK));
  const auto *ack = reinterpret_cast<const proto::AckResp *>(pub_resp.data() + 5);
  (void)ack;
  assert(swap_be64(ack->value) == 0); // First message offset should be 0

  std::cout << "[Test] RequestHandler tests passed!" << std::endl;
}

int main() {
  test_frame_decoder();
  test_topic_registry();
  test_request_handler();
  std::cout << "All unit tests completed successfully!" << std::endl;
  return 0;
}
