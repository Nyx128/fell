#include "broker/request_handler.hpp"

namespace fell {

  // Endianness serialization/deserialization helpers
  static inline uint16_t read_be16(const uint8_t *p) {
    return (static_cast<uint16_t>(p[0]) << 8) | static_cast<uint16_t>(p[1]);
  }

  static inline uint32_t read_be32(const uint8_t *p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
  }

  static inline uint64_t read_be64(const uint8_t *p) {
    return (static_cast<uint64_t>(p[0]) << 56) | (static_cast<uint64_t>(p[1]) << 48) |
           (static_cast<uint64_t>(p[2]) << 40) | (static_cast<uint64_t>(p[3]) << 32) |
           (static_cast<uint64_t>(p[4]) << 24) | (static_cast<uint64_t>(p[5]) << 16) |
           (static_cast<uint64_t>(p[6]) << 8) | static_cast<uint64_t>(p[7]);
  }

  static inline void write_be32(uint8_t *p, uint32_t val) {
    p[0] = static_cast<uint8_t>((val >> 24) & 0xFF);
    p[1] = static_cast<uint8_t>((val >> 16) & 0xFF);
    p[2] = static_cast<uint8_t>((val >> 8) & 0xFF);
    p[3] = static_cast<uint8_t>(val & 0xFF);
  }

  static inline void write_be64(uint8_t *p, uint64_t val) {
    p[0] = static_cast<uint8_t>((val >> 56) & 0xFF);
    p[1] = static_cast<uint8_t>((val >> 48) & 0xFF);
    p[2] = static_cast<uint8_t>((val >> 40) & 0xFF);
    p[3] = static_cast<uint8_t>((val >> 32) & 0xFF);
    p[4] = static_cast<uint8_t>((val >> 24) & 0xFF);
    p[5] = static_cast<uint8_t>((val >> 16) & 0xFF);
    p[6] = static_cast<uint8_t>((val >> 8) & 0xFF);
    p[7] = static_cast<uint8_t>(val & 0xFF);
  }

  RequestHandler::RequestHandler(TopicRegistry &registry) : registry_(registry) {
  }

  std::vector<uint8_t> RequestHandler::handle(const Frame &f, ConnectionState &conn) {
    switch (f.op) {
    case Op::CREATE_TOPIC:
      return handle_create_topic(f);
    case Op::PUBLISH:
      return handle_publish(f, conn);
    case Op::SUBSCRIBE:
      return handle_subscribe(f, conn);
    case Op::FETCH:
      return handle_fetch(f, conn);
    default:
      return encode_error(ErrCode::UNKNOWN_OP, "Unknown operation");
    }
  }

  std::vector<uint8_t> RequestHandler::handle_create_topic(const Frame &f) {
    if (f.payload.size() < sizeof(proto::CreateTopicReq)) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "payload too small for CREATE_TOPIC");
    }

    const auto *req = reinterpret_cast<const proto::CreateTopicReq *>(f.payload.data());

    // Validate topic name is non-empty
    if (req->name_len == 0) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Topic name cannot be empty");
    }

    std::string topic_name(req->name, req->name_len);

    uint16_t num_partitions = read_be16(reinterpret_cast<const uint8_t *>(&req->num_partitions));
    if (num_partitions == 0) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Partition count must be at least 1");
    }

    bool success = registry_.create_topic(topic_name, num_partitions);
    if (!success) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Topic already exists");
    }
    return encode_ack(0);
  }

  std::vector<uint8_t> RequestHandler::handle_publish(const Frame &f, ConnectionState &conn) {
    if (f.payload.size() < sizeof(proto::PublishReq)) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "payload too small for PUBLISH header");
    }

    const auto *req = reinterpret_cast<const proto::PublishReq *>(f.payload.data());

    // Read and convert payload size from big-endian
    uint32_t payload_size = read_be32(reinterpret_cast<const uint8_t *>(&req->payload_size));

    // frame size must exactly match header + payload size
    if (f.payload.size() != sizeof(proto::PublishReq) + payload_size) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Payload size mismatch in PUBLISH");
    }

    if (req->topic_len == 0) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Topic name cannot be empty");
    }

    std::string topic_name(req->topic, req->topic_len);
    uint16_t partition = read_be16(reinterpret_cast<const uint8_t *>(&req->partition));
    uint16_t total_partitions = registry_.num_partitions(topic_name);
    if (total_partitions == 0) {
      return encode_error(ErrCode::UNKNOWN_TOPIC, "Topic does not exist");
    }

    // Handle round-robin partition selection if requested (0xFFFF)
    if (partition == 0xFFFF) {
      partition = conn.rr_counter % total_partitions;
      conn.rr_counter++;
    } else if (partition >= total_partitions) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition index out of bounds");
    }

    Partition *p = registry_.get_partition(topic_name, partition);
    if (!p) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition registry lookup failed");
    }

    // Extract message payload and append to partition
    std::vector<uint8_t> msg_payload(f.payload.begin() + sizeof(proto::PublishReq),
                                     f.payload.end());
    uint64_t offset = p->append(std::move(msg_payload));
    return encode_ack(offset);
  }

  std::vector<uint8_t> RequestHandler::handle_subscribe(const Frame &f, ConnectionState &conn) {
    // Boundary check
    if (f.payload.size() < sizeof(proto::SubscribeReq)) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Payload too small for SUBSCRIBE");
    }

    const auto *req = reinterpret_cast<const proto::SubscribeReq *>(f.payload.data());

    if (req->topic_len == 0) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Topic name cannot be empty");
    }

    std::string topic_name(req->topic, req->topic_len);
    uint16_t partition = read_be16(reinterpret_cast<const uint8_t *>(&req->partition));
    uint64_t start_offset = read_be64(reinterpret_cast<const uint8_t *>(&req->start_offset));

    // Validate topic and partition
    uint16_t total_partitions = registry_.num_partitions(topic_name);
    if (total_partitions == 0) {
      return encode_error(ErrCode::UNKNOWN_TOPIC, "Topic does not exist");
    }

    if (partition >= total_partitions) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition index out of bounds");
    }

    Partition *p = registry_.get_partition(topic_name, partition);
    if (!p) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition registry lookup failed");
    }

    // Verify the requested offset does not lie in the future.
    if (start_offset != 0xFFFFFFFFFFFFFFFFULL && start_offset > p->next_offset()) {
      return encode_error(ErrCode::INVALID_OFFSET, "Start offset lies in the future");
    }

    // Update the connection subscription state
    conn.sub_topic = topic_name;
    conn.sub_partition = partition;
    if (start_offset == 0xFFFFFFFFFFFFFFFFULL) {
      conn.fetch_offset = p->next_offset();
    } else {
      conn.fetch_offset = start_offset;
    }
    conn.subscribed = true;

    return encode_ack(conn.fetch_offset);
  }

  std::vector<uint8_t> RequestHandler::handle_fetch(const Frame &f, ConnectionState &conn) {
    // Boundary check
    if (f.payload.size() < sizeof(proto::FetchReq)) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Payload too small for FETCH");
    }

    const auto *req = reinterpret_cast<const proto::FetchReq *>(f.payload.data());

    if (req->topic_len == 0) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Topic name cannot be empty");
    }

    std::string topic_name(req->topic, req->topic_len);
    uint16_t partition = read_be16(reinterpret_cast<const uint8_t *>(&req->partition));
    uint64_t offset = read_be64(reinterpret_cast<const uint8_t *>(&req->offset));
    uint16_t max_messages = read_be16(reinterpret_cast<const uint8_t *>(&req->max_messages));

    // Validate topic and partition
    uint16_t total_partitions = registry_.num_partitions(topic_name);
    if (total_partitions == 0) {
      return encode_error(ErrCode::UNKNOWN_TOPIC, "Topic does not exist");
    }

    if (partition >= total_partitions) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition index out of bounds");
    }

    Partition *p = registry_.get_partition(topic_name, partition);
    if (!p) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition registry lookup failed");
    }

    // Fetch from partition (returns empty if offset >= next_offset)
    std::vector<Message> messages = p->fetch(offset, max_messages);
    std::vector<uint8_t> response_bytes;

    // Encode each message into its own FETCH_RESPONSE frame and concatenate
    for (const auto &msg : messages) {
      proto::FetchResponseHeader header = {};
      write_be64(reinterpret_cast<uint8_t *>(&header.offset), msg.offset);
      write_be64(reinterpret_cast<uint8_t *>(&header.timestamp_ms), msg.timestamp_ms);
      write_be32(reinterpret_cast<uint8_t *>(&header.payload_size),
                 static_cast<uint32_t>(msg.payload.size()));

      // Construct header + payload buffer for the single frame
      std::vector<uint8_t> frame_payload(sizeof(proto::FetchResponseHeader) + msg.payload.size());
      std::memcpy(frame_payload.data(), &header, sizeof(proto::FetchResponseHeader));
      if (!msg.payload.empty()) {
        std::memcpy(frame_payload.data() + sizeof(proto::FetchResponseHeader), msg.payload.data(),
                    msg.payload.size());
      }

      // Encode single message as an individual FETCH_RESPONSE frame
      std::vector<uint8_t> msg_frame =
          encode_frame(Op::FETCH_RESPONSE, frame_payload.data(), frame_payload.size());

      // Append to the total return byte stream
      response_bytes.insert(response_bytes.end(), msg_frame.begin(), msg_frame.end());

      // Advance connection fetch offset to the next message
      conn.fetch_offset = msg.offset + 1;
    }

    // If no messages were found, return a success ACK(0) rather than an error
    if (response_bytes.empty()) {
      return encode_ack(0);
    }

    return response_bytes;
  }

  std::vector<uint8_t> RequestHandler::encode_frame(Op op, const uint8_t *payload, size_t len) {
    std::vector<uint8_t> frame;
    frame.resize(4 + 1 + len);

    uint32_t frame_len = 1 + static_cast<uint32_t>(len);
    write_be32(frame.data(), frame_len);

    frame[4] = static_cast<uint8_t>(op);
    if (len > 0 && payload != nullptr) {
      std::memcpy(frame.data() + 5, payload, len);
    }

    return frame;
  }

  std::vector<uint8_t> RequestHandler::encode_ack(uint64_t value) {
    proto::AckResp resp = {};
    write_be64(reinterpret_cast<uint8_t *>(&resp.value), value);
    return encode_frame(Op::ACK, reinterpret_cast<const uint8_t *>(&resp), sizeof(proto::AckResp));
  }

  std::vector<uint8_t> RequestHandler::encode_error(ErrCode code, const char *msg) {
    proto::ErrorResp resp = {};
    resp.code = static_cast<uint8_t>(code);
    size_t len = std::strlen(msg);
    if (len > 255) {
      len = 255;
    }
    resp.msg_len = static_cast<uint8_t>(len);
    std::memcpy(resp.msg, msg, len);

    return encode_frame(Op::ERR, reinterpret_cast<const uint8_t *>(&resp),
                        sizeof(proto::ErrorResp));
  }

} // namespace fell