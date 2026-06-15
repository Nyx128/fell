#include "broker/request_handler.hpp"
#include "storage/partition_store.hpp"

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

  RequestHandler::RequestHandler(TopicRegistry &registry, const repl::ClusterConfig *cfg,
                                 repl::PartitionMetaRegistry *meta_reg, DeferAckCb defer_cb)
      : registry_(registry), cfg_(cfg), meta_reg_(meta_reg), defer_cb_(std::move(defer_cb)) {
  }

  std::vector<uint8_t> RequestHandler::handle(const Frame &f, ConnectionState &conn) {
    switch (f.op) {
    case Op::METADATA_REQ:
      return handle_metadata_req(f);
    case Op::CREATE_TOPIC:
      return handle_create_topic(f);
    case Op::PUBLISH:
      return handle_publish(f, conn);
    case Op::PUBLISH_V2:
      return handle_publish_v2(f, conn);
    case Op::SUBSCRIBE:
      return handle_subscribe(f, conn);
    case Op::FETCH:
      return handle_fetch(f, conn);
    default:
      return encode_error(ErrCode::UNKNOWN_OP, "Unknown operation");
    }
  }

  std::vector<uint8_t> RequestHandler::handle_metadata_req(const Frame &f) {
    if (f.payload.size() < sizeof(proto::MetadataReq)) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "payload too small for METADATA_REQ");
    }
    const auto *req = reinterpret_cast<const proto::MetadataReq *>(f.payload.data());
    std::string topic_name(req->topic, req->topic_len);

    if (!cfg_ || !meta_reg_) {
      return encode_error(ErrCode::METADATA_UNAVAILABLE, "Cluster metadata unavailable");
    }

    uint16_t num_partitions = registry_.num_partitions(topic_name);

    std::vector<uint8_t> resp;
    // num_brokers (2), [id(4), host_len(1), host, client_port(2)]
    uint16_t num_brokers = static_cast<uint16_t>(1 + cfg_->peers.size());
    uint8_t num_brokers_buf[2];
    num_brokers_buf[0] = (num_brokers >> 8) & 0xFF;
    num_brokers_buf[1] = num_brokers & 0xFF;
    resp.insert(resp.end(), num_brokers_buf, num_brokers_buf + 2);

    auto add_broker = [&](uint32_t id, const std::string &host, uint16_t client_port) {
      uint8_t buf[5];
      write_be32(buf, id);
      buf[4] = static_cast<uint8_t>(host.size());
      resp.insert(resp.end(), buf, buf + 5);
      resp.insert(resp.end(), host.begin(), host.end());
      uint8_t port_buf[2];
      port_buf[0] = (client_port >> 8) & 0xFF;
      port_buf[1] = client_port & 0xFF;
      resp.insert(resp.end(), port_buf, port_buf + 2);
    };

    add_broker(cfg_->broker_id, "127.0.0.1", cfg_->client_port);
    for (const auto &peer : cfg_->peers) {
      add_broker(peer.broker_id, peer.host, peer.client_port);
    }

    // num_partitions (2), [partition_index(2), leader_id(4)]
    uint8_t num_part_buf[2];
    num_part_buf[0] = (num_partitions >> 8) & 0xFF;
    num_part_buf[1] = num_partitions & 0xFF;
    resp.insert(resp.end(), num_part_buf, num_part_buf + 2);

    uint32_t total_brokers = std::max<uint32_t>(1, static_cast<uint32_t>(1 + cfg_->peers.size()));
    for (uint16_t i = 0; i < num_partitions; ++i) {
      uint32_t leader_id = 0;
      try {
        auto &meta = meta_reg_->get(topic_name, i);
        leader_id = meta.leader_id;
      } catch (...) {
        leader_id = i % total_brokers;
      }

      uint8_t part_buf[6];
      part_buf[0] = (i >> 8) & 0xFF;
      part_buf[1] = i & 0xFF;
      write_be32(part_buf + 2, leader_id);
      resp.insert(resp.end(), part_buf, part_buf + 6);
    }

    return encode_frame(Op::METADATA_RESP, resp.data(), resp.size());
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

    // Seed partition roles for the new topic immediately.
    if (cfg_ && meta_reg_) {
      uint32_t num_brokers = std::max<uint32_t>(1, static_cast<uint32_t>(1 + cfg_->peers.size()));
      meta_reg_->assign_roles(cfg_->broker_id, num_brokers, {topic_name}, {num_partitions});
    }

    return encode_ack(0);
  }

  std::vector<uint8_t> RequestHandler::handle_publish(const Frame &f, ConnectionState &conn) {
    publish_requests_total_++;
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

    if (meta_reg_) {
      try {
        if (meta_reg_->get(topic_name, partition).role != repl::PartitionRole::Leader) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      } catch (...) {
        uint32_t total_brokers =
            std::max<uint32_t>(1, static_cast<uint32_t>(1 + cfg_->peers.size()));
        if ((partition % total_brokers) != cfg_->broker_id) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      }
    }

    Partition *p = registry_.get_partition(topic_name, partition);
    if (!p) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition registry lookup failed");
    }

    // Extract message payload and append to partition directly
    const uint8_t *payload_ptr = f.payload.data() + sizeof(proto::PublishReq);
    uint32_t payload_len = static_cast<uint32_t>(f.payload.size() - sizeof(proto::PublishReq));

    storage::AppendResult result = p->append(payload_ptr, payload_len);
    if (!result.accepted) {
      publish_busy_total_++;
      if (result.error == storage::AppendError::Closed) {
        return encode_error(ErrCode::INTERNAL_ERROR, "Partition is closed");
      }
      return encode_error(ErrCode::BUSY, "Partition append queue is full");
    }
    bytes_published_total_ += payload_len;

    if (cfg_ && cfg_->acks == -1 && defer_cb_) {
      defer_cb_(topic_name, partition, result.offset, encode_ack(result.offset), conn.fd);
      return {};
    }

    return encode_ack(result.offset);
  }

  // FNV-1a hash
  static uint64_t fnv1a(const char *data, size_t len) {
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < len; ++i) {
      hash ^= static_cast<uint8_t>(data[i]);
      hash *= 1099511628211ULL;
    }
    return hash;
  }

  std::vector<uint8_t> RequestHandler::handle_publish_v2(const Frame &f, ConnectionState &conn) {
    publish_requests_total_++;
    if (f.payload.size() < sizeof(proto::PublishV2Req)) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "payload too small for PUBLISH_V2 header");
    }

    const auto *req = reinterpret_cast<const proto::PublishV2Req *>(f.payload.data());
    uint32_t payload_size = read_be32(reinterpret_cast<const uint8_t *>(&req->payload_size));

    if (f.payload.size() != sizeof(proto::PublishV2Req) + payload_size) {
      return encode_error(ErrCode::MALFORMED_REQUEST, "Payload size mismatch in PUBLISH_V2");
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

    if (req->key_len > 0) {
      // Key-based deterministic routing
      partition = static_cast<uint16_t>(fnv1a(req->key, req->key_len) % total_partitions);
    } else if (partition == 0xFFFF) {
      // Round-robin fallback
      partition = conn.rr_counter % total_partitions;
      conn.rr_counter++;
    } else if (partition >= total_partitions) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition index out of bounds");
    }

    if (meta_reg_) {
      try {
        if (meta_reg_->get(topic_name, partition).role != repl::PartitionRole::Leader) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      } catch (...) {
        uint32_t total_brokers =
            std::max<uint32_t>(1, static_cast<uint32_t>(1 + cfg_->peers.size()));
        if ((partition % total_brokers) != cfg_->broker_id) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      }
    }

    Partition *p = registry_.get_partition(topic_name, partition);
    if (!p) {
      return encode_error(ErrCode::UNKNOWN_PARTITION, "Partition registry lookup failed");
    }

    const uint8_t *payload_ptr = f.payload.data() + sizeof(proto::PublishV2Req);
    uint32_t payload_len = static_cast<uint32_t>(f.payload.size() - sizeof(proto::PublishV2Req));

    storage::AppendResult result = p->append(payload_ptr, payload_len);
    if (!result.accepted) {
      publish_busy_total_++;
      if (result.error == storage::AppendError::Closed) {
        return encode_error(ErrCode::INTERNAL_ERROR, "Partition is closed");
      }
      return encode_error(ErrCode::BUSY, "Partition append queue is full");
    }
    bytes_published_total_ += payload_len;

    if (cfg_ && cfg_->acks == -1 && defer_cb_) {
      defer_cb_(topic_name, partition, result.offset, encode_ack(result.offset), conn.fd);
      return {};
    }

    return encode_ack(result.offset);
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

    if (meta_reg_) {
      try {
        if (meta_reg_->get(topic_name, partition).role != repl::PartitionRole::Leader) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      } catch (...) {
        uint32_t total_brokers =
            std::max<uint32_t>(1, static_cast<uint32_t>(1 + cfg_->peers.size()));
        if ((partition % total_brokers) != cfg_->broker_id) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      }
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

    if (meta_reg_) {
      try {
        if (meta_reg_->get(topic_name, partition).role != repl::PartitionRole::Leader) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      } catch (...) {
        uint32_t total_brokers =
            std::max<uint32_t>(1, static_cast<uint32_t>(1 + cfg_->peers.size()));
        if ((partition % total_brokers) != cfg_->broker_id) {
          return encode_error(ErrCode::NOT_LEADER, "Not leader for partition");
        }
      }
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