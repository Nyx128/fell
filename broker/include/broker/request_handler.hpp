#pragma once

#include "broker/connection_state.hpp"
#include "broker/protocol.hpp"
#include "broker/topic_registry.hpp"

namespace fell {
  class RequestHandler {
  public:
    explicit RequestHandler(TopicRegistry &registry);

    // Dispatches frame to the correct private handler.
    // Returns serialised response bytes ready to send(). May be empty.
    std::vector<uint8_t> handle(const Frame &f, ConnectionState &conn);

  private:
    std::vector<uint8_t> handle_create_topic(const Frame &f);
    std::vector<uint8_t> handle_publish(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_subscribe(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_fetch(const Frame &f, ConnectionState &conn);

    TopicRegistry &registry_;

    // Wraps op + payload bytes into a full length-prefixed frame.
    // length field = 1 (op byte) + payload.size(), big-endian.
    static std::vector<uint8_t> encode_frame(Op op, const uint8_t *payload, size_t len);
    // Shorthand for an ACK frame carrying a uint64 value.
    static std::vector<uint8_t> encode_ack(uint64_t value);
    // Shorthand for an ERROR frame.
    static std::vector<uint8_t> encode_error(ErrCode code, const char *msg = "");
  };

} // namespace fell