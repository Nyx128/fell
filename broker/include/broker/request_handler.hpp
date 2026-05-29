#pragma once

#include "broker/connection_state.hpp"
#include "broker/protocol.hpp"
#include "broker/topic_registry.hpp"
#include <vector>

namespace fell {

  /**
   * @class RequestHandler
   * @brief Stateless routing controller for network requests.
   * 
   * Design Insight:
   * Translates deserialized socket network frames into partition engine storage 
   * operations. Encodes the outcome back into length-prefixed big-endian network frames.
   * Promotes low latency by returning raw heap buffers (`std::vector<uint8_t>`) that 
   * can be written directly to network socket buffers.
   */
  class RequestHandler {
  public:
    /**
     * @brief Creates a request handler bound to the Topic Registry.
     */
    explicit RequestHandler(TopicRegistry &registry);

    // Disable copy
    RequestHandler(const RequestHandler &) = delete;
    RequestHandler &operator=(const RequestHandler &) = delete;

    /**
     * @brief Parses and routes a decoded network frame.
     * @param f Decoded network frame.
     * @param conn Session context tracking state (e.g. partition subscription indexes).
     * @return Encoded network byte array containing response, or empty vector.
     */
    std::vector<uint8_t> handle(const Frame &f, ConnectionState &conn);

  private:
    std::vector<uint8_t> handle_create_topic(const Frame &f);
    std::vector<uint8_t> handle_publish(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_subscribe(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_fetch(const Frame &f, ConnectionState &conn);

    TopicRegistry &registry_;

    static std::vector<uint8_t> encode_frame(Op op, const uint8_t *payload, size_t len);
    static std::vector<uint8_t> encode_ack(uint64_t value);
    static std::vector<uint8_t> encode_error(ErrCode code, const char *msg = "");
  };

} // namespace fell