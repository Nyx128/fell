#pragma once

#include "broker/connection_state.hpp"
#include "broker/protocol.hpp"
#include "broker/topic_registry.hpp"
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include <atomic>
#include <functional>
#include <vector>

namespace fell {

  class RequestHandler {
  public:
    using DeferAckCb =
        std::function<void(const std::string &topic, uint16_t partition, uint64_t offset,
                           std::vector<uint8_t> ack_resp, int producer_fd)>;

    RequestHandler(TopicRegistry &registry, const repl::ClusterConfig *cfg = nullptr,
                   repl::PartitionMetaRegistry *meta_reg = nullptr, DeferAckCb defer_cb = nullptr);

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
    std::vector<uint8_t> handle_metadata_req(const Frame &f);
    std::vector<uint8_t> handle_create_topic(const Frame &f);
    std::vector<uint8_t> handle_publish(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_publish_v2(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_subscribe(const Frame &f, ConnectionState &conn);
    std::vector<uint8_t> handle_fetch(const Frame &f, ConnectionState &conn);

    TopicRegistry &registry_;
    const repl::ClusterConfig *cfg_;
    repl::PartitionMetaRegistry *meta_reg_;
    DeferAckCb defer_cb_;

    // Phase 3 Metrics
    std::atomic<uint64_t> publish_requests_total_{0};
    std::atomic<uint64_t> publish_busy_total_{0};
    std::atomic<uint64_t> bytes_published_total_{0};

    static std::vector<uint8_t> encode_frame(Op op, const uint8_t *payload, size_t len);
    static std::vector<uint8_t> encode_ack(uint64_t value);
    static std::vector<uint8_t> encode_error(ErrCode code, const char *msg = "");
  };

} // namespace fell