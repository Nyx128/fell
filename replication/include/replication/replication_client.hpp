#pragma once

#include "broker/topic_registry.hpp"
#include "cluster_config.hpp"
#include "partition_meta.hpp"
#include "platform/ipoller.hpp"
#include "platform/socket.hpp"
#include "repl_protocol.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace fell::repl {

  /**
   * @class ReplicationClient
   * @brief Follower-side replication engine for a single topic-partition.
   *
   * Each partition this broker follows gets its own `ReplicationClient`.  It
   * runs on a dedicated thread that:
   *   1. Connects to the current partition leader's replication port.
   *   2. Sends a `FETCH_LOG` request from the follower's committed offset.
   *   3. Processes incoming `REPLICA_SYNC` frames, appending records to the
   *      local partition store.
   *   4. Sends `REPLICA_ACK` frames back to the leader after each commit.
   *   5. Triggers a leader-election broadcast when the leader becomes unreachable.
   */
  class ReplicationClient {
  public:
    ReplicationClient(const std::string &topic, uint16_t partition, const ClusterConfig &cfg,
                      PartitionMetaRegistry &meta_reg, fell::TopicRegistry &topic_reg);
    ~ReplicationClient();

    void start();
    void stop();

    /// Constructs a `FetchLogReq` from the current committed offset of the local partition.
    FetchLogReq build_fetch_log_request() const;

  private:
    void event_loop();
    void connect_to_leader();
    void send_fetch_log();

    /// Attempts to promote this broker to leader when the current leader is unreachable.
    void maybe_elect_self();

    /// Opens a short-lived connection to @p peer and sends a `LEADER_ELECTION` frame.
    void broadcast_election(const BrokerAddr &peer, const LeaderElection &le);

    void on_readable();
    void handle_frame(uint8_t opcode, const uint8_t *payload, size_t size);
    void on_replica_sync(const ReplicaSyncHeader &hdr, const uint8_t *payload);

    std::string topic_;
    uint16_t partition_;
    const ClusterConfig &cfg_;
    PartitionMetaRegistry &meta_reg_;
    fell::TopicRegistry &topic_reg_;

    std::unique_ptr<platform::IPoller> poller_;
    socket_t fd_ = INVALID_SOCKET_T;
    std::thread thread_;
    std::atomic<bool> running_{false};

    socket_t notify_read_fd_ = INVALID_SOCKET_T;
    socket_t notify_write_fd_ = INVALID_SOCKET_T;
    int notify_sentinel_ = 0;

    std::vector<uint8_t> recv_buf_;

    // ACKs queued on the commit callback thread, flushed by the event loop.
    std::vector<ReplicaAck> out_acks_;
    std::mutex out_mu_;
  };

} // namespace fell::repl
