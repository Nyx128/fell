#pragma once

#include "broker/topic_registry.hpp"
#include "cluster_config.hpp"
#include "partition_meta.hpp"
#include "platform/ipoller.hpp"
#include "platform/socket.hpp"
#include "repl_protocol.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <thread>
#include <unordered_map>

namespace fell::repl {

  /**
   * @class ReplicationServer
   * @brief Leader-side replication listener for a broker node.
   *
   * Runs on a dedicated thread and accepts inbound TCP connections on the
   * broker's replication port.  For each connected follower it handles:
   *
   *   - `FETCH_LOG`      — streams committed records from the requested offset.
   *   - `REPLICA_ACK`    — forwards the acknowledgement to the `ReplicaManager`
   *                        via the registered callback, which then checks quorum.
   *   - `HEARTBEAT`      — updates follower liveness state in the ISR.
   *   - `LEADER_ELECTION`— applies incoming leadership claims to `PartitionMeta`.
   */
  class ReplicationServer {
  public:
    ReplicationServer(const ClusterConfig &cfg, PartitionMetaRegistry &meta_reg,
                      fell::TopicRegistry &topic_reg);
    ~ReplicationServer();

    void start();
    void stop();

    /**
     * @brief Callback invoked on the event-loop thread when a `REPLICA_ACK` arrives.
     *
     * The broker wires this to `ReplicaManager::on_replica_ack` so that quorum
     * decisions and deferred producer ACKs are processed on a single path.
     */
    using ReplicaAckCb = std::function<void(uint32_t follower_id, uint64_t acked_offset,
                                            const std::string &topic, uint16_t partition)>;
    void set_replica_ack_cb(ReplicaAckCb cb) {
      replica_ack_cb_ = std::move(cb);
    }

    /**
     * @brief Callback invoked when a follower's `FETCH_LOG` is received.
     *
     * Gives the broker an opportunity to register the follower's socket fd
     * with the appropriate `ReplicaManager` so it can stream new records.
     */
    using FetchLogCb = std::function<void(uint32_t follower_id, const std::string &topic,
                                          uint16_t partition, socket_t fd)>;
    void set_fetch_log_cb(FetchLogCb cb) {
      fetch_log_cb_ = std::move(cb);
    }

  private:
    void event_loop();
    void on_readable(socket_t fd);
    void handle_frame(socket_t fd, uint8_t opcode, const uint8_t *payload, size_t size);

    void handle_fetch_log(socket_t fd, const FetchLogReq &req);
    void handle_replica_ack(const ReplicaAck &req);
    void handle_heartbeat(socket_t fd, const Heartbeat &hb);
    void handle_leader_election(const LeaderElection &le);

    const ClusterConfig &cfg_;
    PartitionMetaRegistry &meta_reg_;
    fell::TopicRegistry &topic_reg_;

    std::unique_ptr<platform::IPoller> poller_;
    socket_t listen_fd_ = INVALID_SOCKET_T;
    std::thread thread_;
    std::atomic<bool> running_{false};

    socket_t notify_read_fd_ = INVALID_SOCKET_T;
    socket_t notify_write_fd_ = INVALID_SOCKET_T;
    int notify_sentinel_ = 0;
    int acceptor_sentinel_ = 0;

    std::unordered_map<socket_t, std::vector<uint8_t>> recv_buffers_;

    ReplicaAckCb replica_ack_cb_;
    FetchLogCb fetch_log_cb_;
  };

} // namespace fell::repl
