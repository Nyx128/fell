#pragma once

#include "cluster_config.hpp"
#include "partition_meta.hpp"
#include "repl_protocol.hpp"
#include "storage/partition_store.hpp"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace fell::repl {

  /**
   * @struct PendingAck
   * @brief Holds a producer ACK that is deferred until quorum replication completes.
   *
   * Created at publish time and stored in `ReplicaManager::pending_`.  Released
   * (i.e. the ACK is sent back to the producer) once the leader has committed
   * the record *and* the required number of ISR followers have acknowledged it.
   */
  struct PendingAck {
    uint64_t offset;                   ///< Partition offset of the record awaiting quorum.
    std::vector<uint8_t> ack_response; ///< Pre-serialised ACK frame to forward to the producer.
    int producer_fd;                   ///< File descriptor of the producer connection.

    bool leader_committed = false; ///< True once the leader's commit thread fires.
    uint32_t isr_acked = 0;        ///< Number of ISR followers that have ACKed this offset.
    uint32_t isr_required = 0; ///< Follower ACKs needed before the record is considered durable.

    std::unordered_set<uint32_t>
        acked_followers; ///< Prevents double-counting from the same follower.
  };

  /**
   * @class ReplicaManager
   * @brief Per-partition component that drives outbound replication and quorum tracking.
   *
   * The leader creates a `ReplicaManager` for each partition it owns.  Its
   * responsibilities are:
   *   - Pushing committed records to follower sockets (worker thread).
   *   - Tracking deferred producer ACKs (`PendingAck`) and releasing them once
   *     the configured durability guarantee (acks=1 or acks=all) is met.
   *   - Updating in-ISR replica state on every `REPLICA_ACK` received.
   */
  class ReplicaManager {
  public:
    ReplicaManager(PartitionMeta &meta, const ClusterConfig &cfg);
    ~ReplicaManager();

    /**
     * @brief Registers a deferred producer ACK for quorum tracking.
     *
     * Must be called from the I/O thread immediately after a record is
     * accepted by the partition store.  For acks=1 the `PendingAck` is
     * released as soon as the leader commits; for acks=all, follower
     * acknowledgements are required as well.
     */
    void register_pending(uint64_t offset, std::vector<uint8_t> ack_response, int producer_fd);

    /**
     * @brief Enqueues a batch of committed records for replication.
     *
     * Non-blocking; hands off to the internal worker thread via a condition
     * variable.  Called from the partition commit callback on the I/O thread.
     */
    void enqueue_committed(uint64_t base_offset,
                           const std::vector<storage::CommittedRecord> &records);

    /**
     * @brief Processes a `REPLICA_ACK` received from a follower.
     *
     * Updates ISR state and releases any `PendingAck` entries whose quorum is
     * now satisfied.
     *
     * @returns List of (producer_fd, serialised_ack) pairs ready to be sent.
     */
    std::vector<std::pair<int, std::vector<uint8_t>>> on_replica_ack(uint32_t follower_id,
                                                                     uint64_t acked_offset);

    using PostResponseCb = std::function<void(int fd, std::vector<uint8_t> resp)>;

    /// Sets the callback used to send ACK responses back to producers.
    void set_post_response_cb(PostResponseCb cb) {
      post_response_cb_ = std::move(cb);
    }

    /// Records the socket fd of a connected follower for log streaming.
    void set_follower_fd(uint32_t broker_id, int fd);

    void start();
    void stop();

  private:
    struct QueuedCommit {
      uint64_t base_offset;
      std::vector<storage::CommittedRecord> records;
    };

    void worker_loop();

    /// Checks whether @p pa has reached quorum and, if so, returns the ACK to send.
    std::vector<std::pair<int, std::vector<uint8_t>>> try_release(PendingAck &pa);

    PartitionMeta &meta_;
    const ClusterConfig &cfg_;
    PostResponseCb post_response_cb_;

    std::vector<PendingAck> pending_;
    std::mutex pending_mu_;

    std::unordered_map<uint32_t, int> follower_fds_;
    std::mutex fds_mu_;

    std::deque<QueuedCommit> commit_queue_;
    std::mutex queue_mu_;
    std::condition_variable queue_cv_;

    std::thread worker_thread_;
    std::atomic<bool> running_{false};
  };

} // namespace fell::repl
