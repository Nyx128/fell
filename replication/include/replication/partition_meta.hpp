#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace fell::repl {

  /// Leadership roles a broker can hold for a given partition.
  enum class PartitionRole { Leader, Follower, Candidate };

  /**
   * @struct ReplicaState
   * @brief Per-replica tracking state maintained by the partition leader.
   */
  struct ReplicaState {
    uint32_t broker_id;        ///< Broker id of this replica.
    uint64_t next_offset = 0;  ///< Next offset the leader will push to this replica.
    uint64_t ack_offset = 0;   ///< Last committed offset this replica has confirmed.
    uint64_t last_seen_ms = 0; ///< Wall-clock time of the most recent heartbeat.
    bool in_isr = true;        ///< Whether this replica is currently in the In-Sync Replica set.
  };

  /**
   * @struct PartitionMeta
   * @brief Authoritative runtime metadata for a single topic-partition.
   *
   * Tracks the current leader, epoch, ISR membership, and per-replica progress.
   * All fields are protected by @c mu.
   */
  struct PartitionMeta {
    std::string topic;
    uint16_t partition_idx = 0;
    PartitionRole role = PartitionRole::Follower;
    uint32_t leader_id = 0; ///< Broker id of the current partition leader.
    uint32_t epoch = 0;     ///< Monotonically increasing leadership epoch.
    std::vector<ReplicaState> replicas;
    mutable std::mutex mu;

    /// Returns the broker ids of all replicas currently in the ISR.
    std::vector<uint32_t> isr_ids() const;

    /// Returns true when every in-ISR replica has acknowledged @p offset.
    bool isr_acked(uint64_t offset) const;

    /**
     * @brief Updates replica progress after receiving a REPLICA_ACK.
     *
     * If the replica was previously behind by more than @p max_lag messages
     * it is re-admitted to the ISR once it catches up.
     */
    void on_replica_ack(uint32_t broker_id, uint64_t acked_offset, uint64_t max_lag,
                        uint64_t leader_committed);

    /// Returns the leader's own committed offset (from its ReplicaState entry).
    uint64_t leader_committed_offset() const;

    /**
     * @brief Evicts followers whose last heartbeat is older than @p timeout_ms.
     * @param now_ms  Current wall-clock time in milliseconds.
     */
    void tick_isr(uint64_t now_ms, uint32_t timeout_ms);
  };

  /**
   * @class PartitionMetaRegistry
   * @brief Thread-safe registry of PartitionMeta objects keyed by topic-partition.
   *
   * Entries are created lazily on first access and persist for the lifetime
   * of the broker process.
   */
  class PartitionMetaRegistry {
  public:
    /// Returns (or lazily creates) the meta entry for the given topic-partition.
    PartitionMeta &get(const std::string &topic, uint16_t partition);

    /// Overload accepting a raw topic buffer (used by replication frame handlers).
    PartitionMeta &get(const char *topic, uint8_t topic_len, uint16_t partition);

    /// Returns the current epoch for the given topic-partition.
    uint32_t epoch(const std::string &topic, uint16_t partition);

    /// @overload
    uint32_t epoch(const char *topic, uint8_t topic_len, uint16_t partition);

    /**
     * @brief Seeds leader/follower roles using round-robin partition assignment.
     *
     * For every partition @c p across each topic, `leader = p % num_brokers`.
     * All brokers are added to the ISR.  Roles are recomputed from scratch on
     * each call, so it is idempotent and safe to call after topic creation.
     *
     * @param broker_id        The id of *this* broker.
     * @param num_brokers      Total number of brokers in the cluster.
     * @param topics           Ordered list of topic names.
     * @param partition_counts Partition count for each topic (parallel to @p topics).
     */
    void assign_roles(uint32_t broker_id, uint32_t num_brokers,
                      const std::vector<std::string> &topics,
                      const std::vector<uint16_t> &partition_counts);

  private:
    std::unordered_map<std::string, std::unique_ptr<PartitionMeta>> meta_;
    std::mutex mu_;
  };

} // namespace fell::repl
