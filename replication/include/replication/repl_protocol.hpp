#pragma once

#include <cstdint>

namespace fell::repl {

  /**
   * @enum ReplOp
   * @brief Opcodes for the internal broker-to-broker replication protocol.
   *
   * Every frame on the replication channel begins with a 4-byte big-endian
   * length followed by one of these opcode bytes.
   */
  enum class ReplOp : uint8_t {
    FETCH_LOG = 0x01,        ///< Follower requests the leader's committed log from a given offset.
    REPLICA_SYNC = 0x02,     ///< Leader pushes a single committed record to a follower.
    REPLICA_SYNC_END = 0x03, ///< Leader signals end of the catch-up batch.
    REPLICA_ACK = 0x04,      ///< Follower acknowledges a successfully applied record.
    HEARTBEAT = 0x05,        ///< Follower proves liveness to the leader.
    HEARTBEAT_ACK = 0x06,    ///< Leader acknowledges a heartbeat (reserved for future use).
    LEADER_ELECTION = 0x07,  ///< Candidate broadcasts a leadership claim to peers.
  };

#pragma pack(push, 1)

  /// Sent by a follower to initiate log replication from `start_offset` onward.
  struct FetchLogReq {
    uint8_t topic_len;     ///< Length of the topic name in bytes.
    char topic[255];       ///< Topic name (not NUL-terminated).
    uint16_t partition;    ///< Partition index (big-endian).
    uint64_t start_offset; ///< Follower's current committed offset (big-endian).
    uint32_t follower_id;  ///< Broker id of the requesting follower (big-endian).
  };

  /// Header prepended to each record the leader streams to a follower.
  struct ReplicaSyncHeader {
    uint8_t topic_len;     ///< Length of the topic name in bytes.
    char topic[255];       ///< Topic name (not NUL-terminated).
    uint16_t partition;    ///< Partition index (big-endian).
    uint32_t epoch;        ///< Leader epoch at time of replication (big-endian).
    uint64_t offset;       ///< Committed offset of this record on the leader (big-endian).
    uint64_t timestamp_ms; ///< Append timestamp in milliseconds (big-endian).
    uint32_t payload_size; ///< Length of the message payload that follows (big-endian).
  };

  /// Sent by a follower after successfully committing a replicated record.
  struct ReplicaAck {
    uint8_t topic_len;     ///< Length of the topic name in bytes.
    char topic[255];       ///< Topic name (not NUL-terminated).
    uint16_t partition;    ///< Partition index (big-endian).
    uint32_t epoch;        ///< Leader epoch this ACK refers to (big-endian).
    uint64_t acked_offset; ///< Follower's committed offset after this batch (big-endian).
    uint32_t follower_id;  ///< Broker id of the acknowledging follower (big-endian).
  };

  /// Periodic liveness probe sent from follower to leader.
  struct Heartbeat {
    uint32_t sender_id;        ///< Broker id of the sender (big-endian).
    uint64_t timestamp_ms;     ///< Wall-clock time of the heartbeat (big-endian).
    uint64_t committed_offset; ///< Sender's committed offset, used for election tie-breaking
                               ///< (big-endian).
  };

  /// Broadcast by a candidate that wishes to become the new partition leader.
  struct LeaderElection {
    uint8_t topic_len;      ///< Length of the topic name in bytes.
    char topic[255];        ///< Topic name (not NUL-terminated).
    uint16_t partition;     ///< Partition index (big-endian).
    uint32_t new_leader_id; ///< Broker id of the proposing candidate (big-endian).
    uint32_t epoch; ///< Proposed new epoch (must be greater than any known epoch) (big-endian).
    uint64_t committed_offset; ///< Candidate's committed offset at proposal time (big-endian).
  };

#pragma pack(pop)

} // namespace fell::repl
