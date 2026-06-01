#pragma once

#include <cstdint>

namespace fell {

#ifdef ERROR
#undef ERROR
#endif

  /**
   * @enum Op
   * @brief Protocol Opcode identifier for network frame payloads.
   * 
   * Design Insight:
   * Uses non-overlapping ranges for request (0x0x) and response (0x1x) types 
   * to simplify packet inspection, Wireshark parsing, and frame decoding logic.
   */
  enum class Op : uint8_t {
    // client to broker
    CREATE_TOPIC = 0x01,  ///< Declare a new topic with N partitions
    PUBLISH = 0x02,       ///< Produce a message to a topic partition
    SUBSCRIBE = 0x03,     ///< Consume continuous streaming updates
    FETCH = 0x04,         ///< Single-shot query for committed records
    COMMIT_OFFSET = 0x05, ///< Commit client consumer progress (Ack offsets)
    PUBLISH_V2 = 0x06,    ///< Produce a message to a topic partition with routing key

    // broker to client
    ACK = 0x10,            ///< Operation succeeded
    ERR = 0x11,            ///< Operation failed
    FETCH_RESPONSE = 0x12, ///< Bulk batch fetch return payload
  };

  /**
   * @enum ErrCode
   * @brief Standardized error codes sent inside ErrorResp frames.
   */
  enum class ErrCode : uint8_t {
    UNKNOWN_TOPIC = 0x01,     ///< Topic does not exist
    UNKNOWN_PARTITION = 0x02, ///< Partition is out-of-bounds for the topic
    INVALID_OFFSET = 0x03,    ///< Requested offset exceeds next log offset
    UNKNOWN_OP = 0x04,        ///< Unrecognized frame opcode
    MALFORMED_REQUEST = 0x05, ///< Frame structure or payload checksum failed
    BUSY = 0x06,              ///< Bounded partition queue full (backpressure)
    INTERNAL_ERROR = 0x07     ///< Internal failure (e.g. partition closed)
  };

  namespace proto {
#pragma pack(push, 1)

    /**
     * @struct CreateTopicReq
     * @brief Raw frame for topic creation.
     */
    struct CreateTopicReq {
      uint8_t name_len;        ///< Real length of the name string
      char name[255];          ///< Padded name string buffer
      uint16_t num_partitions; ///< Partitions to initialize
    };

    /**
     * @struct PublishReq
     * @brief Raw frame for producing a message.
     */
    struct PublishReq {
      uint8_t topic_len;      ///< Topic name length
      char topic[255];        ///< Topic name string
      uint16_t partition;     ///< Target partition index, or 0xFFFF for round-robin
      uint32_t payload_size;  ///< Size of the trailing payload binary
      // payload_size raw bytes follow immediately after this struct
    };

    /**
     * @struct PublishV2Req
     * @brief Raw frame for producing a message with a routing key.
     */
    struct PublishV2Req {
      uint8_t topic_len;      ///< Topic name length
      char topic[255];        ///< Topic name string
      uint16_t partition;     ///< Target partition index, or 0xFFFF for round-robin
      uint8_t key_len;        ///< Routing key length
      char key[255];          ///< Routing key string
      uint32_t payload_size;  ///< Size of the trailing payload binary
      // payload_size raw bytes follow immediately after this struct
    };

    /**
     * @struct SubscribeReq
     * @brief Raw frame for subscribe stream query.
     */
    struct SubscribeReq {
      uint8_t topic_len;
      char topic[255];
      uint16_t partition;
      uint64_t start_offset;
    };

    /**
     * @struct FetchReq
     * @brief Raw frame for bulk log query.
     */
    struct FetchReq {
      uint8_t topic_len;
      char topic[255];
      uint16_t partition;
      uint64_t offset;
      uint16_t max_messages; ///< Upper limit threshold for this round-trip
    };

    /**
     * @struct FetchResponseHeader
     * @brief Bulk fetch segment descriptor.
     */
    struct FetchResponseHeader {
      uint64_t offset;
      uint64_t timestamp_ms;
      uint32_t payload_size;
      // payload_size bytes follow
    };

    /**
     * @struct AckResp
     * @brief Success acknowledgment.
     */
    struct AckResp {
      uint64_t value; ///< Assigned offset for PUBLISH; 0 for other operations
    };

    /**
     * @struct ErrorResp
     * @brief Standard failure payload.
     */
    struct ErrorResp {
      uint8_t code; ///< ErrCode enum value
      uint8_t msg_len;
      char msg[255];
    };

#pragma pack(pop)
  } // namespace proto

  // Static packing asserts to prevent compilers from breaking network alignment
  static_assert(sizeof(proto::AckResp) == 8);
  static_assert(sizeof(proto::FetchResponseHeader) == 20);
  static_assert(sizeof(proto::ErrorResp) == 257);
} // namespace fell