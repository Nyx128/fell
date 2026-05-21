#pragma once
#include <cstdint>

namespace fell {
  // Opcodes use non-overlapping ranges for request (0x0x) and response (0x1x) types
  // to simplify debugging and protocol analysis.

  enum class Op : uint8_t {
    // client to broker
    CREATE_TOPIC = 0x01,
    PUBLISH = 0x02,
    SUBSCRIBE = 0x03,
    FETCH = 0x04,
    COMMIT_OFFSET = 0x05,

    // broker to client
    ACK = 0x10,
    ERROR = 0x11,
    FETCH_RESPONSE = 0x12,
  };

  // err codes returned by the broker
  enum class ErrCode : uint8_t {
    UNKNOWN_TOPIC = 0x01,
    UNKNOWN_PARTITION = 0x02,
    INVALID_OFFSET = 0x03,
    UNKNOWN_OP = 0x04,
    MALFORMED_REQUEST = 0x05,
  };

  namespace proto {
#pragma pack(push, 1)

    // CREATE_TOPIC request
    struct CreateTopicReq {
      uint8_t name_len;
      char name[255]; // only name_len bytes meaningful
      uint16_t num_partitions;
    };

    // PUBLISH request
    struct PublishReq {
      uint8_t topic_len;
      char topic[255];
      uint16_t partition; // 0xFFFF = broker picks (round-robin)
      uint32_t payload_size;
      // payload_size raw bytes follow immediately after this struct
    };

    struct SubscribeReq {
      uint8_t topic_len;
      char topic[255];
      uint16_t partition;
      uint64_t start_offset;
    };

    struct FetchReq {
      uint8_t topic_len;
      char topic[255];
      uint16_t partition;
      uint64_t offset;
      uint16_t max_messages; // upper bound per response
    };

    struct FetchResponseHeader {
      uint64_t offset;
      uint64_t timestamp_ms;
      uint32_t payload_size;
      // payload_size bytes follow
    };

    // ACK response
    struct AckResp {
      uint64_t value; // assigned offset for PUBLISH; 0 for other ops
    };

    // ERROR response
    struct ErrorResp {
      uint8_t code; // ErrCode enum value
      uint8_t msg_len;
      char msg[255];
    };

#pragma pack(pop)
  } // namespace proto
  // compile time checks for tight packing
  static_assert(sizeof(proto::AckResp) == 8);
  static_assert(sizeof(proto::FetchResponseHeader) == 20);
  static_assert(sizeof(proto::ErrorResp) == 257);
} // namespace fell