#pragma once
#include "broker/frame_decoder.hpp"
#include "platform/socket.hpp"
#include <string>

#include <vector>

namespace fell {

  struct OutboundState {
    std::vector<uint8_t> data;
    size_t write_offset = 0;
  };

  struct ConnectionState {
    explicit ConnectionState(size_t max_frame_size = 1048576) : decoder(max_frame_size) {}

    socket_t fd;
    FrameDecoder decoder;

    // Consumer subscription set by SUBSCRIBE, used by FETCH
    std::string sub_topic;
    uint16_t sub_partition = 0;
    uint64_t fetch_offset = 0;
    bool subscribed = false;

    // Round-robin counter for keyless PUBLISH (partition = 0xFFFF)
    // Stored per-connection
    uint16_t rr_counter = 0;

    // Phase 3: Outbound queue for non-blocking writes
    OutboundState outbound;
    bool read_disabled = false;
  };

} // namespace fell
