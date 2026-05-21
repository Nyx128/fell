#pragma once
#include "protocol.hpp"
#include <cstdint>
#include <cstring> // std::memcpy
#include <vector>

namespace fell {

  struct Frame {
    Op op;
    std::vector<uint8_t> payload;
  };

  class FrameDecoder {
  public:
    // Append raw bytes received from a single read() call.
    // Emits complete Frame objects into out[]. Returns count emitted.
    int push(const uint8_t *data, size_t len, std::vector<Frame> &out);

    // Reset internal buffer — call on connection reuse or error recovery.
    void reset() {
      buf_.clear();
    }

  private:
    std::vector<uint8_t> buf_;
  };

} // namespace fell
