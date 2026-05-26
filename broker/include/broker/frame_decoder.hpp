#pragma once
#include "protocol.hpp"
#include <cstdint>
#include <cstring> // std::memcpy
#include <vector>

namespace fell {

  struct Frame {
    Op op;
    // TODO(phase3): payload causes one heap allocation per decoded frame (~80 ns on Zen 4 for
    // small payloads). Replace with a small-buffer optimisation (inline storage for payloads
    // <= 64 bytes, heap fallback beyond that) to bring SingleFrame decode into the 5–15 ns range.
    // Only relevant once the reactor is zero-copy or profiling shows allocator as a top hotspot.
    std::vector<uint8_t> payload;
  };

  class FrameDecoder {
  public:
    // Append raw bytes received from a single read() call.
    // Emits complete Frame objects into out[]. Returns count emitted.
    int push(const uint8_t *data, size_t len, std::vector<Frame> &out);

    // Reset internal buffer, call on connection reuse or error recovery.
    void reset() {
      buf_.clear();
    }

  private:
    std::vector<uint8_t> buf_;
  };

} // namespace fell
