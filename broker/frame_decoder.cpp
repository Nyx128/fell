#include "broker/frame_decoder.hpp"
#include <cstddef>

namespace fell {
  int FrameDecoder::push(const uint8_t *data, size_t len, std::vector<Frame> &out) {
    buf_.insert(buf_.end(), data, data + len);
    int count = 0;

    while (true) {
      if (buf_.size() < 4) {
        break;
      }

      uint32_t frame_len = (uint32_t(buf_[0]) << 24) | (uint32_t(buf_[1]) << 16) |
                           (uint32_t(buf_[2]) << 8) | uint32_t(buf_[3]);

      if (frame_len < 1) {
        buf_.clear();
        break;
      }

      if (buf_.size() < 4 + static_cast<size_t>(frame_len)) {
        break;
      }

      Frame f;
      f.op = static_cast<Op>(buf_[4]);

      if (frame_len > 1) {
        f.payload.assign(buf_.begin() + 5, buf_.begin() + 4 + static_cast<ptrdiff_t>(frame_len));
      }

      out.push_back(std::move(f));
      buf_.erase(buf_.begin(), buf_.begin() + 4 + static_cast<ptrdiff_t>(frame_len));
      count++;
    }

    return count;
  }
} // namespace fell
