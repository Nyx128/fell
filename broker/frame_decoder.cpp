#include "broker/frame_decoder.hpp"
#include <cstddef>

namespace fell {
  int FrameDecoder::push(const uint8_t *data, size_t len, std::vector<Frame> &out) {
    if (buf_.size() - read_idx_ + len > max_frame_size_ + 4096) {
      return -1;
    }

    buf_.insert(buf_.end(), data, data + len);
    int count = 0;
    size_t cursor = read_idx_;

    while (true) {
      if (buf_.size() - cursor < 4) {
        break;
      }

      uint32_t frame_len = (uint32_t(buf_[cursor]) << 24) | (uint32_t(buf_[cursor + 1]) << 16) |
                           (uint32_t(buf_[cursor + 2]) << 8) | uint32_t(buf_[cursor + 3]);

      if (frame_len < 1) {
        buf_.clear();
        read_idx_ = 0;
        break;
      }

      if (frame_len > max_frame_size_) {
        return -1;
      }

      if (buf_.size() - cursor < 4 + static_cast<size_t>(frame_len)) {
        break;
      }

      Frame f;
      f.op = static_cast<Op>(buf_[cursor + 4]);

      if (frame_len > 1) {
        f.payload.assign(buf_.begin() + cursor + 5,
                         buf_.begin() + cursor + 4 + static_cast<ptrdiff_t>(frame_len));
      }

      out.push_back(std::move(f));
      cursor += 4 + static_cast<size_t>(frame_len);
      count++;
    }

    read_idx_ = cursor;

    if (read_idx_ >= 8192) {
      buf_.erase(buf_.begin(), buf_.begin() + static_cast<ptrdiff_t>(read_idx_));
      read_idx_ = 0;
    }

    return count;
  }
} // namespace fell
