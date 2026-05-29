#pragma once
#include "protocol.hpp"
#include <cstdint>
#include <cstring> // std::memcpy
#include <vector>

namespace fell {

  /**
   * @class SmallPayload
   * @brief A small-buffer optimized (SSO) payload container.
   * 
   * Avoids heap allocations for payloads less than or equal to `Capacity`.
   * Transparently falls back to heap allocation when payload exceeds the threshold.
   */
  template <size_t Capacity = 64>
  class SmallPayload {
  public:
    static constexpr size_t kSmallCapacity = Capacity;

    SmallPayload() : size_(0), is_small_(true) {}

    ~SmallPayload() {
      free_heap();
    }

    // Disable copy. Frames should be moved to avoid expensive reallocations.
    SmallPayload(const SmallPayload &) = delete;
    SmallPayload &operator=(const SmallPayload &) = delete;

    SmallPayload(SmallPayload &&other) noexcept : size_(other.size_), is_small_(other.is_small_) {
      if (is_small_) {
        std::memcpy(inline_data_, other.inline_data_, size_);
      } else {
        heap_data_ = other.heap_data_;
        other.heap_data_ = nullptr;
        other.is_small_ = true;
        other.size_ = 0;
      }
    }

    SmallPayload &operator=(SmallPayload &&other) noexcept {
      if (this != &other) {
        free_heap();
        size_ = other.size_;
        is_small_ = other.is_small_;
        if (is_small_) {
          std::memcpy(inline_data_, other.inline_data_, size_);
        } else {
          heap_data_ = other.heap_data_;
          other.heap_data_ = nullptr;
          other.is_small_ = true;
          other.size_ = 0;
        }
      }
      return *this;
    }

    template <typename Iter>
    void assign(Iter begin, Iter end) {
      free_heap();
      size_ = static_cast<size_t>(std::distance(begin, end));
      if (size_ <= kSmallCapacity) {
        is_small_ = true;
        if (size_ > 0) {
          std::copy(begin, end, inline_data_);
        }
      } else {
        is_small_ = false;
        heap_data_ = new uint8_t[size_];
        std::copy(begin, end, heap_data_);
      }
    }

    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }

    const uint8_t *data() const {
      return is_small_ ? inline_data_ : heap_data_;
    }

    uint8_t operator[](size_t idx) const {
      return data()[idx];
    }

  private:
    void free_heap() {
      if (!is_small_ && heap_data_) {
        delete[] heap_data_;
        heap_data_ = nullptr;
      }
    }

    size_t size_;
    bool is_small_;
    union {
      uint8_t inline_data_[kSmallCapacity];
      uint8_t *heap_data_;
    };
  };

  struct Frame {
    Op op;
    // Uses SSO to eliminate heap allocations for small messages.
    // The threshold capacity is configurable here.
    SmallPayload<64> payload;
  };

  class FrameDecoder {
  public:
    explicit FrameDecoder(size_t max_frame_size = 1048576) : max_frame_size_(max_frame_size) {}

    // Append raw bytes received from a single read() call.
    // Emits complete Frame objects into out[]. Returns count emitted.
    // Returns -1 if max_frame_size is exceeded (fatal error).
    int push(const uint8_t *data, size_t len, std::vector<Frame> &out);

    // Reset internal buffer, call on connection reuse or error recovery.
    void reset() {
      buf_.clear();
      read_idx_ = 0;
    }

  private:
    std::vector<uint8_t> buf_;
    size_t max_frame_size_;
    size_t read_idx_ = 0;
  };

} // namespace fell
