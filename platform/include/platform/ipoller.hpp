#pragma once
#include <cstdint>
#include <memory>

namespace fell::platform {

  // Own bitmask constants — never expose OS-level EPOLLIN/POLLRDNORM above this
  // header. The broker only knows PF_READ, PF_WRITE, PF_HUP.
  enum PollFlags : uint32_t {
    PF_READ = 1 << 0,
    PF_WRITE = 1 << 1,
    PF_HUP = 1 << 2, // peer closed or socket error
  };

  struct PollEvent {
    void *ctx;      // pointer registered at add() — typically ConnectionState*
    uint32_t flags; // PollFlags bitmask
  };

  class IPoller {
  public:
    virtual ~IPoller() = default;

    virtual void add(int fd, uint32_t flags, void *ctx) = 0;
    virtual void modify(int fd, uint32_t flags, void *ctx) = 0;
    virtual void remove(int fd) = 0;

    // Blocks up to timeout_ms (-1 = indefinite).
    // Writes up to max ready events into out[].
    // Returns the number of events written.
    virtual int wait(PollEvent *out, int max, int timeout_ms) = 0;
  };

  // Returns the correct implementation for the current platform.
  std::unique_ptr<IPoller> make_poller();

} // namespace fell::platform
