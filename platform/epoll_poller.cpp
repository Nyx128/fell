#ifdef FELL_PLATFORM_LINUX
#include "platform/ipoller.hpp"
#include <algorithm> // std::min
#include <memory>
#include <stdexcept>
#include <sys/epoll.h>
#include <unistd.h>

namespace fell::platform {

  class EpollPoller final : public IPoller {
    int epfd_;

  public:
    EpollPoller() : epfd_(::epoll_create1(EPOLL_CLOEXEC)) {
      if (epfd_ < 0)
        throw std::runtime_error("epoll_create1 failed");
    }

    ~EpollPoller() override {
      ::close(epfd_);
    }

    void add(int fd, uint32_t flags, void *ctx) override {
      epoll_event ev{};
      ev.events = to_epoll(flags);
      ev.data.ptr = ctx;
      ::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev);
    }

    void modify(int fd, uint32_t flags, void *ctx) override {
      epoll_event ev{};
      ev.events = to_epoll(flags);
      ev.data.ptr = ctx;
      ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev);
    }

    void remove(int fd) override {
      ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);
    }

    int wait(PollEvent *out, int max, int timeout_ms) override {
      // Fixed-size stack buffer — no VLA (non-standard in C++17).
      // 64 is a reasonable batch size; callers should not pass max > 64.
      static constexpr int kMaxBatch = 64;
      epoll_event raw[kMaxBatch];
      int n = ::epoll_wait(epfd_, raw, std::min(max, kMaxBatch), timeout_ms);
      if (n < 0)
        return 0;
      for (int i = 0; i < n; ++i) {
        out[i].ctx = raw[i].data.ptr;
        out[i].flags = from_epoll(raw[i].events);
      }
      return n;
    }

  private:
    static uint32_t to_epoll(uint32_t f) {
      uint32_t e = EPOLLERR | EPOLLHUP; // always watch for errors
      if (f & PF_READ)
        e |= EPOLLIN;
      if (f & PF_WRITE)
        e |= EPOLLOUT;
      return e;
    }

    static uint32_t from_epoll(uint32_t e) {
      uint32_t f = 0;
      if (e & EPOLLIN)
        f |= PF_READ;
      if (e & EPOLLOUT)
        f |= PF_WRITE;
      if (e & (EPOLLERR | EPOLLHUP))
        f |= PF_HUP;
      return f;
    }
  };

  std::unique_ptr<IPoller> make_poller() {
    return std::make_unique<EpollPoller>();
  }

} // namespace fell::platform
#endif
