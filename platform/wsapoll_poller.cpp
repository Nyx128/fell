#include "include/platform/socket.hpp"
#ifdef FELL_PLATFORM_WINDOWS
#include "platform/ipoller.hpp"
#include <memory>
#include <vector>
#include <winsock2.h>

namespace fell::platform {

  struct Entry {
    socket_t fd;
    void *ctx;
  };

  // Windows-specific poller implementation using WSAPoll.
  // Source: https://learn.microsoft.com/en-us/windows/win32/api/winsock2/ns-winsock2-wsapollfd
  class WSAPollPoller final : public IPoller {
    std::vector<Entry> entries_;
    std::vector<WSAPOLLFD> pfds_;

  public:
    void add(socket_t fd, uint32_t flags, void *ctx) override {
      SOCKET s = static_cast<SOCKET>(fd);
      entries_.push_back({s, ctx});
      WSAPOLLFD pfd{};
      pfd.fd = s;
      pfd.events = to_wsapoll(flags);
      pfds_.push_back(pfd);
    }

    void modify(socket_t fd, uint32_t flags, void *ctx) override {
      for (size_t i = 0; i < entries_.size(); ++i) {
        if (entries_[i].fd == static_cast<SOCKET>(fd)) {
          entries_[i].ctx = ctx;
          pfds_[i].events = to_wsapoll(flags);
          return;
        }
      }
    }

    void remove(socket_t fd) override {
      for (size_t i = 0; i < entries_.size(); ++i) {
        if (entries_[i].fd == static_cast<SOCKET>(fd)) {
          entries_.erase(entries_.begin() + static_cast<ptrdiff_t>(i));
          pfds_.erase(pfds_.begin() + static_cast<ptrdiff_t>(i));
          return;
        }
      }
    }

    int wait(PollEvent *out, int max, int timeout_ms) override {
      if (pfds_.empty())
        return 0;
      int n = WSAPoll(pfds_.data(), static_cast<ULONG>(pfds_.size()), timeout_ms);
      if (n <= 0)
        return 0;
      int count = 0;
      for (size_t i = 0; i < pfds_.size() && count < max; ++i) {
        if (pfds_[i].revents) {
          out[count].ctx = entries_[i].ctx;
          out[count].flags = from_wsapoll(pfds_[i].revents);
          ++count;
        }
      }
      return count;
    }

  private:
    static SHORT to_wsapoll(uint32_t f) {
      SHORT e = 0;
      if (f & PF_READ)
        e |= POLLRDNORM;
      if (f & PF_WRITE)
        e |= POLLWRNORM;
      return e;
    }

    static uint32_t from_wsapoll(SHORT e) {
      uint32_t f = 0;
      if (e & POLLRDNORM)
        f |= PF_READ;
      if (e & POLLWRNORM)
        f |= PF_WRITE;
      if (e & (POLLERR | POLLHUP))
        f |= PF_HUP;
      return f;
    }
  };

  std::unique_ptr<IPoller> make_poller() {
    return std::make_unique<WSAPollPoller>();
  }

} // namespace fell::platform
#endif
