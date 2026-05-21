#include "broker/acceptor.hpp"
#include "platform/socket.hpp"

#include <stdexcept>

namespace fell {
  void Acceptor::start(uint16_t port, platform::IPoller &poller, ConnectionManager & /*conn_mgr*/) {
    listen_fd_ = platform::create_listen_socket(port);
    if (listen_fd_ == -1) {
      throw std::runtime_error("Failed to create listen socket");
    }
    poller.add(listen_fd_, platform::PF_READ, this);
  }

  void Acceptor::accept_all(platform::IPoller &poller, ConnectionManager &conn_mgr) {
    while (true) {
      int fd = platform::accept_connection(listen_fd_);
      if (fd < 0) {
        if (platform::would_block())
          break;
        break;
      }
      platform::set_nonblocking(fd);
      conn_mgr.add(fd, poller);
    }
  }

} // namespace fell