#pragma once
#include "broker/connection_manager.hpp"

namespace fell {
  class Acceptor {
  public:
    // Creates listen socket, binds, listens, registers with poller.
    void start(uint16_t port, platform::IPoller &poller, ConnectionManager &conn_mgr);

    // Called by the event loop when the listen fd is readable.
    // Loops accept() until EAGAIN, hands each fd to conn_mgr.
    void accept_all(platform::IPoller &poller, ConnectionManager &conn_mgr);

    // Returns 'this' — used by the event loop to identify listen fd events.
    void *sentinel() {
      return this;
    }

  private:
    int listen_fd_ = -1;
  };

} // namespace fell
