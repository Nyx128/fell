#pragma once
#include "broker/connection_state.hpp"
#include "platform/ipoller.hpp"
#include "platform/socket.hpp"

#include <memory>
#include <unordered_map>

namespace fell {
  class ConnectionManager {
  public:
    explicit ConnectionManager(size_t max_frame_size = 1048576) : max_frame_size_(max_frame_size) {}

    // Creates a ConnectionState for fd, registers with poller on PF_READ.
    // Returns the raw pointer (poller holds it as ctx).
    ConnectionState *add(socket_t fd, platform::IPoller &poller);

    // Removes from poller, closes fd, erases from map.
    void remove(socket_t fd, platform::IPoller &poller);

  private:
    std::unordered_map<socket_t, std::unique_ptr<ConnectionState>> conns_;
    size_t max_frame_size_;
  };

} // namespace fell