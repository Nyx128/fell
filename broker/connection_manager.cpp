#include "broker/connection_manager.hpp"
#include "platform/socket.hpp"
#include <cassert>

namespace fell {
  ConnectionState *ConnectionManager::add(socket_t fd, platform::IPoller &poller) {
    auto it = conns_.find(fd);
    if (it != conns_.end()) {
      poller.remove(fd);
      platform::close_socket(it->second->fd);
      conns_.erase(it);
    }

    auto conn = std::make_unique<ConnectionState>(max_frame_size_);
    conn->fd = fd;
    ConnectionState *ptr = conn.get();
    conns_.emplace(fd, std::move(conn));
    poller.add(fd, platform::PF_READ, ptr);
    return ptr;
  }

  void ConnectionManager::remove(socket_t fd, platform::IPoller &poller) {
    auto it = conns_.find(fd);
    if (it != conns_.end()) {
      poller.remove(fd);
      platform::close_socket(fd);
      conns_.erase(it);
    }
  }

  ConnectionState *ConnectionManager::get(socket_t fd) {
    auto it = conns_.find(fd);
    if (it != conns_.end()) {
      return it->second.get();
    }
    return nullptr;
  }

} // namespace fell