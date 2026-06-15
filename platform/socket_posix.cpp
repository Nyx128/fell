#ifdef FELL_PLATFORM_LINUX
#include "platform/socket.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace fell::platform {

  void platform_net_init() {
    // No-op on Linux — no WSAStartup equivalent needed.
  }

  void platform_net_cleanup() {
    // No-op on Linux.
  }

  void set_nonblocking(socket_t fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags == -1)
      throw std::runtime_error("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
      throw std::runtime_error("fcntl F_SETFL O_NONBLOCK failed");
  }

  int close_socket(socket_t fd) {
    return ::close(fd);
  }

  socket_t create_listen_socket(uint16_t port) {
    // Create TCP socket
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0)
      throw std::runtime_error("socket() failed");

    // SO_REUSEADDR — must be set before bind() so a restarted broker can
    // rebind the port immediately while the old socket is in TIME_WAIT.
    int opt = 1;
    ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to 0.0.0.0:port
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (::bind(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      ::close(s);
      throw std::runtime_error("bind() failed");
    }

    // Start listening — backlog of 128 is the conventional default
    if (::listen(s, 128) < 0) {
      ::close(s);
      throw std::runtime_error("listen() failed");
    }

    set_nonblocking(s);
    return s;
  }

  bool would_block() {
    return errno == EAGAIN || errno == EWOULDBLOCK;
  }

  socket_t accept_connection(socket_t listen_fd) {
    return ::accept(listen_fd, nullptr, nullptr);
  }

  socket_t connect_socket(const char *host, uint16_t port) {
    int s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
      return INVALID_SOCKET_T;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (::inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
      addrinfo hints{}, *res = nullptr;
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      if (::getaddrinfo(host, nullptr, &hints, &res) == 0) {
        addr.sin_addr = reinterpret_cast<sockaddr_in *>(res->ai_addr)->sin_addr;
        ::freeaddrinfo(res);
      } else {
        ::close(s);
        return INVALID_SOCKET_T;
      }
    }
    if (::connect(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      ::close(s);
      return INVALID_SOCKET_T;
    }
    set_tcp_nodelay(s);
    return s;
  }
  int send_data(socket_t fd, const void *buf, size_t len) {
    return ::send(fd, buf, len, 0);
  }
  int recv_data(socket_t fd, void *buf, size_t len) {
    return ::recv(fd, buf, len, 0);
  }

  void set_tcp_nodelay(socket_t fd) {
    int flag = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
  }

  void create_notify_pair(socket_t *read_fd, socket_t *write_fd) {
    int fds[2];
    if (::pipe(fds) == -1) {
      throw std::runtime_error("pipe() failed for notify pair");
    }
    set_nonblocking(fds[0]);
    set_nonblocking(fds[1]);
    *read_fd = fds[0];
    *write_fd = fds[1];
  }

} // namespace fell::platform
#endif
