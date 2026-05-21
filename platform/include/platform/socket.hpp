#pragma once
#include <cstdint>

namespace fell::platform {

  // Windows: WSAStartup. Linux: no-op.
  // Must be called before any socket operations.
  void platform_net_init();

  // Windows: WSACleanup. Linux: no-op.
  void platform_net_cleanup();

  // Set fd to non-blocking mode. Fatal on failure — call once per accepted fd.
  void set_nonblocking(int fd);

  // close() on Linux, closesocket() on Windows.
  int close_socket(int fd);

  // Creates a TCP listen socket bound to port on all interfaces (0.0.0.0).
  // Sets SO_REUSEADDR, calls bind() and listen(), sets nonblocking.
  // Returns the fd on success. Throws std::runtime_error on failure.
  int create_listen_socket(uint16_t port);

  // Returns true if the last socket error means "no data yet, try again".
  // Linux: errno == EAGAIN || EWOULDBLOCK. Windows: WSAGetLastError() ==
  // WSAEWOULDBLOCK.
  bool would_block();

  // Accepts one pending connection from listen_fd.
  // Returns the new client fd, or -1 if no connection is ready.
  // Call would_block() after a -1 return to distinguish "try again" from real
  // error.
  int accept_connection(int listen_fd);

  // Connects to a host on a TCP port. Returns the socket fd on success, -1 on
  // failure.
  int connect_socket(const char *host, uint16_t port);

  // Sends data over the socket. Returns bytes sent, or -1 on error.
  int send_data(int fd, const void *buf, size_t len);

  // Receives data from the socket. Returns bytes received, 0 on clean close, -1
  // on error.
  int recv_data(int fd, void *buf, size_t len);

  // Disable Nagle's algorithm on fd. Call once per accepted socket.
  // Linux: setsockopt(TCP_NODELAY). Windows: same via Winsock.
  void set_tcp_nodelay(int fd);

} // namespace fell::platform
