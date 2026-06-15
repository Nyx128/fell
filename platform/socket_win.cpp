#include "include/platform/socket.hpp"
#ifdef FELL_PLATFORM_WINDOWS
#include "platform/socket.hpp"
#include <WinSock2.h>
#include <stdexcept>
#include <ws2tcpip.h>

namespace fell::platform {
  // Source:
  // https://learn.microsoft.com/en-us/windows/win32/api/winsock/nf-winsock-wsastartup
  void platform_net_init() {
    WSADATA wsa_data;

    int res = WSAStartup(MAKEWORD(2, 2), &wsa_data);

    if (res != 0) {
      throw std::runtime_error("WSAStartup failed");
    }
  }

  void platform_net_cleanup() {
    WSACleanup();
  }

  // Source:
  // https://learn.microsoft.com/en-us/windows/win32/api/winsock/nf-winsock-ioctlsocket
  void set_nonblocking(socket_t fd) {
    u_long mode = 1;
    int res = ioctlsocket(static_cast<SOCKET>(fd), FIONBIO, &mode);
    if (res != NO_ERROR) {
      throw std::runtime_error("ioctlsocket failed to set non-blocking mode");
    }
  }

  // Source:
  // https://learn.microsoft.com/en-us/windows/win32/api/winsock/nf-winsock-closesocket
  int close_socket(socket_t fd) {
    return closesocket(fd);
  }

  socket_t create_listen_socket(uint16_t port) {
    // Create TCP socket
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET)
      throw std::runtime_error("socket() failed");

    // SO_REUSEADDR — must be set before bind() so a restarted broker can
    // rebind the port immediately while the old socket is in TIME_WAIT.
    int opt = 1;
    ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char *>(&opt), sizeof(opt));

    // Bind to 0.0.0.0:port
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (::bind(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == SOCKET_ERROR) {
      ::closesocket(s);
      throw std::runtime_error("bind() failed");
    }

    // Start listening — backlog of 128 is the conventional default
    if (::listen(s, 128) == SOCKET_ERROR) {
      ::closesocket(s);
      throw std::runtime_error("listen() failed");
    }

    // Put the listen socket itself into nonblocking mode so accept_all()
    // can loop until WSAEWOULDBLOCK without stalling the event loop thread.
    set_nonblocking(s);
    return s;
  }

  bool would_block() {
    return WSAGetLastError() == WSAEWOULDBLOCK;
  }

  socket_t accept_connection(socket_t listen_fd) {
    SOCKET client = ::accept(listen_fd, nullptr, nullptr);
    if (client == INVALID_SOCKET)
      return INVALID_SOCKET_T;
    return static_cast<socket_t>(client);
  }

  socket_t connect_socket(const char *host, uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
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
        ::closesocket(s);
        return INVALID_SOCKET_T;
      }
    }

    if (::connect(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == SOCKET_ERROR) {
      ::closesocket(s);
      return INVALID_SOCKET_T;
    }
    set_tcp_nodelay(static_cast<int>(s));
    return static_cast<int>(s);
  }

  int send_data(socket_t fd, const void *buf, size_t len) {
    return ::send(fd, reinterpret_cast<const char *>(buf), static_cast<int>(len), 0);
  }

  int recv_data(socket_t fd, void *buf, size_t len) {
    return ::recv(fd, reinterpret_cast<char *>(buf), static_cast<int>(len), 0);
  }

  void set_tcp_nodelay(socket_t fd) {
    BOOL flag = TRUE;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char *>(&flag), sizeof(flag));
  }

  void create_notify_pair(socket_t *read_fd, socket_t *write_fd) {
    SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET)
      throw std::runtime_error("socket failed");

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (::bind(listener, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == SOCKET_ERROR) {
      ::closesocket(listener);
      throw std::runtime_error("bind failed for notify pair");
    }

    if (::listen(listener, 1) == SOCKET_ERROR) {
      ::closesocket(listener);
      throw std::runtime_error("listen failed for notify pair");
    }

    int addr_len = sizeof(addr);
    if (::getsockname(listener, reinterpret_cast<sockaddr *>(&addr), &addr_len) == SOCKET_ERROR) {
      ::closesocket(listener);
      throw std::runtime_error("getsockname failed");
    }

    SOCKET client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (client == INVALID_SOCKET) {
      ::closesocket(listener);
      throw std::runtime_error("socket failed");
    }

    if (::connect(client, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == SOCKET_ERROR) {
      ::closesocket(listener);
      ::closesocket(client);
      throw std::runtime_error("connect failed for notify pair");
    }

    SOCKET server = ::accept(listener, nullptr, nullptr);
    if (server == INVALID_SOCKET) {
      ::closesocket(listener);
      ::closesocket(client);
      throw std::runtime_error("accept failed for notify pair");
    }

    ::closesocket(listener);

    set_nonblocking(client);
    set_nonblocking(server);
    set_tcp_nodelay(client);
    set_tcp_nodelay(server);

    *read_fd = server;
    *write_fd = client;
  }

} // namespace fell::platform
#endif