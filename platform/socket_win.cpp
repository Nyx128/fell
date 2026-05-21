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
  void set_nonblocking(int fd) {
    u_long mode = 1;
    int res = ioctlsocket(static_cast<SOCKET>(fd), FIONBIO, &mode);
    if (res != NO_ERROR) {
      throw std::runtime_error("ioctlsocket failed to set non-blocking mode");
    }
  }

  // Source:
  // https://learn.microsoft.com/en-us/windows/win32/api/winsock/nf-winsock-closesocket
  int close_socket(int fd) {
    return closesocket(static_cast<SOCKET>(fd));
  }

  int create_listen_socket(uint16_t port) {
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
    int fd = static_cast<int>(s);
    set_nonblocking(fd);
    return fd;
  }

  bool would_block() {
    return WSAGetLastError() == WSAEWOULDBLOCK;
  }

  int accept_connection(int listen_fd) {
    SOCKET client = ::accept(static_cast<SOCKET>(listen_fd), nullptr, nullptr);
    if (client == INVALID_SOCKET)
      return -1;
    return static_cast<int>(client);
  }

  int connect_socket(const char *host, uint16_t port) {
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
      return -1;
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
        return -1;
      }
    }

    if (::connect(s, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == SOCKET_ERROR) {
      ::closesocket(s);
      return -1;
    }
    return static_cast<int>(s);
  }

  int send_data(int fd, const void *buf, size_t len) {
    return ::send(static_cast<SOCKET>(fd), reinterpret_cast<const char *>(buf),
                  static_cast<int>(len), 0);
  }

  int recv_data(int fd, void *buf, size_t len) {
    return ::recv(static_cast<SOCKET>(fd), reinterpret_cast<char *>(buf), static_cast<int>(len), 0);
  }

} // namespace fell::platform
#endif