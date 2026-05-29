#include "broker/broker.hpp"
#include "platform/socket.hpp"

#include <iostream>

#ifdef FELL_PLATFORM_WINDOWS
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace fell {

  Broker::Broker(const std::filesystem::path &data_dir, size_t max_frame_size,
                 storage::StorageOptions storage_options)
      : poller_(platform::make_poller()), conn_mgr_(max_frame_size),
        registry_(data_dir, storage_options), handler_(registry_) {
    registry_.recover_all();
  }

  Broker::~Broker() {
    stop();
  }

  void Broker::run(uint16_t port) {
    platform::platform_net_init();

    std::cout << "[Broker] Starting broker on port " << port << "..." << std::endl;
    acceptor_.start(port, *poller_, conn_mgr_);

    running_ = true;
    event_loop();

    platform::platform_net_cleanup();
  }

  void Broker::stop() {
    running_ = false;
  }

  void Broker::event_loop() {
    platform::PollEvent events[64];
    while (running_) {
      // 100ms timeout prevents holding up thread on stop() signals
      int n = poller_->wait(events, 64, 100);
      for (int i = 0; i < n; ++i) {
        auto &ev = events[i];
        if (ev.ctx == acceptor_.sentinel()) {
          acceptor_.accept_all(*poller_, conn_mgr_);
        } else {
          auto *conn = static_cast<ConnectionState *>(ev.ctx);
          if (ev.flags & platform::PF_HUP) {
            on_hangup(*conn);
            continue;
          }
          if (ev.flags & platform::PF_READ) {
            on_readable(*conn);
          }
        }
      }
    }
  }

  void Broker::on_hangup(ConnectionState &conn) {
    std::cout << "[Broker] Client disconnected on fd " << conn.fd << std::endl;
    conn_mgr_.remove(conn.fd, *poller_);
  }

  void Broker::on_readable(ConnectionState &conn) {
    uint8_t buf[4096];
    while (true) {
      int n = ::recv(conn.fd, reinterpret_cast<char *>(buf), sizeof(buf), 0);
      if (n == 0) {
        on_hangup(conn);
        return;
      }
      if (n < 0) {
        if (platform::would_block())
          break;
        on_hangup(conn);
        return;
      }

      std::vector<Frame> frames;
      if (conn.decoder.push(buf, static_cast<size_t>(n), frames) == -1) {
        std::cerr << "[Broker] Client sent frame exceeding max size limit. Disconnecting fd "
                  << conn.fd << std::endl;
        on_hangup(conn);
        return;
      }

      // Accumulate all responses for this recv batch
      std::vector<uint8_t> batch_resp;
      for (auto &f : frames) {
        std::vector<uint8_t> resp = handler_.handle(f, conn);
        batch_resp.insert(batch_resp.end(), resp.begin(), resp.end());
      }

      // Single send for the entire batch
      if (!batch_resp.empty()) {
        if (!send_all(conn.fd, batch_resp.data(), batch_resp.size())) {
          on_hangup(conn);
          return;
        }
      }
    }
  }

  bool Broker::send_all(socket_t fd, const uint8_t *data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
      int n =
          ::send(fd, reinterpret_cast<const char *>(data + sent), static_cast<int>(len - sent), 0);
      if (n < 0) {
        // would_block here means the send buffer is full.
        // A proper fix is a per-connection write queue + PF_WRITE event
        // (Phase 3). For Phase 1, disconnect — a stalled connection must
        // not block the reactor.
        return false;
      }
      sent += static_cast<size_t>(n);
    }
    return true;
  }

} // namespace fell
