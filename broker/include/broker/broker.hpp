#pragma once
#include "broker/acceptor.hpp"
#include "broker/connection_manager.hpp"
#include "broker/request_handler.hpp"
#include "broker/topic_registry.hpp"
#include "platform/ipoller.hpp"
#include "platform/socket.hpp"
#include <atomic>
#include <memory>

namespace fell {

  class Broker {
  public:
    Broker(const std::filesystem::path& data_dir, size_t max_frame_size = 1048576);
    ~Broker();

    void run(uint16_t port);
    void stop();

  private:
    void event_loop();
    void on_readable(ConnectionState &conn);
    void on_hangup(ConnectionState &conn);

    // Sends all response bytes to the client, handling non-blocking short writes
    bool send_all(socket_t fd, const uint8_t *data, size_t len);

    std::unique_ptr<platform::IPoller> poller_;
    Acceptor acceptor_;
    ConnectionManager conn_mgr_;
    TopicRegistry registry_;
    RequestHandler handler_;
    std::atomic<bool> running_{true};
  };

} // namespace fell
