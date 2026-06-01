#pragma once

#include "broker/acceptor.hpp"
#include "broker/connection_manager.hpp"
#include "broker/request_handler.hpp"
#include "broker/topic_registry.hpp"
#include "platform/ipoller.hpp"
#include "storage/storage_options.hpp"

#include <atomic>
#include <filesystem>
#include <memory>

namespace fell {

  /**
   * @class Broker
   * @brief Core message broker runtime orchestration container.
   * 
   * Design Insight:
   * Employs a highly efficient reactive I/O multiplexing event loop (reactor pattern 
   * leveraging `platform::IPoller`). Drives high-performance socket polling, client 
   * connection state machines, streaming subscription flushes, and background partition 
   * write routing over single-thread network threads.
   */
  class Broker {
  public:
    /**
     * @brief Creates the Broker server instance.
     * @param data_dir Persistent storage subdirectory.
     * @param max_frame_size Maximum allowed packet size.
     * @param storage_options Advanced tuning configurations.
     */
    Broker(const std::filesystem::path &data_dir, size_t max_frame_size = 1048576,
           storage::StorageOptions storage_options = {});
    ~Broker();

    // Disable copy
    Broker(const Broker &) = delete;
    Broker &operator=(const Broker &) = delete;

    /**
     * @brief Launches the reactor event loop on the specified port.
     */
    void run(uint16_t port);

    /**
     * @brief Requests graceful shutdown of the event loop.
     */
    void stop();

  private:
    void event_loop();
    void on_readable(ConnectionState &conn);
    void on_writable(ConnectionState &conn);
    void on_hangup(ConnectionState &conn);
    std::unique_ptr<platform::IPoller> poller_;
    Acceptor acceptor_;
    ConnectionManager conn_mgr_;
    TopicRegistry registry_;
    RequestHandler handler_;
    std::atomic<bool> running_{true};
  };

} // namespace fell
