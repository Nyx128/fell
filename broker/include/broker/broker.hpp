#pragma once

#include "broker/acceptor.hpp"
#include "broker/connection_manager.hpp"
#include "broker/request_handler.hpp"
#include "broker/topic_registry.hpp"
#include "platform/ipoller.hpp"
#include "platform/socket.hpp"
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include "replication/replica_manager.hpp"
#include "replication/replication_client.hpp"
#include "replication/replication_server.hpp"
#include "storage/storage_options.hpp"

#include <atomic>
#include <deque>
#include <filesystem>
#include <memory>
#include <mutex>
#include <vector>

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
     * @brief Creates the Broker server instance (single-node defaults).
     * @param data_dir Persistent storage subdirectory.
     * @param max_frame_size Maximum allowed packet size.
     * @param storage_options Advanced tuning configurations.
     */
    Broker(const std::filesystem::path &data_dir, size_t max_frame_size = 1048576,
           storage::StorageOptions storage_options = {});

    /**
     * @brief Creates the Broker server instance with a full cluster configuration.
     * @param cfg Fully parsed ClusterConfig (e.g. from an INI file).
     * @param max_frame_size Maximum allowed packet size.
     * @param storage_options Advanced tuning configurations.
     */
    Broker(repl::ClusterConfig cfg, size_t max_frame_size = 1048576,
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

    void post_response(int fd, std::vector<uint8_t> resp);

  private:
    void init_broker_(size_t max_frame_size, storage::StorageOptions storage_options);
    void initialize_replication();
    std::string replication_key(const std::string &topic, uint16_t partition) const;
    void init_notify_pipe();
    void drain_response_queue();
    void event_loop();
    void on_readable(ConnectionState &conn);
    void on_writable(ConnectionState &conn);
    void on_hangup(ConnectionState &conn);
    repl::ReplicationClient *get_or_create_replication_client(const std::string &topic,
                                                              uint16_t partition);
    std::unique_ptr<platform::IPoller> poller_;
    Acceptor acceptor_;
    ConnectionManager conn_mgr_;
    TopicRegistry registry_;

    repl::ClusterConfig cfg_;
    repl::PartitionMetaRegistry meta_reg_;
    std::unique_ptr<repl::ReplicationServer> repl_server_;

    std::mutex repl_mu_;
    std::unordered_map<std::string, std::unique_ptr<repl::ReplicaManager>> replica_managers_;
    std::unordered_map<std::string, std::unique_ptr<repl::ReplicationClient>> replica_clients_;

    repl::ReplicaManager *get_or_create_replica_manager(const std::string &topic,
                                                        uint16_t partition);

    RequestHandler handler_;
    std::atomic<bool> running_{true};

    struct PendingResponse {
      int fd;
      std::vector<uint8_t> data;
    };

    std::deque<PendingResponse> response_queue_;
    std::mutex response_mu_;
    socket_t notify_write_fd_ = INVALID_SOCKET_T;
    socket_t notify_read_fd_ = INVALID_SOCKET_T;
    int notify_sentinel_ = 0;
  };

} // namespace fell
