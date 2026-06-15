#include "broker/broker.hpp"
#include "platform/socket.hpp"

#include <algorithm>
#include <iostream>

#ifdef FELL_PLATFORM_WINDOWS
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace fell {

  // Shared initialisation performed after cfg_ is set.
  void Broker::init_broker_(size_t /*max_frame_size*/,
                            storage::StorageOptions /*storage_options*/) {
    registry_.recover_all();
    init_notify_pipe();
    repl_server_ = std::make_unique<repl::ReplicationServer>(cfg_, meta_reg_, registry_);
    repl_server_->set_replica_ack_cb([this](uint32_t follower_id, uint64_t acked_offset,
                                            const std::string &topic, uint16_t partition) {
      std::lock_guard<std::mutex> lk(repl_mu_);
      std::string key = replication_key(topic, partition);
      auto it = replica_managers_.find(key);
      if (it != replica_managers_.end()) {
        auto to_send = it->second->on_replica_ack(follower_id, acked_offset);
        for (auto &[fd, resp] : to_send) {
          post_response(fd, std::move(resp));
        }
      }
    });
    repl_server_->set_fetch_log_cb(
        [this](uint32_t follower_id, const std::string &topic, uint16_t partition, socket_t fd) {
          std::lock_guard<std::mutex> lk(repl_mu_);
          auto it = replica_managers_.find(replication_key(topic, partition));
          if (it != replica_managers_.end()) {
            it->second->set_follower_fd(follower_id, static_cast<int>(fd));
          }
        });
  }

  Broker::Broker(const std::filesystem::path &data_dir, size_t max_frame_size,
                 storage::StorageOptions storage_options)
      : poller_(platform::make_poller()), conn_mgr_(max_frame_size),
        registry_(data_dir, storage_options), cfg_(repl::ClusterConfig::load("")),
        handler_(registry_, &cfg_, &meta_reg_,
                 [this](const std::string &topic, uint16_t partition, uint64_t offset,
                        std::vector<uint8_t> ack_resp, int producer_fd) {
                   auto *rm = this->get_or_create_replica_manager(topic, partition);
                   if (rm) {
                     rm->register_pending(offset, std::move(ack_resp), producer_fd);
                   }
                 }) {
    init_broker_(max_frame_size, storage_options);
  }

  Broker::Broker(repl::ClusterConfig cfg, size_t max_frame_size,
                 storage::StorageOptions storage_options)
      : poller_(platform::make_poller()), conn_mgr_(max_frame_size),
        registry_(cfg.data_dir, storage_options), cfg_(std::move(cfg)),
        handler_(registry_, &cfg_, &meta_reg_,
                 [this](const std::string &topic, uint16_t partition, uint64_t offset,
                        std::vector<uint8_t> ack_resp, int producer_fd) {
                   auto *rm = this->get_or_create_replica_manager(topic, partition);
                   if (rm) {
                     rm->register_pending(offset, std::move(ack_resp), producer_fd);
                   }
                 }) {
    init_broker_(max_frame_size, storage_options);
  }

  Broker::~Broker() {
    stop();
    if (repl_server_)
      repl_server_->stop();
    for (auto &[k, rm] : replica_managers_)
      rm->stop();
    for (auto &[k, rc] : replica_clients_)
      rc->stop();

    if (notify_read_fd_ != INVALID_SOCKET_T) {
      platform::close_socket(notify_read_fd_);
    }
    if (notify_write_fd_ != INVALID_SOCKET_T) {
      platform::close_socket(notify_write_fd_);
    }
  }

  void Broker::run(uint16_t port) {
    platform::platform_net_init();

    if (repl_server_)
      repl_server_->start();

    initialize_replication();

    std::cout << "[Broker] Starting broker on port " << port << "..." << std::endl;
    acceptor_.start(port, *poller_, conn_mgr_);

    running_ = true;
    event_loop();

    platform::platform_net_cleanup();
  }

  void Broker::stop() {
    running_ = false;
  }

  void Broker::init_notify_pipe() {
    platform::create_notify_pair(&notify_read_fd_, &notify_write_fd_);
    poller_->add(notify_read_fd_, platform::PF_READ, &notify_sentinel_);
  }

  void Broker::post_response(int fd, std::vector<uint8_t> resp) {
    {
      std::lock_guard<std::mutex> lk(response_mu_);
      response_queue_.push_back({fd, std::move(resp)});
    }
    uint8_t byte = 1;
    platform::send_data(notify_write_fd_, &byte, 1);
  }

  std::string Broker::replication_key(const std::string &topic, uint16_t partition) const {
    return topic + "-" + std::to_string(partition);
  }

  void Broker::initialize_replication() {
    auto topics = registry_.list_topics();
    std::vector<std::string> topic_names;
    std::vector<uint16_t> partition_counts;
    topic_names.reserve(topics.size());
    partition_counts.reserve(topics.size());
    for (const auto &topic : topics) {
      topic_names.push_back(topic.name);
      partition_counts.push_back(topic.partition_count);
    }

    const uint32_t num_brokers = static_cast<uint32_t>(std::max<size_t>(1 + cfg_.peers.size(), 1));
    meta_reg_.assign_roles(cfg_.broker_id, num_brokers, topic_names, partition_counts);

    for (const auto &topic : topics) {
      for (uint16_t partition = 0; partition < topic.partition_count; ++partition) {
        auto &meta = meta_reg_.get(topic.name, partition);
        if (meta.role == repl::PartitionRole::Leader) {
          get_or_create_replica_manager(topic.name, partition);
        } else {
          get_or_create_replication_client(topic.name, partition);
        }
      }
    }
  }

  void Broker::drain_response_queue() {
    std::deque<PendingResponse> local;
    {
      std::lock_guard<std::mutex> lk(response_mu_);
      local.swap(response_queue_);
    }
    for (auto &r : local) {
      ConnectionState *conn = conn_mgr_.get(r.fd);
      if (conn) {
        conn->outbound.data.insert(conn->outbound.data.end(), r.data.begin(), r.data.end());
        int flags = platform::PF_WRITE;
        if (!conn->read_disabled) {
          flags |= platform::PF_READ;
        }
        poller_->modify(conn->fd, flags, conn);
      }
    }
  }

  repl::ReplicaManager *Broker::get_or_create_replica_manager(const std::string &topic,
                                                              uint16_t partition) {
    std::lock_guard<std::mutex> lk(repl_mu_);
    std::string key = replication_key(topic, partition);
    auto it = replica_managers_.find(key);
    if (it != replica_managers_.end()) {
      return it->second.get();
    }

    auto &meta = meta_reg_.get(topic, partition);
    auto rm = std::make_unique<repl::ReplicaManager>(meta, cfg_);

    rm->set_post_response_cb(
        [this](int fd, std::vector<uint8_t> resp) { this->post_response(fd, std::move(resp)); });

    Partition *p = registry_.get_partition(topic, partition);
    if (p) {
      p->set_commit_callback(
          [rm_ptr = rm.get()](uint64_t base_offset,
                              const std::vector<storage::CommittedRecord> &recs) {
            rm_ptr->enqueue_committed(base_offset, recs);
          });
    }

    rm->start();
    repl::ReplicaManager *ptr = rm.get();
    replica_managers_.emplace(key, std::move(rm));
    return ptr;
  }

  repl::ReplicationClient *Broker::get_or_create_replication_client(const std::string &topic,
                                                                    uint16_t partition) {
    std::lock_guard<std::mutex> lk(repl_mu_);
    std::string key = replication_key(topic, partition);
    auto it = replica_clients_.find(key);
    if (it != replica_clients_.end()) {
      return it->second.get();
    }

    auto rc =
        std::make_unique<repl::ReplicationClient>(topic, partition, cfg_, meta_reg_, registry_);
    rc->start();
    repl::ReplicationClient *ptr = rc.get();
    replica_clients_.emplace(key, std::move(rc));
    return ptr;
  }

  void Broker::event_loop() {
    platform::PollEvent events[64];
    while (running_) {
      // 100ms timeout prevents holding up thread on stop() signals
      int n = poller_->wait(events, 64, 100);

      // Drain deferred responses before processing new events
      drain_response_queue();

      for (int i = 0; i < n; ++i) {
        auto &ev = events[i];
        if (ev.ctx == &notify_sentinel_) {
          uint8_t buf[64];
          platform::recv_data(notify_read_fd_, buf, sizeof(buf));
          continue;
        }
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
          if (ev.flags & platform::PF_WRITE) {
            on_writable(*conn);
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

      // Enqueue for non-blocking write
      if (!batch_resp.empty()) {
        conn.outbound.data.insert(conn.outbound.data.end(), batch_resp.begin(), batch_resp.end());
        int flags = platform::PF_WRITE;
        if (!conn.read_disabled) {
          flags |= platform::PF_READ;
        }
        poller_->modify(conn.fd, flags, &conn); // modify: socket already registered
      }

      // Connection-level backpressure (16MB threshold)
      if (conn.outbound.data.size() - conn.outbound.write_offset > 16 * 1024 * 1024) {
        if (!conn.read_disabled) {
          poller_->modify(conn.fd, platform::PF_WRITE, &conn); // disable PF_READ
          conn.read_disabled = true;
        }
      }
    }
  }

  void Broker::on_writable(ConnectionState &conn) {
    auto &out = conn.outbound;
    if (out.write_offset < out.data.size()) {
      int n = ::send(conn.fd, reinterpret_cast<const char *>(out.data.data() + out.write_offset),
                     static_cast<int>(out.data.size() - out.write_offset), 0);
      if (n > 0) {
        out.write_offset += static_cast<size_t>(n);
      } else if (n < 0 && !platform::would_block()) {
        on_hangup(conn);
        return;
      }
    }

    if (out.write_offset == out.data.size()) {
      out.data.clear();
      out.write_offset = 0;

      conn.read_disabled = false; // re-enable read when drained
      // i had this on add and my performance died TwT, dont make mistakes like these, silent
      // mistakes have O(n^2) cost here
      poller_->modify(conn.fd, platform::PF_READ,
                      &conn); // modify, because socket already registered
    }
  }

} // namespace fell
