#include "replication/replication_server.hpp"
#include "platform/endian.hpp"
#include <cstring>
#include <iostream>

namespace fell::repl {

  ReplicationServer::ReplicationServer(const ClusterConfig &cfg, PartitionMetaRegistry &meta_reg,
                                       fell::TopicRegistry &topic_reg)
      : cfg_(cfg), meta_reg_(meta_reg), topic_reg_(topic_reg), poller_(platform::make_poller()) {
  }

  ReplicationServer::~ReplicationServer() {
    stop();
    if (notify_read_fd_ != INVALID_SOCKET_T)
      platform::close_socket(notify_read_fd_);
    if (notify_write_fd_ != INVALID_SOCKET_T)
      platform::close_socket(notify_write_fd_);
    if (listen_fd_ != INVALID_SOCKET_T)
      platform::close_socket(listen_fd_);
  }

  void ReplicationServer::start() {
    if (running_)
      return;

    listen_fd_ = platform::create_listen_socket(cfg_.repl_port);
    poller_->add(listen_fd_, platform::PF_READ, &acceptor_sentinel_);

    platform::create_notify_pair(&notify_read_fd_, &notify_write_fd_);
    poller_->add(notify_read_fd_, platform::PF_READ, &notify_sentinel_);

    running_ = true;
    thread_ = std::thread(&ReplicationServer::event_loop, this);
  }

  void ReplicationServer::stop() {
    if (!running_)
      return;
    running_ = false;
    uint8_t byte = 1;
    platform::send_data(notify_write_fd_, &byte, 1);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void ReplicationServer::event_loop() {
    platform::PollEvent events[64];
    while (running_) {
      int n = poller_->wait(events, 64, 100);
      for (int i = 0; i < n; ++i) {
        auto &ev = events[i];
        if (ev.ctx == &notify_sentinel_) {
          uint8_t buf[64];
          platform::recv_data(notify_read_fd_, buf, sizeof(buf));
          continue;
        }
        if (ev.ctx == &acceptor_sentinel_) {
          while (true) {
            auto fd = platform::accept_connection(listen_fd_);
            if (fd == INVALID_SOCKET_T)
              break;
            platform::set_nonblocking(fd);
            platform::set_tcp_nodelay(fd);
            poller_->add(fd, platform::PF_READ,
                         reinterpret_cast<void *>(static_cast<uintptr_t>(fd)));
          }
        } else {
          auto fd = static_cast<socket_t>(reinterpret_cast<uintptr_t>(ev.ctx));
          if (ev.flags & platform::PF_HUP) {
            poller_->remove(fd);
            platform::close_socket(fd);
            recv_buffers_.erase(fd);
            continue;
          }
          if (ev.flags & platform::PF_READ) {
            on_readable(fd);
          }
        }
      }
    }
  }

  void ReplicationServer::on_readable(socket_t fd) {
    auto &buf = recv_buffers_[fd];
    uint8_t temp[4096];
    while (true) {
      int n = platform::recv_data(fd, temp, sizeof(temp));
      if (n == 0) {
        poller_->remove(fd);
        platform::close_socket(fd);
        recv_buffers_.erase(fd);
        return;
      }
      if (n < 0) {
        if (platform::would_block())
          break;
        poller_->remove(fd);
        platform::close_socket(fd);
        recv_buffers_.erase(fd);
        return;
      }
      buf.insert(buf.end(), temp, temp + n);

      while (buf.size() >= 5) {
        uint32_t net_len;
        std::memcpy(&net_len, buf.data(), 4);
        uint32_t len = platform::be32_to_host(net_len);
        if (buf.size() >= 4 + len) {
          uint8_t opcode = buf[4];
          handle_frame(fd, opcode, buf.data() + 5, len - 1);
          buf.erase(buf.begin(), buf.begin() + 4 + len);
        } else {
          break;
        }
      }
    }
  }

  void ReplicationServer::handle_frame(socket_t fd, uint8_t opcode, const uint8_t *payload,
                                       size_t size) {
    auto op = static_cast<ReplOp>(opcode);
    switch (op) {
    case ReplOp::FETCH_LOG: {
      if (size >= sizeof(FetchLogReq)) {
        FetchLogReq req;
        std::memcpy(&req, payload, sizeof(FetchLogReq));
        handle_fetch_log(fd, req);
      }
      break;
    }
    case ReplOp::REPLICA_ACK: {
      if (size >= sizeof(ReplicaAck)) {
        ReplicaAck req;
        std::memcpy(&req, payload, sizeof(ReplicaAck));
        handle_replica_ack(req);
      }
      break;
    }
    case ReplOp::HEARTBEAT: {
      if (size >= sizeof(Heartbeat)) {
        Heartbeat hb;
        std::memcpy(&hb, payload, sizeof(Heartbeat));
        handle_heartbeat(fd, hb);
      }
      break;
    }
    case ReplOp::LEADER_ELECTION: {
      if (size >= sizeof(LeaderElection)) {
        LeaderElection le;
        std::memcpy(&le, payload, sizeof(LeaderElection));
        handle_leader_election(le);
      }
      break;
    }
    default:
      break;
    }
  }

  void ReplicationServer::handle_fetch_log(socket_t fd, const FetchLogReq &req) {
    const std::string topic(req.topic, req.topic_len);
    const uint16_t partition = platform::be16_to_host(req.partition);
    fell::Partition *p = topic_reg_.get_partition(topic, partition);
    if (!p) {
      poller_->remove(fd);
      platform::close_socket(fd);
      recv_buffers_.erase(fd);
      return;
    }

    if (fetch_log_cb_) {
      fetch_log_cb_(platform::be32_to_host(req.follower_id), topic, partition, fd);
    }

    uint64_t offset = platform::be64_to_host(req.start_offset);

    while (offset < p->committed_offset()) {
      auto msgs = p->fetch(offset, 128);
      for (auto &msg : msgs) {
        ReplicaSyncHeader hdr;
        std::memset(&hdr, 0, sizeof(hdr));
        hdr.topic_len = req.topic_len;
        std::memcpy(hdr.topic, req.topic, req.topic_len);
        hdr.partition = req.partition;
        hdr.epoch = platform::host_to_be32(meta_reg_.epoch(topic, partition));
        hdr.offset = platform::host_to_be64(msg.offset);
        hdr.timestamp_ms = platform::host_to_be64(msg.timestamp_ms);
        hdr.payload_size = platform::host_to_be32(static_cast<uint32_t>(msg.payload.size()));

        uint32_t frame_size =
            sizeof(ReplicaSyncHeader) + static_cast<uint32_t>(msg.payload.size()) + 1;
        uint32_t net_frame_size = platform::host_to_be32(frame_size);
        uint8_t opcode = static_cast<uint8_t>(ReplOp::REPLICA_SYNC);

        platform::send_data(fd, &net_frame_size, 4);
        platform::send_data(fd, &opcode, 1);
        platform::send_data(fd, &hdr, sizeof(hdr));
        if (!msg.payload.empty()) {
          platform::send_data(fd, msg.payload.data(), msg.payload.size());
        }
        ++offset;
      }
      if (msgs.empty())
        break;
    }

    uint32_t frame_size = sizeof(ReplicaSyncHeader) + 1;
    uint32_t net_frame_size = platform::host_to_be32(frame_size);
    uint8_t opcode = static_cast<uint8_t>(ReplOp::REPLICA_SYNC_END);
    ReplicaSyncHeader hdr;
    std::memset(&hdr, 0, sizeof(hdr));
    hdr.topic_len = req.topic_len;
    std::memcpy(hdr.topic, req.topic, req.topic_len);
    hdr.partition = req.partition;
    platform::send_data(fd, &net_frame_size, 4);
    platform::send_data(fd, &opcode, 1);
    platform::send_data(fd, &hdr, sizeof(hdr));
  }

  void ReplicationServer::handle_replica_ack(const ReplicaAck &req) {
    if (!replica_ack_cb_)
      return;
    const std::string topic(req.topic, req.topic_len);
    uint16_t partition = platform::be16_to_host(req.partition);
    uint64_t acked_offset = platform::be64_to_host(req.acked_offset);
    uint32_t follower_id = platform::be32_to_host(req.follower_id);
    replica_ack_cb_(follower_id, acked_offset, topic, partition);
  }

  void ReplicationServer::handle_heartbeat(socket_t fd, const Heartbeat &hb) {
    (void)fd;
    (void)hb;
  }

  void ReplicationServer::handle_leader_election(const LeaderElection &le) {
    auto &meta = meta_reg_.get(le.topic, le.topic_len, platform::be16_to_host(le.partition));
    std::lock_guard<std::mutex> lk(meta.mu);

    uint32_t incoming_epoch = platform::be32_to_host(le.epoch);
    if (incoming_epoch <= meta.epoch)
      return;

    fell::Partition *p = topic_reg_.get_partition(std::string(le.topic, le.topic_len),
                                                  platform::be16_to_host(le.partition));

    uint64_t incoming_offset = platform::be64_to_host(le.committed_offset);
    if (p && incoming_offset < p->committed_offset())
      return;

    meta.epoch = incoming_epoch;
    meta.leader_id = platform::be32_to_host(le.new_leader_id);
    meta.role =
        (meta.leader_id == cfg_.broker_id) ? PartitionRole::Leader : PartitionRole::Follower;
  }

} // namespace fell::repl
