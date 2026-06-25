#define NOMINMAX
#include "replication/replication_client.hpp"
#include "platform/endian.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>

namespace fell::repl {

  ReplicationClient::ReplicationClient(const std::string &topic, uint16_t partition,
                                       const ClusterConfig &cfg, PartitionMetaRegistry &meta_reg,
                                       fell::TopicRegistry &topic_reg)
      : topic_(topic), partition_(partition), cfg_(cfg), meta_reg_(meta_reg), topic_reg_(topic_reg),
        poller_(platform::make_poller()) {
  }

  ReplicationClient::~ReplicationClient() {
    stop();
    if (notify_read_fd_ != INVALID_SOCKET_T)
      platform::close_socket(notify_read_fd_);
    if (notify_write_fd_ != INVALID_SOCKET_T)
      platform::close_socket(notify_write_fd_);
    if (fd_ != INVALID_SOCKET_T)
      platform::close_socket(fd_);
  }

  void ReplicationClient::start() {
    if (running_)
      return;

    platform::create_notify_pair(&notify_read_fd_, &notify_write_fd_);
    poller_->add(notify_read_fd_, platform::PF_READ, &notify_sentinel_);

    running_ = true;
    thread_ = std::thread(&ReplicationClient::event_loop, this);
  }

  void ReplicationClient::stop() {
    if (!running_)
      return;
    running_ = false;
    if (notify_write_fd_ != INVALID_SOCKET_T) {
      uint8_t dummy = 1;
      platform::send_data(notify_write_fd_, &dummy, 1);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void ReplicationClient::connect_to_leader() {
    if (fd_ != INVALID_SOCKET_T)
      return;

    auto &meta = meta_reg_.get(topic_, partition_);
    uint32_t leader_id;
    {
      std::lock_guard<std::mutex> lk(meta.mu);
      leader_id = meta.leader_id;
    }

    if (leader_id == cfg_.broker_id) {
      maybe_elect_self();
      return;
    }

    const BrokerAddr *leader_addr = nullptr;
    for (const auto &peer : cfg_.peers) {
      if (peer.broker_id == leader_id) {
        leader_addr = &peer;
        break;
      }
    }

    if (!leader_addr)
      return;

    fd_ = platform::connect_socket(leader_addr->host.c_str(), leader_addr->repl_port);
    if (fd_ != INVALID_SOCKET_T) {
      platform::set_nonblocking(fd_);
      poller_->add(fd_, platform::PF_READ, &fd_);
      send_fetch_log();
    } else {
      // Leader might be down. Trigger election.
      maybe_elect_self();
    }
  }

  void ReplicationClient::send_fetch_log() {
    FetchLogReq req = build_fetch_log_request();

    uint32_t frame_size = sizeof(FetchLogReq) + 1;
    uint32_t net_frame_size = platform::host_to_be32(frame_size);
    uint8_t opcode = static_cast<uint8_t>(ReplOp::FETCH_LOG);

    platform::send_data(fd_, &net_frame_size, 4);
    platform::send_data(fd_, &opcode, 1);
    platform::send_data(fd_, &req, sizeof(req));
  }

  FetchLogReq ReplicationClient::build_fetch_log_request() const {
    FetchLogReq req;
    std::memset(&req, 0, sizeof(req));
    req.topic_len = static_cast<uint8_t>(std::min(topic_.size(), static_cast<size_t>(255)));
    std::memcpy(req.topic, topic_.c_str(), req.topic_len);
    req.partition = platform::host_to_be16(partition_);

    uint64_t start_offset = 0;
    if (fell::Partition *p = topic_reg_.get_partition(topic_, partition_)) {
      start_offset = p->committed_offset();
    }
    req.start_offset = platform::host_to_be64(start_offset);
    req.follower_id = platform::host_to_be32(cfg_.broker_id);
    return req;
  }

  void ReplicationClient::event_loop() {
    platform::PollEvent events[64];
    while (running_) {
      if (fd_ == INVALID_SOCKET_T) {
        connect_to_leader();
        if (fd_ == INVALID_SOCKET_T) {
          // Sleep to avoid spinning when leader is down
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }

      int n = poller_->wait(events, 64, 100);
      for (int i = 0; i < n; ++i) {
        auto &ev = events[i];
        if (ev.ctx == &notify_sentinel_) {
          uint8_t buf[64];
          platform::recv_data(notify_read_fd_, buf, sizeof(buf));

          std::vector<ReplicaAck> acks;
          {
            std::lock_guard<std::mutex> lk(out_mu_);
            acks.swap(out_acks_);
          }
          if (fd_ != INVALID_SOCKET_T) {
            for (const auto &ack : acks) {
              uint32_t frame_size = sizeof(ReplicaAck) + 1;
              uint32_t net_frame_size = platform::host_to_be32(frame_size);
              uint8_t opcode = static_cast<uint8_t>(ReplOp::REPLICA_ACK);

              platform::send_data(fd_, &net_frame_size, 4);
              platform::send_data(fd_, &opcode, 1);
              platform::send_data(fd_, &ack, sizeof(ack));
            }
          }
          continue;
        }
        if (ev.flags & platform::PF_HUP) {
          poller_->remove(fd_);
          platform::close_socket(fd_);
          fd_ = INVALID_SOCKET_T;
          recv_buf_.clear();
          continue;
        }
        if (ev.flags & platform::PF_READ) {
          on_readable();
        }
      }
    }
  }

  void ReplicationClient::on_readable() {
    uint8_t temp[4096];
    while (true) {
      int n = platform::recv_data(fd_, temp, sizeof(temp));
      if (n == 0) {
        poller_->remove(fd_);
        platform::close_socket(fd_);
        fd_ = INVALID_SOCKET_T;
        recv_buf_.clear();
        return;
      }
      if (n < 0) {
        if (platform::would_block())
          break;
        poller_->remove(fd_);
        platform::close_socket(fd_);
        fd_ = INVALID_SOCKET_T;
        recv_buf_.clear();
        return;
      }
      recv_buf_.insert(recv_buf_.end(), temp, temp + n);

      while (recv_buf_.size() >= 5) {
        uint32_t net_len;
        std::memcpy(&net_len, recv_buf_.data(), 4);
        uint32_t len = platform::be32_to_host(net_len);
        if (recv_buf_.size() >= 4 + len) {
          uint8_t opcode = recv_buf_[4];
          handle_frame(opcode, recv_buf_.data() + 5, len - 1);
          recv_buf_.erase(recv_buf_.begin(), recv_buf_.begin() + 4 + len);
        } else {
          break;
        }
      }
    }
  }

  void ReplicationClient::handle_frame(uint8_t opcode, const uint8_t *payload, size_t size) {
    auto op = static_cast<ReplOp>(opcode);
    if (op == ReplOp::REPLICA_SYNC) {
      if (size >= sizeof(ReplicaSyncHeader)) {
        ReplicaSyncHeader hdr;
        std::memcpy(&hdr, payload, sizeof(ReplicaSyncHeader));
        on_replica_sync(hdr, payload + sizeof(ReplicaSyncHeader));
      }
    } else if (op == ReplOp::REPLICA_SYNC_END) {
      // Log sync finished, wait for further updates from leader
    }
  }

  void ReplicationClient::on_replica_sync(const ReplicaSyncHeader &hdr, const uint8_t *payload) {
    const std::string topic(hdr.topic, hdr.topic_len);
    fell::Partition *p = topic_reg_.get_partition(topic, platform::be16_to_host(hdr.partition));
    if (!p)
      return;

    const uint64_t expected_offset = platform::be64_to_host(hdr.offset);

    if (expected_offset != p->next_offset()) {
      // Offset gap — reconnect and re-fetch
      poller_->remove(fd_);
      platform::close_socket(fd_);
      fd_ = INVALID_SOCKET_T;
      recv_buf_.clear();
      return;
    }

    const uint32_t payload_size = platform::be32_to_host(hdr.payload_size);
    std::vector<uint8_t> msg_payload(payload, payload + payload_size);

    // Register commit callback BEFORE appending so it is in place
    // when the I/O thread fires
    p->set_once_commit_callback(expected_offset, [this, hdr]() {
      ReplicaAck ack{};
      std::memset(&ack, 0, sizeof(ack));
      ack.topic_len = hdr.topic_len;
      std::memcpy(ack.topic, hdr.topic, hdr.topic_len);
      ack.partition = hdr.partition;
      ack.epoch = hdr.epoch;
      uint64_t offset_val = platform::be64_to_host(hdr.offset);
      ack.acked_offset = platform::host_to_be64(offset_val + 1);
      ack.follower_id = platform::host_to_be32(cfg_.broker_id);

      {
        std::lock_guard<std::mutex> lk(out_mu_);
        out_acks_.push_back(ack);
      }
      uint8_t byte = 1;
      platform::send_data(notify_write_fd_, &byte, 1);
    });

    auto result = p->append(msg_payload.data(), static_cast<uint32_t>(msg_payload.size()));
    if (!result.accepted) {
      poller_->remove(fd_);
      platform::close_socket(fd_);
      fd_ = INVALID_SOCKET_T;
      recv_buf_.clear();
    }
  }

  void ReplicationClient::maybe_elect_self() {
    fell::Partition *p = topic_reg_.get_partition(topic_, partition_);
    if (!p)
      return;

    auto &meta = meta_reg_.get(topic_, partition_);
    {
      std::lock_guard<std::mutex> lk(meta.mu);
      if (meta.role == PartitionRole::Candidate)
        return;
      meta.role = PartitionRole::Candidate;
      meta.epoch += 1;
    }

    LeaderElection le{};
    std::memset(&le, 0, sizeof(le));
    le.topic_len = static_cast<uint8_t>(std::min(topic_.size(), static_cast<size_t>(255)));
    std::memcpy(le.topic, topic_.data(), le.topic_len);
    le.partition = platform::host_to_be16(partition_);
    le.new_leader_id = platform::host_to_be32(cfg_.broker_id);
    le.epoch = platform::host_to_be32(meta.epoch);
    le.committed_offset = platform::host_to_be64(p->committed_offset());

    for (const auto &peer : cfg_.peers) {
      if (peer.broker_id == cfg_.broker_id)
        continue;
      broadcast_election(peer, le);
    }
  }

  void ReplicationClient::broadcast_election(const BrokerAddr &peer, const LeaderElection &le) {
    socket_t s = platform::connect_socket(peer.host.c_str(), peer.repl_port);
    if (s != INVALID_SOCKET_T) {
      uint32_t frame_size = sizeof(LeaderElection) + 1;
      uint32_t net_frame_size = platform::host_to_be32(frame_size);
      uint8_t opcode = static_cast<uint8_t>(ReplOp::LEADER_ELECTION);
      platform::send_data(s, &net_frame_size, 4);
      platform::send_data(s, &opcode, 1);
      platform::send_data(s, &le, sizeof(le));
      platform::close_socket(s);
    }
  }

} // namespace fell::repl
