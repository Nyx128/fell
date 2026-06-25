#define NOMINMAX
#include "replication/replica_manager.hpp"
#include "platform/endian.hpp"
#include "platform/socket.hpp"

#include <algorithm>
#include <cstring>

namespace fell::repl {

  ReplicaManager::ReplicaManager(PartitionMeta &meta, const ClusterConfig &cfg)
      : meta_(meta), cfg_(cfg) {
  }

  ReplicaManager::~ReplicaManager() {
    stop();
  }

  void ReplicaManager::start() {
    if (running_)
      return;
    running_ = true;
    worker_thread_ = std::thread(&ReplicaManager::worker_loop, this);
  }

  void ReplicaManager::stop() {
    if (!running_)
      return;
    running_ = false;
    queue_cv_.notify_all();
    if (worker_thread_.joinable())
      worker_thread_.join();
  }

  void ReplicaManager::register_pending(uint64_t offset, std::vector<uint8_t> ack_response,
                                        int producer_fd) {
    std::lock_guard<std::mutex> lk(pending_mu_);
    std::lock_guard<std::mutex> meta_lk(meta_.mu);

    PendingAck pa;
    pa.offset = offset;
    pa.ack_response = std::move(ack_response);
    pa.producer_fd = producer_fd;
    // Followers only — the leader's own commit is tracked via leader_committed.
    pa.isr_required =
        static_cast<uint32_t>(std::max(0, static_cast<int>(meta_.isr_ids().size()) - 1));

    pending_.push_back(std::move(pa));
  }

  void ReplicaManager::enqueue_committed(uint64_t base_offset,
                                         const std::vector<storage::CommittedRecord> &records) {
    std::lock_guard<std::mutex> lk(queue_mu_);
    commit_queue_.push_back({base_offset, records});
    queue_cv_.notify_one();
  }

  void ReplicaManager::set_follower_fd(uint32_t broker_id, int fd) {
    std::lock_guard<std::mutex> lk(fds_mu_);
    follower_fds_[broker_id] = fd;
  }

  std::vector<std::pair<int, std::vector<uint8_t>>>
  ReplicaManager::on_replica_ack(uint32_t follower_id, uint64_t acked_offset) {
    std::vector<std::pair<int, std::vector<uint8_t>>> to_send;
    std::lock_guard<std::mutex> lk(pending_mu_);
    std::lock_guard<std::mutex> meta_lk(meta_.mu);

    meta_.on_replica_ack(follower_id, acked_offset, cfg_.max_lag_messages,
                         meta_.leader_committed_offset());

    for (auto &pa : pending_) {
      if (acked_offset > pa.offset && pa.acked_followers.insert(follower_id).second) {
        ++pa.isr_acked;
        auto ready = try_release(pa);
        to_send.insert(to_send.end(), ready.begin(), ready.end());
      }
    }

    pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
                                  [](const PendingAck &p) {
                                    return p.leader_committed && p.isr_acked >= p.isr_required;
                                  }),
                   pending_.end());

    return to_send;
  }

  std::vector<std::pair<int, std::vector<uint8_t>>> ReplicaManager::try_release(PendingAck &pa) {
    const bool quorum_met = (pa.isr_required == 0)
                                ? pa.leader_committed
                                : (pa.leader_committed && pa.isr_acked >= pa.isr_required);

    if (!quorum_met)
      return {};

    std::vector<std::pair<int, std::vector<uint8_t>>> ret;
    if (!pa.ack_response.empty())
      ret.push_back({pa.producer_fd, std::move(pa.ack_response)});
    return ret;
  }

  void ReplicaManager::worker_loop() {
    while (running_) {
      QueuedCommit qc;
      {
        std::unique_lock<std::mutex> lk(queue_mu_);
        queue_cv_.wait(lk, [this] { return !running_ || !commit_queue_.empty(); });
        if (!running_ && commit_queue_.empty())
          break;
        qc = std::move(commit_queue_.front());
        commit_queue_.pop_front();
      }

      // Release deferred producer ACKs whose offset was just committed by the leader.
      std::vector<std::pair<int, std::vector<uint8_t>>> to_send;
      {
        std::lock_guard<std::mutex> lk(pending_mu_);
        for (auto &pa : pending_) {
          if (pa.offset >= qc.base_offset && pa.offset < qc.base_offset + qc.records.size()) {
            pa.leader_committed = true;
            auto ready = try_release(pa);
            to_send.insert(to_send.end(), ready.begin(), ready.end());
          }
        }
        pending_.erase(std::remove_if(pending_.begin(), pending_.end(),
                                      [](const PendingAck &p) {
                                        return p.leader_committed && p.isr_acked >= p.isr_required;
                                      }),
                       pending_.end());
      }

      if (post_response_cb_) {
        for (auto &[fd, resp] : to_send)
          post_response_cb_(fd, std::move(resp));
      }

      // Update the leader's own ack_offset so isr_acked() is accurate.
      uint32_t epoch = 0;
      {
        std::lock_guard<std::mutex> meta_lk(meta_.mu);
        if (!qc.records.empty()) {
          const uint64_t committed = qc.records.back().offset + 1;
          for (auto &r : meta_.replicas) {
            if (r.broker_id == meta_.leader_id) {
              r.ack_offset = std::max(r.ack_offset, committed);
              break;
            }
          }
        }
        epoch = meta_.epoch;
      }

      // Stream each committed record to every connected follower socket.
      std::lock_guard<std::mutex> fds_lk(fds_mu_);
      for (const auto &[follower_id, fd] : follower_fds_) {
        for (const auto &rec : qc.records) {
          ReplicaSyncHeader hdr;
          std::memset(&hdr, 0, sizeof(hdr));
          hdr.topic_len = static_cast<uint8_t>(std::min(meta_.topic.size(), size_t(255)));
          std::memcpy(hdr.topic, meta_.topic.data(), hdr.topic_len);
          hdr.partition = platform::host_to_be16(meta_.partition_idx);
          hdr.epoch = platform::host_to_be32(epoch);
          hdr.offset = platform::host_to_be64(rec.offset);
          hdr.timestamp_ms = platform::host_to_be64(rec.timestamp_ms);
          hdr.payload_size = platform::host_to_be32(static_cast<uint32_t>(rec.payload.size()));

          const uint32_t frame_size =
              sizeof(ReplicaSyncHeader) + static_cast<uint32_t>(rec.payload.size()) + 1;
          const uint32_t net_frame_size = platform::host_to_be32(frame_size);
          const uint8_t opcode = static_cast<uint8_t>(ReplOp::REPLICA_SYNC);

          platform::send_data(fd, &net_frame_size, 4);
          platform::send_data(fd, &opcode, 1);
          platform::send_data(fd, &hdr, sizeof(hdr));
          if (!rec.payload.empty())
            platform::send_data(fd, rec.payload.data(), rec.payload.size());
        }
      }
    }
  }

} // namespace fell::repl
