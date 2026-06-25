#include "replication/partition_meta.hpp"
#include <algorithm>

namespace fell::repl {

  std::vector<uint32_t> PartitionMeta::isr_ids() const {
    std::vector<uint32_t> ids;
    for (const auto &r : replicas)
      if (r.in_isr)
        ids.push_back(r.broker_id);
    return ids;
  }

  bool PartitionMeta::isr_acked(uint64_t offset) const {
    for (const auto &r : replicas)
      if (r.in_isr && r.ack_offset < offset)
        return false;
    return true;
  }

  void PartitionMeta::on_replica_ack(uint32_t broker_id, uint64_t acked_offset, uint64_t max_lag,
                                     uint64_t leader_committed) {
    for (auto &r : replicas) {
      if (r.broker_id != broker_id)
        continue;
      r.ack_offset = std::max(r.ack_offset, acked_offset);
      // Re-admit the replica to the ISR once it has caught up.
      if (!r.in_isr && leader_committed >= r.ack_offset &&
          (leader_committed - r.ack_offset) <= max_lag)
        r.in_isr = true;
      break;
    }
  }

  uint64_t PartitionMeta::leader_committed_offset() const {
    for (const auto &r : replicas)
      if (r.broker_id == leader_id)
        return r.ack_offset;
    return 0;
  }

  void PartitionMeta::tick_isr(uint64_t now_ms, uint32_t timeout_ms) {
    for (auto &r : replicas) {
      if (r.broker_id == leader_id)
        continue;
      if (r.in_isr && (now_ms - r.last_seen_ms) > timeout_ms)
        r.in_isr = false;
    }
  }

  static std::string make_key(const std::string &topic, uint16_t partition) {
    return topic + "-" + std::to_string(partition);
  }

  PartitionMeta &PartitionMetaRegistry::get(const std::string &topic, uint16_t partition) {
    std::lock_guard<std::mutex> lk(mu_);
    const std::string key = make_key(topic, partition);
    auto it = meta_.find(key);
    if (it == meta_.end()) {
      auto m = std::make_unique<PartitionMeta>();
      m->topic = topic;
      m->partition_idx = partition;
      it = meta_.emplace(key, std::move(m)).first;
    }
    return *it->second;
  }

  PartitionMeta &PartitionMetaRegistry::get(const char *topic, uint8_t topic_len,
                                            uint16_t partition) {
    return get(std::string(topic, topic_len), partition);
  }

  uint32_t PartitionMetaRegistry::epoch(const std::string &topic, uint16_t partition) {
    auto &m = get(topic, partition);
    std::lock_guard<std::mutex> lk(m.mu);
    return m.epoch;
  }

  uint32_t PartitionMetaRegistry::epoch(const char *topic, uint8_t topic_len, uint16_t partition) {
    return epoch(std::string(topic, topic_len), partition);
  }

  void PartitionMetaRegistry::assign_roles(uint32_t broker_id, uint32_t num_brokers,
                                           const std::vector<std::string> &topics,
                                           const std::vector<uint16_t> &partition_counts) {
    if (num_brokers == 0)
      return;

    for (size_t i = 0; i < topics.size(); ++i) {
      const uint16_t count = (i < partition_counts.size()) ? partition_counts[i] : 1;
      for (uint16_t p = 0; p < count; ++p) {
        auto &meta = get(topics[i], p);
        std::lock_guard<std::mutex> lk(meta.mu);

        meta.leader_id = p % num_brokers;
        meta.role = (meta.leader_id == broker_id) ? PartitionRole::Leader : PartitionRole::Follower;

        meta.replicas.clear();
        meta.replicas.reserve(num_brokers);
        for (uint32_t rid = 0; rid < num_brokers; ++rid)
          meta.replicas.push_back({rid, 0, 0, 0, true});
      }
    }
  }

} // namespace fell::repl
