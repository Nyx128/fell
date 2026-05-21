#include "broker/topic_registry.hpp"
#include <chrono>

namespace fell {
  uint64_t Partition::append(std::vector<uint8_t> payload) {
    std::lock_guard<std::mutex> lock(mu_);

    Message msg;
    msg.offset = next_offset_;

    // Get current time in milliseconds
    auto now = std::chrono::system_clock::now();
    msg.timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    msg.payload = std::move(payload);

    log_.push_back(std::move(msg));

    return next_offset_++;
  }

  std::vector<Message> Partition::fetch(uint64_t offset, uint16_t max_count) const {
    std::lock_guard<std::mutex> lock(mu_);

    // If the requested offset hasn't been written yet, return nothing
    if (offset >= next_offset_) {
      return {};
    }

    std::vector<Message> result;

    // Calculate how many messages can be returned
    size_t available = static_cast<size_t>(next_offset_ - offset);
    size_t count = std::min(static_cast<size_t>(max_count), available);

    result.reserve(count);

    // jump directly to offset
    auto it = log_.begin() + static_cast<ptrdiff_t>(offset);
    for (size_t i = 0; i < count; ++i) {
      result.push_back(*it++);
    }

    return result;
  }

  uint64_t Partition::next_offset() const {
    std::lock_guard<std::mutex> lock(mu_);
    return next_offset_;
  }

  bool TopicRegistry::create_topic(const std::string &name, uint16_t num_partitions) {
    std::lock_guard<std::mutex> lock(mu_);
    // Check if topic already exists
    if (topics_.find(name) != topics_.end()) {
      return false;
    }
    // Create the partitions
    std::vector<std::unique_ptr<Partition>> partitions;
    partitions.reserve(num_partitions);
    for (uint16_t i = 0; i < num_partitions; ++i) {
      partitions.push_back(std::make_unique<Partition>());
    }
    topics_[name] = std::move(partitions);
    return true;
  }

  Partition *TopicRegistry::get_partition(std::string_view topic, uint16_t partition) {
    std::lock_guard<std::mutex> lock(mu_);

    auto it = topics_.find(std::string(topic));
    if (it == topics_.end()) {
      return nullptr;
    }
    if (partition >= it->second.size()) {
      return nullptr;
    }
    return it->second[partition].get();
  }

  uint16_t TopicRegistry::num_partitions(std::string_view topic) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = topics_.find(std::string(topic));
    if (it == topics_.end()) {
      return 0;
    }
    return static_cast<uint16_t>(it->second.size());
  }

} // namespace fell