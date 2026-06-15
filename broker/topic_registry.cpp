#include "broker/topic_registry.hpp"
#include "storage/partition_store.hpp"
#include <chrono>
#include <memory>
#include <shared_mutex>

namespace fell {

  Partition::Partition(const std::filesystem::path &data_dir,
                       storage::StorageOptions storage_options) {
    store_ = std::make_unique<storage::PartitionStore>(data_dir, storage_options);
  }

  storage::AppendResult Partition::append(const uint8_t *payload, uint32_t size) {
    return store_->append(payload, size);
  }

  std::vector<Message> Partition::fetch(uint64_t offset, uint16_t max_count) const {
    return store_->fetch(offset, max_count);
  }

  uint64_t Partition::next_offset() const {
    return store_->next_offset();
  }

  void Partition::set_commit_callback(storage::CommitCallback cb) {
    store_->set_commit_callback(std::move(cb));
  }

  void Partition::set_once_commit_callback(uint64_t offset, std::function<void()> cb) {
    store_->set_once_commit_callback(offset, std::move(cb));
  }

  uint64_t Partition::committed_offset() const {
    return store_->committed_offset();
  }

  TopicRegistry::TopicRegistry(std::filesystem::path data_root,
                               storage::StorageOptions storage_options)
      : data_root_(std::move(data_root)), storage_options_(storage_options) {
  }

  void TopicRegistry::recover_all() {
    std::unique_lock<std::shared_mutex> lock(mu_);

    if (!std::filesystem::exists(data_root_) || !std::filesystem::is_directory(data_root_)) {
      return;
    }

    for (const auto &topic_entry : std::filesystem::directory_iterator(data_root_)) {
      if (!topic_entry.is_directory())
        continue;

      std::string topic_name = topic_entry.path().filename().string();
      std::vector<std::filesystem::path> partition_dirs;

      for (const auto &part_entry : std::filesystem::directory_iterator(topic_entry.path())) {
        if (!part_entry.is_directory())
          continue;
        std::string part_name = part_entry.path().filename().string();
        if (part_name.rfind("partition-", 0) == 0) {
          partition_dirs.push_back(part_entry.path());
        }
      }

      if (partition_dirs.empty())
        continue;

      std::vector<std::unique_ptr<Partition>> partitions;

      for (const auto &p_dir : partition_dirs) {
        std::string name = p_dir.filename().string();
        size_t idx = std::stoull(name.substr(10)); // "partition-" is 10 chars
        if (idx >= partitions.size()) {
          partitions.resize(idx + 1);
        }
        partitions[idx] = std::make_unique<Partition>(p_dir);
      }

      topics_[topic_name] = std::move(partitions);
    }
  }

  bool TopicRegistry::create_topic(const std::string &name, uint16_t num_partitions) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    // Check if topic already exists
    if (topics_.find(name) != topics_.end()) {
      return false;
    }
    auto topic_dir = data_root_ / name;

    // Create the partitions
    std::vector<std::unique_ptr<Partition>> partitions;
    partitions.reserve(num_partitions);
    for (uint16_t i = 0; i < num_partitions; ++i) {
      auto partition_dir = topic_dir / ("partition-" + std::to_string(i));
      std::filesystem::create_directories(partition_dir);
      partitions.push_back(std::make_unique<Partition>(partition_dir, storage_options_));
    }
    topics_[name] = std::move(partitions);
    return true;
  }

  // Hot path: shared (reader) lock — multiple concurrent fetches/appends
  // can proceed in parallel as long as no topic is being created.
  Partition *TopicRegistry::get_partition(std::string_view topic, uint16_t partition) {
    std::shared_lock<std::shared_mutex> lock(mu_);

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
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = topics_.find(std::string(topic));
    if (it == topics_.end()) {
      return 0;
    }
    return static_cast<uint16_t>(it->second.size());
  }

  std::vector<TopicRegistry::TopicInfo> TopicRegistry::list_topics() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<TopicInfo> topics;
    topics.reserve(topics_.size());
    for (const auto &[name, partitions] : topics_) {
      topics.push_back({name, static_cast<uint16_t>(partitions.size())});
    }
    return topics;
  }

} // namespace fell
