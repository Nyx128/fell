#pragma once
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "storage/partition_store.hpp"
#include <filesystem>

namespace fell {

  using Message = storage::StoredMessage;

  class Partition {
  public:
    Partition(const std::filesystem::path &data_dir);

    // Appends payload, assigns next offset, returns assigned offset.
    uint64_t append(std::vector<uint8_t> payload);

    // Returns up to max_count messages starting at offset.
    // Returns empty vector if offset >= next_offset_.
    std::vector<Message> fetch(uint64_t offset, uint16_t max_count) const;

    uint64_t next_offset() const;

  private:
    std::unique_ptr<storage::PartitionStore> store_;
  };

  class TopicRegistry {
  public:
    TopicRegistry(std::filesystem::path data_root);

    void recover_all();
    // Returns false if topic already exists.
    bool create_topic(const std::string &name, uint16_t num_partitions);

    // Returns nullptr if topic or partition index does not exist.
    // std::string_view avoids an allocation on every hot-path lookup.
    Partition *get_partition(std::string_view topic, uint16_t partition);

    uint16_t num_partitions(std::string_view topic) const;

  private:
    std::unordered_map<std::string, std::vector<std::unique_ptr<Partition>>> topics_;
    std::filesystem::path data_root_;
    mutable std::mutex mu_;
  };

} // namespace fell
