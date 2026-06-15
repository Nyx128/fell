#pragma once

#include "storage/partition_store.hpp"
#include "storage/storage_options.hpp"
#include <cstdint>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace fell {

  using Message = storage::StoredMessage;

  /**
   * @class Partition
   * @brief Broker-level abstraction of a single log partition.
   *
   * Design Insight:
   * Acts as a clean intermediary boundary. Keeps network processing in the broker
   * separated from low-level storage batch thread concurrency, backpressure, and indexes.
   */
  class Partition {
  public:
    /**
     * @brief Loads or initializes a partition log store.
     * @param data_dir Path to the partition directory.
     * @param storage_options System tuning configurations.
     */
    Partition(const std::filesystem::path &data_dir, storage::StorageOptions storage_options = {});

    /**
     * @brief Appends a raw binary payload.
     * @return AppendResult detailing accepted state, assigned offset, or backpressure.
     */
    storage::AppendResult append(const uint8_t *payload, uint32_t size);

    /**
     * @brief Fetches sequential committed messages.
     * @param offset Absolute starting offset.
     * @param max_count Upper bound limit of messages to read.
     * @return List of retrieved committed messages.
     */
    std::vector<Message> fetch(uint64_t offset, uint16_t max_count) const;

    /// @brief Gets the next expected logical offset.
    uint64_t next_offset() const;

    void set_commit_callback(storage::CommitCallback cb);
    void set_once_commit_callback(uint64_t offset, std::function<void()> cb);
    uint64_t committed_offset() const;

  private:
    std::unique_ptr<storage::PartitionStore> store_;
  };

  /**
   * @class TopicRegistry
   * @brief Global registry managing topics and their distributed partition logs.
   *
   * Design Insight:
   * Combines high-concurrency read-write locks (`std::shared_mutex`) to optimize hot-path
   * routing lookups. `get_partition()` lookups utilize `std::string_view` to avoid
   * allocations on request hot paths.
   */
  class TopicRegistry {
  public:
    struct TopicInfo {
      std::string name;
      uint16_t partition_count;
    };

    /**
     * @brief Initializes the global registry.
     * @param data_root Root directory containing all topic partitions.
     * @param storage_options Tuning configurations.
     */
    TopicRegistry(std::filesystem::path data_root, storage::StorageOptions storage_options = {});

    // Disable copy
    TopicRegistry(const TopicRegistry &) = delete;
    TopicRegistry &operator=(const TopicRegistry &) = delete;

    /**
     * @brief Discovers and recovers existing partition logs on startup.
     */
    void recover_all();

    /**
     * @brief Declares and creates a new multi-partitioned topic.
     * @return True if created successfully, false if topic already exists.
     */
    bool create_topic(const std::string &name, uint16_t num_partitions);

    /**
     * @brief Hot-path routing lookup. Resolves a partition reference.
     * @return Raw pointer to the Partition, or nullptr if not found.
     */
    Partition *get_partition(std::string_view topic, uint16_t partition);

    /**
     * @brief Gets total configured partitions for a topic.
     */
    uint16_t num_partitions(std::string_view topic) const;

    /**
     * @brief Returns a snapshot of known topics and partition counts.
     */
    std::vector<TopicInfo> list_topics() const;

  private:
    std::unordered_map<std::string, std::vector<std::unique_ptr<Partition>>> topics_;
    std::filesystem::path data_root_;
    mutable std::shared_mutex mu_;
    storage::StorageOptions storage_options_;
  };

} // namespace fell
