#pragma once

#include "log_format.hpp"
#include <cstdint>
#include <filesystem>
#include <vector>

namespace fell::storage {

  /**
   * @class OffsetIndex
   * @brief In-memory sparse index mapping logical offsets to physical byte positions.
   * 
   * Design Insight:
   * Keeps segment indices lean. Rather than writing one index entry per record, we write 
   * one entry every `INDEX_INTERVAL` records. The lookup executes a fast in-memory binary 
   * search (`std::lower_bound`) to identify the closest preceding physical file boundary,
   * minimizing random read disk latency and kernel I/O hops.
   */
  class OffsetIndex {
  public:
    /**
     * @brief Loads an index file (`.idx`) completely into memory.
     * @param idx_path Path to the `.idx` file.
     */
    explicit OffsetIndex(const std::filesystem::path &idx_path);

    // Disable copy
    OffsetIndex(const OffsetIndex &) = delete;
    OffsetIndex &operator=(const OffsetIndex &) = delete;

    /**
     * @brief Finds the closest physical log position preceding or equal to the target offset.
     * @param offset Target logical offset.
     * @return Physical byte offset in the corresponding `.log` segment file.
     */
    uint64_t lookup(uint64_t offset) const;

    /// @brief Gets total sparse index entries.
    size_t size() const { return entries_.size(); }

  private:
    std::vector<IndexEntry> entries_;
  };

} // namespace fell::storage
