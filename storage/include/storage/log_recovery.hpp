#pragma once

#include <cstdint>
#include <filesystem>

namespace fell::storage {

  /**
   * @struct RecoveryResult
   * @brief Status of log parsing, truncation, and recovered state on partition startup.
   */
  struct RecoveryResult {
    uint64_t next_offset;       ///< Safe logical offset to resume writing
    uint64_t log_end_position;  ///< Accurate byte size of the active segment
    uint32_t records_recovered; ///< Total healthy records found in active segment
    bool truncated;             ///< True if a corrupt/partial append tail was resized away
  };

  /**
   * @brief Performs validation and recovery on a partition directory.
   * 
   * Design Insight:
   * Scans log segments in alphabetical/offset order. While historical sealed segments
   * are trusted as immutable, the active segment is scanned record-by-record to identify and 
   * cleanly slice off partial/corrupted writes from sudden crash/power loss scenarios.
   * 
   * @param partition_dir Partition directory path.
   * @return RecoveryResult containing state to safely resume log operations.
   */
  RecoveryResult recover_partition(const std::filesystem::path &partition_dir);

} // namespace fell::storage
