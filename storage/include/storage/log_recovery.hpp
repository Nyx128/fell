#pragma once
#include <cstdint>
#include <filesystem>

namespace fell::storage {

struct RecoveryResult {
    uint64_t next_offset;       // offset to assign to the next new message
    uint64_t log_end_position;  // byte position at end of last valid record
    uint32_t records_recovered; // how many valid records were found
    bool     truncated;         // true if a partial write was trimmed
};

// Scans every segment in partition_dir in order.
// The last segment is fully validated; earlier segments are trusted.
// Returns the state needed to resume writing.
RecoveryResult recover_partition(const std::filesystem::path& partition_dir);

} // namespace fell::storage
