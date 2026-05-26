#pragma once
#include "log_format.hpp"
#include <cstdint>
#include <filesystem>
#include <vector>

namespace fell::storage {

class OffsetIndex {
public:
    // Load an existing index from disk (read mode).
    explicit OffsetIndex(const std::filesystem::path& idx_path);

    // Returns the file_position in the .log to start scanning from
    // in order to find offset. Never returns a position past the target.
    // Returns 0 if offset < first entry (scan from start of segment).
    uint64_t lookup(uint64_t offset) const;

    // Number of entries loaded.
    size_t size() const { return entries_.size(); }

private:
    std::vector<IndexEntry> entries_;
};

} // namespace fell::storage
