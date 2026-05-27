#pragma once
#include "log_format.hpp"
#include "segment_writer.hpp"
#include "segment_reader.hpp"
#include "offset_index.hpp"
#include "debug_mutex.hpp"
#include <filesystem>
#include <vector>
#include <memory>

namespace fell::storage {

// Mirrors the Message struct from broker/topic_registry.hpp so that
// Partition does not need to depend on storage internals.
struct StoredMessage {
    uint64_t             offset;
    uint64_t             timestamp_ms;
    std::vector<uint8_t> payload;
};

class PartitionStore {
public:
    // Opens an existing partition or creates a new one.
    explicit PartitionStore(const std::filesystem::path& partition_dir);

    // Append a message. Returns assigned offset.
    uint64_t append(const uint8_t* payload, uint32_t size);

    // Fetch up to max_count messages starting at offset.
    std::vector<StoredMessage> fetch(uint64_t offset, uint16_t max_count) const;

    uint64_t next_offset() const;

private:
    // Given a target offset, find the segment whose base_offset <= target.
    std::filesystem::path find_segment_for(uint64_t offset) const;

    std::filesystem::path            dir_;
    std::unique_ptr<SegmentWriter>   writer_;
    mutable DebugMutex               mu_;

    // Sorted list of (base_offset, log_path) for all sealed segments.
    // The active segment is always writer_->base_offset().
    struct SegmentMeta { uint64_t base_offset; std::filesystem::path path; };
    std::vector<SegmentMeta>         sealed_segments_;
};

} // namespace fell::storage
