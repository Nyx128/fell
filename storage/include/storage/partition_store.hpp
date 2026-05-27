#pragma once
#include "log_format.hpp"
#include "offset_index.hpp"
#include "segment_reader.hpp"
#include "segment_writer.hpp"
#include "debug_mutex.hpp"
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <unordered_map>
#include <vector>

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

    // Per-segment cache: shared index + persistent open reader fd.
    // Both are built lazily on first fetch and evicted on rotation.
    struct SegmentCache {
        std::shared_ptr<OffsetIndex>   index;
        std::shared_ptr<SegmentReader> reader;
    };
    // Cache keyed by log path string. Only accessed under mu_.
    mutable std::unordered_map<std::string, SegmentCache> segment_cache_;

    std::filesystem::path            dir_;
    std::unique_ptr<SegmentWriter>   writer_;
    mutable DebugMutex               mu_;

    // Sorted list of (base_offset, log_path) for all sealed segments.
    // The active segment is always writer_->base_offset().
    struct SegmentMeta { uint64_t base_offset; std::filesystem::path path; };
    std::vector<SegmentMeta>         sealed_segments_;

    // ── Bottleneck 2: Coarse timestamp cache ──────────────────────────────
    // system_clock::now() is a vDSO call (~30ns) taken only once per
    // timestamp_refresh_every_ appends; intermediate writes reuse the
    // cached value. Millisecond granularity makes this safe.
    static constexpr uint32_t kTimestampRefreshEvery = 16;
    uint64_t                         last_timestamp_ms_ = 0;
    uint32_t                         ts_counter_        = 0;

    // ── Bottleneck 3: Flush gate ──────────────────────────────────────────
    // Ensures only one thread executes the slow fdatasync per flush window.
    // Set to true under mu_ when a thread volunteers to flush; cleared
    // (still outside the lock) after the flush completes.
    std::atomic<bool>                flush_in_flight_{false};
};

} // namespace fell::storage
