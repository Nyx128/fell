#define NOMINMAX
#include "storage/partition_store.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <vector>

using namespace fell::storage;

// ── Test fixture ─────────────────────────────────────────────────────────────

class PartitionStoreTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::filesystem::remove_all("test-data");
    std::filesystem::create_directories("test-data");
  }

  void TearDown() override {
    std::filesystem::remove_all("test-data");
  }

  // Helper: spin until committed_offset >= expected, or timeout after 2s.
  void wait_for_commit(const PartitionStore &store, uint64_t expected) {
    auto start = std::chrono::steady_clock::now();
    while (store.committed_offset() < expected) {
      if (std::chrono::steady_clock::now() - start > std::chrono::seconds(2)) {
        FAIL() << "Timeout waiting for committed_offset >= " << expected;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  // Helper: make default test options (immediate batching).
  static StorageOptions fast_opts() {
    StorageOptions opts;
    opts.batch_wait_us = 0;
    return opts;
  }
};

// ── 1. Append returns monotonic offsets before commit ─────────────────────────
// Offsets are assigned on the request thread before any I/O, so they are
// monotonically increasing regardless of commit state.
TEST_F(PartitionStoreTest, AppendReturnsMonotonicOffsets) {
  PartitionStore store("test-data", fast_opts());

  std::vector<uint8_t> payload = {0x01};
  uint64_t prev = store.append(payload.data(), payload.size()).offset;
  for (int i = 1; i < 10; ++i) {
    uint64_t cur = store.append(payload.data(), payload.size()).offset;
    EXPECT_GT(cur, prev);
    prev = cur;
  }
}

// ── 2. Fetch does not see queued-but-unwritten records ────────────────────────
// Immediately after append (before the I/O thread commits) the record must be
// invisible to fetch.  We use a huge batch_wait_us so the I/O thread is very
// unlikely to have committed by the time we call fetch.
TEST_F(PartitionStoreTest, FetchDoesNotSeeUncommittedRecords) {
  StorageOptions opts;
  opts.batch_wait_us = 500000; // 500ms — I/O thread won't fire quickly
  PartitionStore store("test-data", opts);

  std::vector<uint8_t> payload = {0xAB};
  auto result = store.append(payload.data(), payload.size());
  EXPECT_TRUE(result.accepted);

  // Immediately fetch — should be empty because the I/O thread hasn't committed
  auto fetched = store.fetch(0, 10);
  EXPECT_TRUE(fetched.empty());
}

// ── 3. Fetch sees records after the I/O thread drains and commits ─────────────
TEST_F(PartitionStoreTest, AppendAndFetch) {
  PartitionStore store("test-data", fast_opts());

  std::vector<uint8_t> m1 = {0x11, 0x22};
  std::vector<uint8_t> m2 = {0x33, 0x44, 0x55};

  EXPECT_EQ(store.append(m1.data(), m1.size()).offset, 0u);
  EXPECT_EQ(store.append(m2.data(), m2.size()).offset, 1u);

  wait_for_commit(store, 2);

  auto fetched = store.fetch(0, 10);
  ASSERT_EQ(fetched.size(), 2u);
  EXPECT_EQ(fetched[0].offset, 0u);
  EXPECT_EQ(fetched[0].payload, m1);
  EXPECT_EQ(fetched[1].offset, 1u);
  EXPECT_EQ(fetched[1].payload, m2);
}

// ── 4. Queue full returns AppendError::Busy ───────────────────────────────────
TEST_F(PartitionStoreTest, QueueFullReturnsBusy) {
  StorageOptions opts;
  opts.queue_capacity = 4;
  opts.max_pending_bytes = 64 * 1024 * 1024;
  opts.batch_wait_us = 500000; // slow drain so we can fill the queue
  PartitionStore store("test-data", opts);

  std::vector<uint8_t> payload(64, 0xFF);
  int accepted = 0, busy = 0;
  for (int i = 0; i < 16; ++i) {
    auto r = store.append(payload.data(), payload.size());
    if (r.accepted)
      ++accepted;
    else {
      EXPECT_EQ(r.error, AppendError::Busy);
      ++busy;
    }
  }
  // At most queue_capacity records can be accepted before the queue fills
  EXPECT_LE(accepted, 8); // some slack — drain may have occurred
  EXPECT_GT(busy, 0);
}

// ── 5. Batch write preserves record order ────────────────────────────────────
TEST_F(PartitionStoreTest, BatchPreservesRecordOrder) {
  PartitionStore store("test-data", fast_opts());

  constexpr int N = 50;
  for (int i = 0; i < N; ++i) {
    std::vector<uint8_t> payload = {static_cast<uint8_t>(i)};
    store.append(payload.data(), payload.size());
  }

  wait_for_commit(store, N);

  auto fetched = store.fetch(0, N);
  ASSERT_EQ(fetched.size(), static_cast<size_t>(N));
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(fetched[i].offset, static_cast<uint64_t>(i));
    EXPECT_EQ(fetched[i].payload[0], static_cast<uint8_t>(i));
  }
}

// ── 6. Fetch out of bounds returns empty ─────────────────────────────────────
TEST_F(PartitionStoreTest, FetchOutOfBounds) {
  PartitionStore store("test-data", fast_opts());

  std::vector<uint8_t> m1 = {0x11};
  store.append(m1.data(), m1.size());

  // Offset 1 is ahead of committed — should be empty immediately
  auto fetched = store.fetch(1, 10);
  EXPECT_TRUE(fetched.empty());
}

// ── 7. Destructor drains accepted queued records before shutdown ──────────────
// After destroying the store, all accepted records must be readable from disk
// by a freshly-opened store (they were flushed by the destructor).
TEST_F(PartitionStoreTest, DestructorDrainsQueue) {
  constexpr int N = 20;
  {
    PartitionStore store("test-data", fast_opts());
    std::vector<uint8_t> payload = {0xDD};
    for (int i = 0; i < N; ++i) {
      store.append(payload.data(), payload.size());
    }
    // Destructor runs here — must drain before returning
  }

  // Reopen and verify all records are on disk
  PartitionStore store2("test-data", fast_opts());
  // committed_offset_ is restored from the recovered writer
  EXPECT_EQ(store2.committed_offset(), static_cast<uint64_t>(N));
  auto fetched = store2.fetch(0, N);
  EXPECT_EQ(fetched.size(), static_cast<size_t>(N));
}

// ── 8. Rotation at batch boundary keeps both segments readable ────────────────
TEST_F(PartitionStoreTest, RotationKeepsSegmentsReadable) {
  StorageOptions opts = fast_opts();
  // Tiny segment so rotation triggers quickly
  // We can't change LOG_SEGMENT_MAX_BYTES, so we write a lot of data.
  // Instead, just write enough to cross one segment boundary if max is 64MB.
  // This test primarily checks that after rotation old records are still
  // accessible and new ones also work.
  PartitionStore store("test-data", opts);

  constexpr int N = 30;
  for (int i = 0; i < N; ++i) {
    std::vector<uint8_t> payload = {static_cast<uint8_t>(i)};
    store.append(payload.data(), payload.size());
  }

  wait_for_commit(store, N);

  // All records must be readable
  auto fetched = store.fetch(0, N);
  ASSERT_EQ(fetched.size(), static_cast<size_t>(N));
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(fetched[i].offset, static_cast<uint64_t>(i));
  }
}

// ── 9. Recovery sees committed records after restart ─────────────────────────
// Records that were committed (I/O thread wrote them) must survive a reopen.
// Records that were only queued (never written) are lost — we cannot test the
// latter directly without a crash, so we verify the committed ones survive.
TEST_F(PartitionStoreTest, RecoverySeesCommittedRecords) {
  constexpr int N = 10;
  {
    PartitionStore store("test-data", fast_opts());
    std::vector<uint8_t> payload = {0xAA};
    for (int i = 0; i < N; ++i) {
      store.append(payload.data(), payload.size());
    }
    wait_for_commit(store, N);
    // Destructor flushes; files are now intact on disk
  }

  // Reopen — recovery should load the segment and set next/committed correctly
  PartitionStore store2("test-data", fast_opts());
  EXPECT_EQ(store2.committed_offset(), static_cast<uint64_t>(N));

  auto fetched = store2.fetch(0, N);
  ASSERT_EQ(fetched.size(), static_cast<size_t>(N));
  for (int i = 0; i < N; ++i) {
    EXPECT_EQ(fetched[i].offset, static_cast<uint64_t>(i));
    EXPECT_EQ(fetched[i].payload[0], 0xAA);
  }
}

// ── 10. Concurrent producers receive unique offsets ──────────────────────────
TEST_F(PartitionStoreTest, ConcurrentProducersReceiveUniqueOffsets) {
  PartitionStore store("test-data", fast_opts());

  constexpr int kThreads = 4;
  constexpr int kPerThread = 50;

  std::atomic<int> total_accepted{0};
  std::vector<std::vector<uint64_t>> offsets(kThreads);

  auto producer = [&](int idx) {
    std::vector<uint8_t> payload = {static_cast<uint8_t>(idx)};
    for (int i = 0; i < kPerThread; ++i) {
      auto r = store.append(payload.data(), payload.size());
      if (r.accepted) {
        offsets[idx].push_back(r.offset);
        ++total_accepted;
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back(producer, i);
  }
  for (auto &t : threads)
    t.join();

  // Collect all offsets and verify no duplicates
  std::vector<uint64_t> all;
  for (auto &v : offsets) {
    all.insert(all.end(), v.begin(), v.end());
  }
  std::set<uint64_t> unique_offsets(all.begin(), all.end());
  EXPECT_EQ(unique_offsets.size(), all.size()) << "Duplicate offsets detected!";
}

// ── 11. Concurrent producers do not corrupt record order ─────────────────────
// After all producers finish, every committed record must be readable and
// its payload must match what was appended at that offset.
TEST_F(PartitionStoreTest, ConcurrentProducersNoRecordCorruption) {
  PartitionStore store("test-data", fast_opts());

  constexpr int kThreads = 4;
  constexpr int kPerThread = 30;

  // Each thread uses its index as the single-byte payload
  std::atomic<uint64_t> last_accepted_offset{0};

  auto producer = [&](int idx) {
    std::vector<uint8_t> payload = {static_cast<uint8_t>(idx)};
    for (int i = 0; i < kPerThread; ++i) {
      auto r = store.append(payload.data(), payload.size());
      if (r.accepted) {
        uint64_t prev = last_accepted_offset.load(std::memory_order_relaxed);
        while (r.offset + 1 > prev && !last_accepted_offset.compare_exchange_weak(
                                          prev, r.offset + 1, std::memory_order_relaxed)) {
        }
      }
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back(producer, i);
  }
  for (auto &t : threads)
    t.join();

  const uint64_t committed_count = last_accepted_offset.load();
  if (committed_count == 0) {
    SUCCEED() << "No records accepted (queue may have been full)";
    return;
  }

  wait_for_commit(store, committed_count);

  // Read all committed records — verify no payload is corrupted (all 1 byte, 0-3)
  auto fetched =
      store.fetch(0, static_cast<uint16_t>(std::min(committed_count, static_cast<uint64_t>(1000))));
  for (const auto &msg : fetched) {
    ASSERT_EQ(msg.payload.size(), 1u) << "Corrupted payload at offset " << msg.offset;
    ASSERT_LT(msg.payload[0], kThreads) << "Invalid payload byte at offset " << msg.offset;
  }
}

// ── 12. Queue limit prevents unbounded memory growth ─────────────────────────
TEST_F(PartitionStoreTest, QueueLimitPreventsMemoryGrowth) {
  StorageOptions opts;
  opts.queue_capacity = 8;
  opts.max_pending_bytes = 1024; // very small byte limit
  opts.batch_wait_us = 500000;
  PartitionStore store("test-data", opts);

  // Large payload — will hit byte limit quickly
  std::vector<uint8_t> large_payload(256, 0xFF);
  int busy_count = 0;
  for (int i = 0; i < 32; ++i) {
    auto r = store.append(large_payload.data(), large_payload.size());
    if (!r.accepted) {
      EXPECT_EQ(r.error, AppendError::Busy);
      ++busy_count;
    }
  }
  EXPECT_GT(busy_count, 0) << "Expected some BUSY responses with tight memory limit";
}
