#include "broker/topic_registry.hpp"
#include <benchmark/benchmark.h>
#include <mutex>
#include <vector>

// ── Single-thread append ──────────────────────────────────────────────────────
// Measures raw Partition::append() throughput with no contention.
// Parameterised over payload size to separate fixed overhead (mutex, deque
// bookkeeping) from variable cost (payload copy).

static void BM_Registry_SingleThreadAppend(benchmark::State &state) {
  fell::TopicRegistry registry;
  registry.create_topic("perf", 1);
  fell::Partition *partition = registry.get_partition("perf", 0);

  const std::vector<uint8_t> payload(static_cast<size_t>(state.range(0)), 0xCC);

  for (auto _ : state) {
    uint64_t offset = partition->append(payload);
    benchmark::DoNotOptimize(offset);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
}
BENCHMARK(BM_Registry_SingleThreadAppend)->Arg(64)->Arg(256)->Arg(1024)->Arg(8192);

// ── Single-thread fetch ───────────────────────────────────────────────────────
// Pre-populates 10 000 messages then measures the cost of fetching 100 at a
// time from offset 0. Dominated by vector-of-vector allocation and copy.
// Parameterised over fetch batch size.

static void BM_Registry_SingleThreadFetch(benchmark::State &state) {
  fell::TopicRegistry registry;
  registry.create_topic("perf", 1);
  fell::Partition *partition = registry.get_partition("perf", 0);

  const std::vector<uint8_t> payload(128, 0xDD);
  for (int i = 0; i < 10000; ++i)
    partition->append(payload);

  const auto batch = static_cast<uint16_t>(state.range(0));

  for (auto _ : state) {
    auto results = partition->fetch(0, batch);
    benchmark::DoNotOptimize(results);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
}
BENCHMARK(BM_Registry_SingleThreadFetch)->Arg(10)->Arg(50)->Arg(100);

// ── Concurrent appends ────────────────────────────────────────────────────────
// Measures Partition::append() throughput under true mutex contention.
//
// Uses Google Benchmark's native Threads() API so the framework pre-spawns all
// threads before the timed region. std::call_once ensures one-time registry
// setup that is:
//   (a) race-free — all threads block on call_once until setup completes
//   (b) shared across Threads(1)/Threads(2)/Threads(4)/Threads(8) variants
//       (the once_flag stays set, objects are reused — intentional, we are
//       measuring contention not partition state)
//
// Raw pointers are intentionally leaked: Google Benchmark exits immediately
// after the last benchmark completes, making destructor ordering irrelevant.

static void BM_Registry_ConcurrentAppends(benchmark::State &state) {
  static std::once_flag init_flag;
  static fell::TopicRegistry *registry = nullptr;
  static fell::Partition *partition = nullptr;

  std::call_once(init_flag, []() {
    registry = new fell::TopicRegistry();
    registry->create_topic("perf", 1);
    partition = registry->get_partition("perf", 0);
  });

  const std::vector<uint8_t> payload(128, 0xEE);

  for (auto _ : state) {
    uint64_t off = partition->append(payload);
    benchmark::DoNotOptimize(off);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 128);
}
BENCHMARK(BM_Registry_ConcurrentAppends)->Threads(1)->Threads(2)->Threads(4)->Threads(8);

// ── Concurrent appends — multiple partitions ──────────────────────────────────
// Each thread writes to its own partition (thread_index() % num_partitions).
// This is the Phase 3 model: per-partition locking means threads on different
// partitions should not contend at all.
// Compare throughput against BM_Registry_ConcurrentAppends (single partition)
// to quantify the contention cost of the single-partition case.

static void BM_Registry_ConcurrentAppendsMultiPartition(benchmark::State &state) {
  static std::once_flag init_flag;
  static fell::TopicRegistry *registry = nullptr;
  static constexpr int kPartitions = 8;

  std::call_once(init_flag, []() {
    registry = new fell::TopicRegistry();
    registry->create_topic("perf_multi", kPartitions);
  });

  const std::vector<uint8_t> payload(128, 0xFF);
  // Each thread pins to its own partition — zero cross-thread contention.
  const auto partition_idx = static_cast<uint16_t>(state.thread_index() % kPartitions);
  fell::Partition *partition = registry->get_partition("perf_multi", partition_idx);

  for (auto _ : state) {
    uint64_t off = partition->append(payload);
    benchmark::DoNotOptimize(off);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 128);
}
BENCHMARK(BM_Registry_ConcurrentAppendsMultiPartition)
    ->Threads(1)
    ->Threads(2)
    ->Threads(4)
    ->Threads(8);

BENCHMARK_MAIN();