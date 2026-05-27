#include "broker/topic_registry.hpp"
#include <benchmark/benchmark.h>
#include <atomic>
#include <filesystem>
#include <mutex>
#include <thread>
#include <vector>

// Helper to avoid ERROR_SHARING_VIOLATION on Windows due to delayed file closure
static void safe_remove_all(const std::filesystem::path &path) {
  if (!std::filesystem::exists(path))
    return;
  for (int i = 0; i < 10; ++i) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (!ec)
      return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::filesystem::remove_all(path); // throw on final failure
}

// ── Single-thread append ──────────────────────────────────────────────────────
static void BM_Registry_SingleThreadAppend(benchmark::State &state) {
  safe_remove_all("bench-data");
  {
    fell::TopicRegistry registry("bench-data");
    registry.create_topic("perf", 1);
    fell::Partition *partition = registry.get_partition("perf", 0);

    const std::vector<uint8_t> payload(static_cast<size_t>(state.range(0)), 0xCC);

    for (auto _ : state) {
      uint64_t offset = partition->append(payload.data(), static_cast<uint32_t>(payload.size()));
      benchmark::DoNotOptimize(offset);
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
  }
  // Cleanup after run to avoid disk filling up. Handles are closed now.
  safe_remove_all("bench-data");
}
BENCHMARK(BM_Registry_SingleThreadAppend)->Arg(64)->Arg(256)->Arg(1024)->Arg(8192);

// ── Single-thread fetch ───────────────────────────────────────────────────────
static void BM_Registry_SingleThreadFetch(benchmark::State &state) {
  safe_remove_all("bench-data");
  {
    fell::TopicRegistry registry("bench-data");
    registry.create_topic("perf", 1);
    fell::Partition *partition = registry.get_partition("perf", 0);

    const std::vector<uint8_t> payload(128, 0xDD);
    for (int i = 0; i < 10000; ++i)
      partition->append(payload.data(), static_cast<uint32_t>(payload.size()));

    const auto batch = static_cast<uint16_t>(state.range(0));

    for (auto _ : state) {
      auto results = partition->fetch(0, batch);
      benchmark::DoNotOptimize(results);
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * state.range(0));
  }
  safe_remove_all("bench-data");
}
BENCHMARK(BM_Registry_SingleThreadFetch)->Arg(10)->Arg(50)->Arg(100);

// TODO: benchmark fails on thread count 4 and up
/*
* void lock() {
        here->>if (_Mtx_lock(_Mymtx()) != _Thrd_result::_Success) {
            // undefined behavior, only occurs for plain mutexes (N4950
[thread.mutex.requirements.mutex.general]/6) _STD _Throw_Cpp_error(_RESOURCE_DEADLOCK_WOULD_OCCUR);
        }

        if (!_Verify_ownership_levels()) {
            // only occurs for recursive mutexes (N4950 [thread.mutex.recursive]/3)
            // POSIX specifies EAGAIN in the corresponding situation:
            // https://pubs.opengroup.org/onlinepubs/9699919799/functions/pthread_mutex_lock.html
            _STD _Throw_Cpp_error(_RESOURCE_UNAVAILABLE_TRY_AGAIN);
        }
    }
*/
// ── Concurrent appends ────────────────────────────────────────────────────────
class RegistryFixture : public benchmark::Fixture {
public:
  static fell::TopicRegistry *registry;
  // Barrier: non-zero threads spin on this until thread 0 finishes SetUp.
  static std::atomic<bool> ready;

  void SetUp(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-data-concurrent");
      registry = new fell::TopicRegistry("bench-data-concurrent");
      registry->create_topic("perf", 1);
      // Release-store: all writes above are visible to threads that
      // acquire-load true below.
      ready.store(true, std::memory_order_release);
    } else {
      // Spin until thread 0 has finished initialisation.
      while (!ready.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
    }
  }

  void TearDown(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      delete registry;
      registry = nullptr;
      safe_remove_all("bench-data-concurrent");
      // Reset barrier AFTER cleanup so the next run's SetUp starts
      // with ready==false regardless of thread ordering.
      ready.store(false, std::memory_order_release);
    }
    // Non-zero threads: nothing to do. The benchmark framework waits
    // for ALL TearDowns to complete before firing the next SetUp,
    // so thread 0's reset is guaranteed visible before the next cycle.
  }
};
fell::TopicRegistry* RegistryFixture::registry = nullptr;
std::atomic<bool> RegistryFixture::ready{false};

BENCHMARK_DEFINE_F(RegistryFixture, ConcurrentAppends)(benchmark::State& state) {
  fell::Partition *partition = registry->get_partition("perf", 0);
  const std::vector<uint8_t> payload(128, 0xEE);

  for (auto _ : state) {
    uint64_t off = partition->append(payload.data(), static_cast<uint32_t>(payload.size()));
    benchmark::DoNotOptimize(off);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 128);
}
BENCHMARK_REGISTER_F(RegistryFixture, ConcurrentAppends)->Threads(1)->Threads(2)->Threads(4)->Threads(8);


// ── Concurrent appends — multiple partitions ──────────────────────────────────
class RegistryMultiFixture : public benchmark::Fixture {
public:
  static fell::TopicRegistry *registry;
  static std::atomic<bool> ready;
  static constexpr int kPartitions = 8;

  void SetUp(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-data-multi");
      registry = new fell::TopicRegistry("bench-data-multi");
      registry->create_topic("perf_multi", kPartitions);
      ready.store(true, std::memory_order_release);
    } else {
      while (!ready.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
    }
  }

  void TearDown(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      delete registry;
      registry = nullptr;
      safe_remove_all("bench-data-multi");
      ready.store(false, std::memory_order_release);
    }
  }
};
fell::TopicRegistry* RegistryMultiFixture::registry = nullptr;
std::atomic<bool> RegistryMultiFixture::ready{false};

BENCHMARK_DEFINE_F(RegistryMultiFixture, ConcurrentAppendsMultiPartition)(benchmark::State& state) {
  const std::vector<uint8_t> payload(128, 0xFF);
  const auto partition_idx = static_cast<uint16_t>(state.thread_index() % RegistryMultiFixture::kPartitions);
  fell::Partition *partition = registry->get_partition("perf_multi", partition_idx);

  for (auto _ : state) {
    uint64_t off = partition->append(payload.data(), static_cast<uint32_t>(payload.size()));
    benchmark::DoNotOptimize(off);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 128);
}
BENCHMARK_REGISTER_F(RegistryMultiFixture, ConcurrentAppendsMultiPartition)
    ->Threads(1)->Threads(2)->Threads(4)->Threads(8);

BENCHMARK_MAIN();