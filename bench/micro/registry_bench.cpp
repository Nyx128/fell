#include "broker/topic_registry.hpp"
#include <benchmark/benchmark.h>
#include <thread>
#include <vector>

static void BM_Registry_SingleThreadAppend(benchmark::State &state) {
  fell::TopicRegistry registry;
  registry.create_topic("perf", 1);
  fell::Partition *partition = registry.get_partition("perf", 0);
  std::vector<uint8_t> payload(state.range(0), 0xCC);

  for (auto _ : state) {
    uint64_t offset = partition->append(payload);
    benchmark::DoNotOptimize(offset);
  }
  state.SetBytesProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_Registry_SingleThreadAppend)->Arg(64)->Arg(1024)->Arg(8192);

static void BM_Registry_SingleThreadFetch(benchmark::State &state) {
  fell::TopicRegistry registry;
  registry.create_topic("perf", 1);
  fell::Partition *partition = registry.get_partition("perf", 0);
  std::vector<uint8_t> payload(128, 0xDD);
  for (int i = 0; i < 10000; ++i) {
    partition->append(payload);
  }

  for (auto _ : state) {
    auto results = partition->fetch(0, 100);
    benchmark::DoNotOptimize(results);
  }
}
BENCHMARK(BM_Registry_SingleThreadFetch);

static void BM_Registry_ConcurrentAppends(benchmark::State &state) {
  fell::TopicRegistry registry;
  registry.create_topic("perf", 1);
  fell::Partition *partition = registry.get_partition("perf", 0);
  std::vector<uint8_t> payload(128, 0xEE);

  for (auto _ : state) {
    std::vector<std::thread> threads;
    // Spawn threads performing concurrent appends to stress Partition::mu_ mutex contention
    for (int i = 0; i < state.range(0); ++i) {
      threads.emplace_back([&]() {
        for (int k = 0; k < 100; ++k) {
          partition->append(payload);
        }
      });
    }
    for (auto &t : threads) {
      t.join();
    }
  }
}
// Run with 2, 4, and 8 concurrent writing threads
BENCHMARK(BM_Registry_ConcurrentAppends)->Arg(2)->Arg(4)->Arg(8);

BENCHMARK_MAIN();
