#include <benchmark/benchmark.h>
#include "storage/segment_writer.hpp"
#include "storage/segment_reader.hpp"
#include "storage/offset_index.hpp"
#include <filesystem>
#include <thread>
#include <vector>

using namespace fell::storage;

// Helper to avoid ERROR_SHARING_VIOLATION on Windows due to delayed file closure
static void safe_remove_all(const std::filesystem::path& path) {
  if (!std::filesystem::exists(path)) return;
  for (int i = 0; i < 10; ++i) {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (!ec) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::filesystem::remove_all(path); // throw on final failure
}

// ── SegmentWriter ─────────────────────────────────────────────────────────────
class StorageFixture : public benchmark::Fixture {
public:
  void SetUp(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage");
      std::filesystem::create_directories("bench-storage");
    }
  }

  void TearDown(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage");
    }
  }
};

BENCHMARK_DEFINE_F(StorageFixture, SegmentWriter_Append)(benchmark::State& state) {
  uint32_t payload_size = static_cast<uint32_t>(state.range(0));
  std::vector<uint8_t> payload(payload_size, 0xFF);
  
  SegmentWriter writer("bench-storage", 0, nullptr, 1000);
  uint64_t offset = 0;
  
  for (auto _ : state) {
    writer.append(offset++, payload.data(), payload.size());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * payload_size);
}
BENCHMARK_REGISTER_F(StorageFixture, SegmentWriter_Append)->Arg(64)->Arg(1024)->Arg(8192);

// ── SegmentReader ─────────────────────────────────────────────────────────────
class ReaderFixture : public benchmark::Fixture {
public:
  void SetUp(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage-read");
      std::filesystem::create_directories("bench-storage-read");
      
      SegmentWriter writer("bench-storage-read", 0, nullptr, 1000);
      std::vector<uint8_t> payload(1024, 0xEE);
      for (uint64_t i = 0; i < 10000; ++i) { // 10k items
        writer.append(i, payload.data(), payload.size());
      }
    }
  }

  void TearDown(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage-read");
    }
  }
};

BENCHMARK_DEFINE_F(ReaderFixture, SegmentReader_Scan)(benchmark::State& state) {
  auto log_path = std::filesystem::path("bench-storage-read") / "00000000000000000000.log";
  
  for (auto _ : state) {
    SegmentReader reader(log_path);
    auto records = reader.read(0, 0, 10000); // read all
    benchmark::DoNotOptimize(records);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 10000 * 1024);
}
BENCHMARK_REGISTER_F(ReaderFixture, SegmentReader_Scan)->Unit(benchmark::kMillisecond);

// ── OffsetIndex ───────────────────────────────────────────────────────────────
class IndexFixture : public benchmark::Fixture {
public:
  OffsetIndex* index = nullptr;

  void SetUp(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage-index");
      std::filesystem::create_directories("bench-storage-index");
      
      // Use Writer to generate a real index file
      {
        SegmentWriter writer("bench-storage-index", 0, nullptr, 1); // sync_every=1 to build index fast
        std::vector<uint8_t> payload(128, 0xAA);
        for (uint64_t i = 0; i < 100000; ++i) {
          writer.append(i, payload.data(), payload.size());
        }
      }
      
      auto idx_path = std::filesystem::path("bench-storage-index") / "00000000000000000000.idx";
      index = new OffsetIndex(idx_path);
    }
  }

  void TearDown(const ::benchmark::State& state) override {
    if (state.thread_index() == 0) {
      delete index;
      safe_remove_all("bench-storage-index");
    }
  }
};

BENCHMARK_DEFINE_F(IndexFixture, OffsetIndex_Lookup)(benchmark::State& state) {
  for (auto _ : state) {
    uint32_t pos = index->lookup(50000); // Lookup middle index
    benchmark::DoNotOptimize(pos);
  }
}
BENCHMARK_REGISTER_F(IndexFixture, OffsetIndex_Lookup);

BENCHMARK_MAIN();
