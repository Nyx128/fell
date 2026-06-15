#include "platform/endian.hpp"
#include "platform/file.hpp"
#include "storage/log_format.hpp"
#include "storage/offset_index.hpp"
#include "storage/partition_store.hpp"
#include "storage/segment_reader.hpp"
#include <array>
#include <benchmark/benchmark.h>
#include <filesystem>
#include <thread>
#include <vector>

using namespace fell::storage;

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
  std::filesystem::remove_all(path);
}

// Write a record directly to disk (no I/O thread) — used only for fixture setup.
static void write_record_raw(const std::filesystem::path &dir, uint64_t offset,
                             uint64_t timestamp_ms, const std::vector<uint8_t> &payload) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%020llu", 0ULL);
  auto log_path = dir / (std::string(buf) + ".log");
  auto idx_path = dir / (std::string(buf) + ".idx");

  file_t log_fd = fell::platform::open_file_append(log_path);
  file_t idx_fd = fell::platform::open_file_append(idx_path);

  uint64_t file_pos = std::filesystem::exists(log_path) ? std::filesystem::file_size(log_path) : 0;

  LogRecordHeader header{fell::platform::host_to_be64(offset),
                         fell::platform::host_to_be64(timestamp_ms),
                         fell::platform::host_to_be32(static_cast<uint32_t>(payload.size()))};
  fell::platform::IOBuffer log_buf{&header, sizeof(header)};
  fell::platform::IOBuffer pay_buf{payload.data(), payload.size()};
  std::array<fell::platform::IOBuffer, 2> bufs = {log_buf, pay_buf};
  fell::platform::write_file_vec(log_fd, bufs.data(), bufs.size());

  IndexEntry entry{fell::platform::host_to_be64(offset), fell::platform::host_to_be64(file_pos)};
  fell::platform::IOBuffer idx_buf{&entry, sizeof(entry)};
  fell::platform::write_file_vec(idx_fd, &idx_buf, 1);

  fell::platform::close_file(log_fd);
  fell::platform::close_file(idx_fd);
}

// ── PartitionStore enqueue (the hot publish path) ──────────────────────────
// This measures the request-thread cost: payload copy + queue push + ACK.
class StorageFixture : public benchmark::Fixture {
public:
  PartitionStore *store = nullptr;

  void SetUp(const ::benchmark::State &state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage");
      std::filesystem::create_directories("bench-storage");
      StorageOptions opts;
      opts.batch_wait_us = 0;
      store = new PartitionStore("bench-storage", opts);
    }
  }

  void TearDown(const ::benchmark::State &state) override {
    if (state.thread_index() == 0) {
      delete store;
      store = nullptr;
      safe_remove_all("bench-storage");
    }
  }
};

BENCHMARK_DEFINE_F(StorageFixture, PartitionStore_Enqueue)(benchmark::State &state) {
  uint32_t payload_size = static_cast<uint32_t>(state.range(0));
  std::vector<uint8_t> payload(payload_size, 0xFF);

  for (auto _ : state) {
    auto result = store->append(payload.data(), payload.size());
    benchmark::DoNotOptimize(result);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * payload_size);
}
BENCHMARK_REGISTER_F(StorageFixture, PartitionStore_Enqueue)->Arg(64)->Arg(1024)->Arg(8192);

// ── SegmentReader ─────────────────────────────────────────────────────────────
class ReaderFixture : public benchmark::Fixture {
public:
  void SetUp(const ::benchmark::State &state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage-read");
      std::filesystem::create_directories("bench-storage-read");
      std::vector<uint8_t> payload(1024, 0xEE);
      for (uint64_t i = 0; i < 10000; ++i) {
        write_record_raw("bench-storage-read", i, 1000 + i, payload);
      }
    }
  }

  void TearDown(const ::benchmark::State &state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage-read");
    }
  }
};

BENCHMARK_DEFINE_F(ReaderFixture, SegmentReader_Scan)(benchmark::State &state) {
  auto log_path = std::filesystem::path("bench-storage-read") / "00000000000000000000.log";

  for (auto _ : state) {
    SegmentReader reader(log_path);
    auto records = reader.read(0, 0, 10000);
    benchmark::DoNotOptimize(records);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * 10000 * 1024);
}
BENCHMARK_REGISTER_F(ReaderFixture, SegmentReader_Scan)->Unit(benchmark::kMillisecond);

// ── OffsetIndex ───────────────────────────────────────────────────────────────
class IndexFixture : public benchmark::Fixture {
public:
  OffsetIndex *index = nullptr;

  void SetUp(const ::benchmark::State &state) override {
    if (state.thread_index() == 0) {
      safe_remove_all("bench-storage-index");
      std::filesystem::create_directories("bench-storage-index");
      std::vector<uint8_t> payload(128, 0xAA);
      for (uint64_t i = 0; i < 100000; ++i) {
        write_record_raw("bench-storage-index", i, 1000 + i, payload);
      }
      auto idx_path = std::filesystem::path("bench-storage-index") / "00000000000000000000.idx";
      index = new OffsetIndex(idx_path);
    }
  }

  void TearDown(const ::benchmark::State &state) override {
    if (state.thread_index() == 0) {
      delete index;
      safe_remove_all("bench-storage-index");
    }
  }
};

BENCHMARK_DEFINE_F(IndexFixture, OffsetIndex_Lookup)(benchmark::State &state) {
  for (auto _ : state) {
    uint32_t pos = index->lookup(50000);
    benchmark::DoNotOptimize(pos);
  }
}
BENCHMARK_REGISTER_F(IndexFixture, OffsetIndex_Lookup);

BENCHMARK_MAIN();
