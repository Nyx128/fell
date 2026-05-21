#include <benchmark/benchmark.h>
#include <cstdint>

static inline uint16_t swap_be16(uint16_t val) {
  return (val >> 8) | (val << 8);
}

static inline uint32_t swap_be32(uint32_t val) {
  return ((val >> 24) & 0x000000FF) | ((val >> 8) & 0x0000FF00) | ((val << 8) & 0x00FF0000) |
         ((val << 24) & 0xFF000000);
}

static inline uint64_t swap_be64(uint64_t val) {
  return ((val >> 56) & 0x00000000000000FFULL) | ((val >> 40) & 0x000000000000FF00ULL) |
         ((val >> 24) & 0x0000000000FF0000ULL) | ((val >> 8) & 0x00000000FF000000ULL) |
         ((val << 8) & 0x000000FF00000000ULL) | ((val << 24) & 0x0000FF0000000000ULL) |
         ((val << 40) & 0x00FF000000000000ULL) | ((val << 56) & 0xFF00000000000000ULL);
}

static void BM_Swap16(benchmark::State &state) {
  uint16_t val = 0x1234;
  for (auto _ : state) {
    benchmark::DoNotOptimize(swap_be16(val));
    val++;
  }
}
BENCHMARK(BM_Swap16);

static void BM_Swap32(benchmark::State &state) {
  uint32_t val = 0x12345678;
  for (auto _ : state) {
    benchmark::DoNotOptimize(swap_be32(val));
    val++;
  }
}
BENCHMARK(BM_Swap32);

static void BM_Swap64(benchmark::State &state) {
  uint64_t val = 0x123456789ABCDEF0ULL;
  for (auto _ : state) {
    benchmark::DoNotOptimize(swap_be64(val));
    val++;
  }
}
BENCHMARK(BM_Swap64);

BENCHMARK_MAIN();
