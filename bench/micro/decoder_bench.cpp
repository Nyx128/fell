#include "broker/frame_decoder.hpp"
#include <benchmark/benchmark.h>
#include <vector>

static void BM_Decoder_SingleFrame(benchmark::State &state) {
  // Length: 2, Opcode: 0x01, Payload: {0xAA}
  // Big-endian length: {0x00, 0x00, 0x00, 0x02}
  const uint8_t complete_frame[] = {0x00, 0x00, 0x00, 0x02, 0x01, 0xAA};

  fell::FrameDecoder decoder;
  std::vector<fell::Frame> out;
  out.reserve(1);

  for (auto _ : state) {
    int count = decoder.push(complete_frame, sizeof(complete_frame), out);
    benchmark::DoNotOptimize(count);
    benchmark::DoNotOptimize(out);
    out.clear();
  }
}
BENCHMARK(BM_Decoder_SingleFrame);

static void BM_Decoder_BatchFrames(benchmark::State &state) {
  // Generate 100 small valid frames concatenated in one buffer
  std::vector<uint8_t> buffer;
  for (int i = 0; i < 100; ++i) {
    // Len: 2, Op: 0x02, Payload: {0xBB}
    uint8_t frame[] = {0x00, 0x00, 0x00, 0x02, 0x02, 0xBB};
    buffer.insert(buffer.end(), frame, frame + sizeof(frame));
  }

  fell::FrameDecoder decoder;
  std::vector<fell::Frame> out;
  out.reserve(100);

  for (auto _ : state) {
    int count = decoder.push(buffer.data(), buffer.size(), out);
    benchmark::DoNotOptimize(count);
    benchmark::DoNotOptimize(out);
    out.clear();
  }
}
BENCHMARK(BM_Decoder_BatchFrames);

static void BM_Decoder_Fragmented(benchmark::State &state) {
  // Feed 1 byte at a time to test decoder buffer accumulation & resizing
  const uint8_t complete_frame[] = {0x00, 0x00, 0x00, 0x02, 0x01, 0xAA};

  fell::FrameDecoder decoder;
  std::vector<fell::Frame> out;
  out.reserve(1);

  for (auto _ : state) {
    for (size_t i = 0; i < sizeof(complete_frame); ++i) {
      decoder.push(&complete_frame[i], 1, out);
    }
    benchmark::DoNotOptimize(out);
    out.clear();
  }
}
BENCHMARK(BM_Decoder_Fragmented);

BENCHMARK_MAIN();
