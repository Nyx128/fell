#include <gtest/gtest.h>
#include "broker/frame_decoder.hpp"
#include "broker/protocol.hpp"

using namespace fell;

TEST(FrameDecoderTest, ParsesCompleteFrame) {
  FrameDecoder decoder;
  std::vector<Frame> out;

  // 1. Parse a complete valid frame
  // Length: 2 (1-byte Op + 1-byte Payload) -> big-endian: {0x00, 0x00, 0x00, 0x02}
  // Opcode: 0x01, Payload: {0xAA}
  uint8_t complete_frame[] = {0x00, 0x00, 0x00, 0x02, 0x01, 0xAA};
  int count = decoder.push(complete_frame, sizeof(complete_frame), out);
  
  EXPECT_EQ(count, 1);
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].op, static_cast<Op>(0x01));
  ASSERT_EQ(out[0].payload.size(), 1);
  EXPECT_EQ(out[0].payload[0], 0xAA);
}

TEST(FrameDecoderTest, ParsesPartialFrames) {
  FrameDecoder decoder;
  std::vector<Frame> out;

  uint8_t chunk1[] = {0x00, 0x00};
  EXPECT_EQ(decoder.push(chunk1, sizeof(chunk1), out), 0); // Not enough for size

  uint8_t chunk2[] = {0x00, 0x03, 0x02};
  EXPECT_EQ(decoder.push(chunk2, sizeof(chunk2), out), 0); // size read as 3, but only 1 byte of payload present

  uint8_t chunk3[] = {0xBB, 0xCC};
  EXPECT_EQ(decoder.push(chunk3, sizeof(chunk3), out), 1); // Completed!
  
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out[0].op, static_cast<Op>(0x02));
  ASSERT_EQ(out[0].payload.size(), 2);
  EXPECT_EQ(out[0].payload[0], 0xBB);
  EXPECT_EQ(out[0].payload[1], 0xCC);
}

TEST(FrameDecoderTest, ParsesMultipleFramesInSingleBuffer) {
  FrameDecoder decoder;
  std::vector<Frame> out;

  // Frame A: Len = 1, Op = 0x03
  // Frame B: Len = 2, Op = 0x04, Payload = {0xDD}
  uint8_t multi_frame[] = {0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0x04, 0xDD};
  EXPECT_EQ(decoder.push(multi_frame, sizeof(multi_frame), out), 2);
  
  ASSERT_EQ(out.size(), 2);
  EXPECT_EQ(out[0].op, static_cast<Op>(0x03));
  EXPECT_TRUE(out[0].payload.empty());
  
  EXPECT_EQ(out[1].op, static_cast<Op>(0x04));
  ASSERT_EQ(out[1].payload.size(), 1);
  EXPECT_EQ(out[1].payload[0], 0xDD);
}

TEST(FrameDecoderTest, RejectsFrameExceedingCeilingGuard) {
  // Use a smaller max frame size limit for testing
  FrameDecoder decoder(128);
  std::vector<Frame> out;

  // Send a chunk of 135 bytes (larger than 128 + 4096 is not needed since our buffer ceiling limit check is:
  // buf_.size() - read_idx_ + len > max_frame_size_ + 4096
  // With max_frame_size_ = 128, the limit is 128 + 4096 = 4224 bytes.
  // Let's push data larger than 4224 bytes.
  std::vector<uint8_t> huge_chunk(4500, 0xAA);
  int status = decoder.push(huge_chunk.data(), huge_chunk.size(), out);
  EXPECT_EQ(status, -1);
}

