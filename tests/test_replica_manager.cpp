// tests/test_replica_manager.cpp
// Phase 4 §13.2 — Unit tests for ReplicaManager leader-side logic.

#define NOMINMAX
#include "platform/endian.hpp"
#include "platform/socket.hpp"
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include "replication/repl_protocol.hpp"
#include "replication/replica_manager.hpp"
#include <chrono>
#include <cstring>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace fell::repl;
using namespace fell::storage;

namespace {

  void init_meta(PartitionMeta &m, uint32_t num_followers) {
    m.topic = "orders";
    m.partition_idx = 0;
    m.leader_id = 0;
    m.role = PartitionRole::Leader;
    m.epoch = 1;
    m.replicas.clear();

    ReplicaState ls;
    ls.broker_id = 0;
    ls.in_isr = true;
    m.replicas.push_back(ls);

    for (uint32_t i = 1; i <= num_followers; ++i) {
      ReplicaState rs;
      rs.broker_id = i;
      rs.in_isr = true;
      m.replicas.push_back(rs);
    }
  }

  ClusterConfig make_cfg(int acks = -1) {
    ClusterConfig cfg;
    cfg.broker_id = 0;
    cfg.acks = acks;
    cfg.replication_factor = 3;
    cfg.heartbeat_interval_ms = 500;
    cfg.heartbeat_timeout_ms = 1500;
    cfg.max_lag_messages = 1000;
    return cfg;
  }

  std::vector<CommittedRecord> make_recs(uint64_t base, uint32_t count) {
    std::vector<CommittedRecord> v;
    for (uint32_t i = 0; i < count; ++i) {
      CommittedRecord r;
      r.offset = base + i;
      r.timestamp_ms = 0;
      r.payload = {static_cast<uint8_t>(i)};
      v.push_back(std::move(r));
    }
    return v;
  }

} // namespace

// ── Test 1: acks=all full flow ───────────────────────────────────────────────
TEST(ReplicaManager, AcksAllFullFlow) {
  PartitionMeta meta;
  init_meta(meta, 1);
  auto cfg = make_cfg(-1);
  ReplicaManager rm(meta, cfg);

  std::vector<std::pair<int, std::vector<uint8_t>>> responses;
  rm.set_post_response_cb([&responses](int fd, std::vector<uint8_t> resp) {
    responses.push_back({fd, std::move(resp)});
  });
  rm.start();

  rm.register_pending(0, {0xAA}, 10);

  // Step 1: leader commits — follower hasn't acked yet
  rm.enqueue_committed(0, make_recs(0, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  EXPECT_TRUE(responses.empty()) << "Must NOT release before ISR ack";

  // Step 2: follower acks
  auto to_send = rm.on_replica_ack(1, 1);
  rm.stop();

  ASSERT_FALSE(to_send.empty()) << "Must release after ISR ack";
  EXPECT_EQ(to_send[0].first, 10);
}

// ── Test 2: acks=1 — no register_pending, no responses enqueued ──────────────
TEST(ReplicaManager, AcksOneNoPending) {
  PartitionMeta meta;
  init_meta(meta, 1);
  auto cfg = make_cfg(1);
  ReplicaManager rm(meta, cfg);

  std::vector<std::pair<int, std::vector<uint8_t>>> responses;
  rm.set_post_response_cb([&responses](int fd, std::vector<uint8_t> resp) {
    responses.push_back({fd, std::move(resp)});
  });
  rm.start();

  // Commits fire (replication still happens) but no pending registered
  rm.enqueue_committed(0, make_recs(0, 5));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  EXPECT_TRUE(responses.empty()) << "No pending: no responses should be enqueued";
}

// ── Test 3: Follower timeout removes from ISR — isr_required degrades ────────
TEST(ReplicaManager, FollowerTimeoutReducesIsrRequired) {
  PartitionMeta meta;
  init_meta(meta, 1);
  auto cfg = make_cfg(-1);
  ReplicaManager rm(meta, cfg);

  // Simulate follower timeout: all last_seen_ms == 0, now_ms >> timeout
  meta.tick_isr(2000, 1500);
  // Follower 1 is now out of ISR; only leader remains

  std::vector<std::pair<int, std::vector<uint8_t>>> responses;
  rm.set_post_response_cb([&responses](int fd, std::vector<uint8_t> resp) {
    responses.push_back({fd, std::move(resp)});
  });
  rm.start();

  // Register after timeout — isr_required should now be 0 (no followers in ISR)
  rm.register_pending(3, {0xBB}, 20);

  rm.enqueue_committed(3, make_recs(3, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  // isr_required == 0 → release on leader commit alone
  EXPECT_FALSE(responses.empty()) << "ISR degraded to leader only: release on leader commit";
}

// ── Test 4: replicate_batch sends correct wire frame ─────────────────────────
// Creates a local notify-pair socket to act as the follower fd, enqueues a
// commit, and verifies the worker thread sends a frame with opcode REPLICA_SYNC.
TEST(ReplicaManager, ReplicateBatchSendsCorrectFrames) {
  fell::platform::platform_net_init();

  socket_t read_fd = INVALID_SOCKET_T;
  socket_t write_fd = INVALID_SOCKET_T;
  fell::platform::create_notify_pair(&read_fd, &write_fd);
  ASSERT_NE(read_fd, INVALID_SOCKET_T);

  PartitionMeta meta;
  init_meta(meta, 1);
  auto cfg = make_cfg(-1);
  ReplicaManager rm(meta, cfg);
  rm.set_follower_fd(1, static_cast<int>(write_fd));
  rm.start();

  auto recs = make_recs(0, 1);
  recs[0].payload = {0x01, 0x02, 0x03};
  rm.enqueue_committed(0, recs);
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  // Read what was written to write_fd (which appeared on read_fd)
  uint8_t buf[1024];
  fell::platform::set_nonblocking(read_fd);
  int n = fell::platform::recv_data(read_fd, buf, sizeof(buf));
  fell::platform::close_socket(read_fd);
  fell::platform::close_socket(write_fd);

  ASSERT_GT(n, 5) << "Expected at least a 5-byte length-prefixed frame";

  // Byte 4: opcode = REPLICA_SYNC (0x02)
  EXPECT_EQ(buf[4], static_cast<uint8_t>(fell::repl::ReplOp::REPLICA_SYNC))
      << "Expected REPLICA_SYNC opcode";

  // Frame length (big-endian uint32 at buf[0..3]) should be > 0
  uint32_t frame_len_be;
  std::memcpy(&frame_len_be, buf, 4);
  uint32_t frame_len = fell::platform::be32_to_host(frame_len_be);
  EXPECT_GT(frame_len, 0u);
}

TEST(ReplicaManager, DuplicateAckFromSameFollowerDoesNotSatisfyQuorum) {
  PartitionMeta meta;
  init_meta(meta, 2);
  auto cfg = make_cfg(-1);
  ReplicaManager rm(meta, cfg);

  std::vector<std::pair<int, std::vector<uint8_t>>> responses;
  rm.set_post_response_cb([&responses](int fd, std::vector<uint8_t> resp) {
    responses.push_back({fd, std::move(resp)});
  });

  rm.register_pending(4, {0xAB}, 40);
  auto first = rm.on_replica_ack(1, 5);
  auto duplicate = rm.on_replica_ack(1, 5);

  EXPECT_TRUE(first.empty());
  EXPECT_TRUE(duplicate.empty());

  auto recs = make_recs(4, 1);
  rm.start();
  rm.enqueue_committed(4, recs);
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  EXPECT_TRUE(responses.empty()) << "Duplicate acks from one follower must not satisfy quorum";

  auto distinct = rm.on_replica_ack(2, 5);
  rm.stop();

  ASSERT_EQ(distinct.size(), 1u);
  EXPECT_EQ(distinct[0].first, 40);
}

TEST(ReplicaManager, DistinctFollowersReleaseExactlyOnce) {
  PartitionMeta meta;
  init_meta(meta, 2);
  auto cfg = make_cfg(-1);
  ReplicaManager rm(meta, cfg);

  rm.register_pending(9, {0xCD}, 55);
  rm.start();
  rm.enqueue_committed(9, make_recs(9, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));

  auto first = rm.on_replica_ack(1, 10);
  auto second = rm.on_replica_ack(2, 10);
  auto duplicate = rm.on_replica_ack(2, 10);
  rm.stop();

  EXPECT_TRUE(first.empty());
  ASSERT_EQ(second.size(), 1u);
  EXPECT_EQ(second[0].first, 55);
  EXPECT_TRUE(duplicate.empty());
}
