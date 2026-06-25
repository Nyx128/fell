// tests/test_pending_ack.cpp
// Phase 4 §13.1 — Unit tests for PendingAck two-phase release logic.

#define NOMINMAX
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include "replication/replica_manager.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace fell::repl;
using namespace fell::storage;

namespace {

  // Populate a PartitionMeta in-place (can't copy due to mutex).
  void init_meta(PartitionMeta &m, uint32_t leader_id, uint32_t num_followers) {
    m.topic = "test";
    m.partition_idx = 0;
    m.leader_id = leader_id;
    m.role = PartitionRole::Leader;
    m.epoch = 1;
    m.replicas.clear();

    ReplicaState ls;
    ls.broker_id = leader_id;
    ls.in_isr = true;
    m.replicas.push_back(ls);

    for (uint32_t i = 1; i <= num_followers; ++i) {
      ReplicaState rs;
      rs.broker_id = i;
      rs.in_isr = true;
      m.replicas.push_back(rs);
    }
  }

  ClusterConfig make_cfg() {
    ClusterConfig cfg;
    cfg.broker_id = 0;
    cfg.acks = -1;
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
      v.push_back(std::move(r));
    }
    return v;
  }

} // namespace

// ── Test 1: Leader commits first, ISR acks second ───────────────────────────
TEST(PendingAck, LeaderFirstThenIsr) {
  PartitionMeta meta;
  init_meta(meta, 0, 1);
  auto cfg = make_cfg();
  ReplicaManager rm(meta, cfg);

  std::vector<std::pair<int, std::vector<uint8_t>>> released;
  rm.set_post_response_cb([&released](int fd, std::vector<uint8_t> resp) {
    released.push_back({fd, std::move(resp)});
  });
  rm.start();

  rm.register_pending(42, {0xAA}, 99);

  // Leader commits — follower has not acked yet
  rm.enqueue_committed(42, make_recs(42, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  EXPECT_TRUE(released.empty()) << "Must NOT release before ISR ack";

  // ISR ack arrives
  auto to_send = rm.on_replica_ack(1, 43);
  rm.stop();

  ASSERT_FALSE(to_send.empty()) << "Should release after ISR ack";
  EXPECT_EQ(to_send[0].first, 99);
  EXPECT_EQ(to_send[0].second, std::vector<uint8_t>{0xAA});
}

// ── Test 2: ISR acks first, leader commits second ───────────────────────────
TEST(PendingAck, IsrFirstThenLeader) {
  PartitionMeta meta;
  init_meta(meta, 0, 1);
  auto cfg = make_cfg();
  ReplicaManager rm(meta, cfg);

  rm.register_pending(10, {0xBB}, 77);

  // ISR ack arrives before leader commits
  auto early = rm.on_replica_ack(1, 11);
  EXPECT_TRUE(early.empty()) << "Must not release before leader commits";

  std::vector<std::pair<int, std::vector<uint8_t>>> released;
  rm.set_post_response_cb([&released](int fd, std::vector<uint8_t> resp) {
    released.push_back({fd, std::move(resp)});
  });
  rm.start();
  rm.enqueue_committed(10, make_recs(10, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  ASSERT_FALSE(released.empty()) << "Should release after leader commits";
  EXPECT_EQ(released[0].first, 77);
}

// ── Test 3: Both arrive simultaneously (released exactly once) ───────────────
TEST(PendingAck, SimultaneousNoDoubleRelease) {
  PartitionMeta meta;
  init_meta(meta, 0, 1);
  auto cfg = make_cfg();
  ReplicaManager rm(meta, cfg);

  rm.register_pending(5, {0xCC}, 55);

  // ISR ack first (no release yet)
  rm.on_replica_ack(1, 6);

  std::vector<std::pair<int, std::vector<uint8_t>>> released;
  rm.set_post_response_cb([&released](int fd, std::vector<uint8_t> resp) {
    released.push_back({fd, std::move(resp)});
  });
  rm.start();
  rm.enqueue_committed(5, make_recs(5, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  EXPECT_EQ(released.size(), 1u) << "Must release exactly once";
}

// ── Test 4: ISR size 0 (leader only) — release on leader commit alone ───────
TEST(PendingAck, LeaderOnlyNoFollowers) {
  PartitionMeta meta;
  init_meta(meta, 0, 0); // no followers
  auto cfg = make_cfg();
  ReplicaManager rm(meta, cfg);

  rm.register_pending(1, {0xDD}, 33);

  std::vector<std::pair<int, std::vector<uint8_t>>> released;
  rm.set_post_response_cb([&released](int fd, std::vector<uint8_t> resp) {
    released.push_back({fd, std::move(resp)});
  });
  rm.start();
  rm.enqueue_committed(1, make_recs(1, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  EXPECT_FALSE(released.empty()) << "Leader-only: release on leader commit alone";
}

// ── Test 5: Multiple pending offsets — each released independently ───────────
TEST(PendingAck, MultiplePendingOffsets) {
  PartitionMeta meta;
  init_meta(meta, 0, 1);
  auto cfg = make_cfg();
  ReplicaManager rm(meta, cfg);

  rm.register_pending(0, {0x01}, 11);
  rm.register_pending(1, {0x02}, 22);
  rm.register_pending(2, {0x03}, 33);

  // ISR acks all three offsets
  rm.on_replica_ack(1, 3);

  std::vector<int> released_fds;
  rm.set_post_response_cb(
      [&released_fds](int fd, std::vector<uint8_t>) { released_fds.push_back(fd); });
  rm.start();
  rm.enqueue_committed(0, make_recs(0, 3));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  rm.stop();

  ASSERT_EQ(released_fds.size(), 3u);
  EXPECT_EQ(released_fds[0], 11);
  EXPECT_EQ(released_fds[1], 22);
  EXPECT_EQ(released_fds[2], 33);
}

// ── Test 6: ISR shrinks after register — isr_required snapshotted ───────────
// register_pending snapshots ISR.size() at registration time.
// Even if a follower drops out of ISR afterwards, existing PendingAcks
// still require the originally-snapshotted number of acks.
TEST(PendingAck, IsrShrinkAfterRegisterKeepsRequired) {
  PartitionMeta meta;
  init_meta(meta, 0, 2); // 2 followers → isr_required = 2
  auto cfg = make_cfg();
  ReplicaManager rm(meta, cfg);

  // Register while both followers are in ISR
  rm.register_pending(7, {0xFF}, 44);

  // Shrink ISR: follower 2 times out
  {
    std::lock_guard<std::mutex> lk(meta.mu);
    for (auto &r : meta.replicas)
      if (r.broker_id == 2)
        r.in_isr = false;
  }

  // Only follower 1 acks
  auto early = rm.on_replica_ack(1, 8);
  EXPECT_TRUE(early.empty()) << "One of required-2 acks must not release";

  std::vector<std::pair<int, std::vector<uint8_t>>> released;
  rm.set_post_response_cb([&released](int fd, std::vector<uint8_t> resp) {
    released.push_back({fd, std::move(resp)});
  });
  rm.start();
  rm.enqueue_committed(7, make_recs(7, 1));
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  EXPECT_TRUE(released.empty()) << "Still waiting for 2nd ISR ack";

  // Second follower acks — both conditions now met
  auto final_release = rm.on_replica_ack(2, 8);
  rm.stop();

  EXPECT_FALSE(final_release.empty()) << "Both acks received: must release now";
}
