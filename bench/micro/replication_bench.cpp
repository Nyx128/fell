#include "benchmark/benchmark.h"
#include "broker/topic_registry.hpp"
#include "platform/socket.hpp"
#include "replication/cluster_config.hpp"
#include "replication/partition_meta.hpp"
#include "replication/replica_manager.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>

using namespace fell;
using namespace fell::repl;
using namespace fell::storage;

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

static bool read_exact(socket_t fd, void *buf, size_t len) {
  size_t total = 0;
  auto *p = static_cast<char *>(buf);
  while (total < len) {
    const int n = platform::recv_data(fd, p + total, len - total);
    if (n <= 0)
      return false;
    total += static_cast<size_t>(n);
  }
  return true;
}

static void init_meta(PartitionMeta &meta, uint32_t followers) {
  meta.topic = "bench-repl";
  meta.partition_idx = 0;
  meta.leader_id = 0;
  meta.role = PartitionRole::Leader;
  meta.epoch = 1;
  meta.replicas.clear();

  ReplicaState leader{};
  leader.broker_id = 0;
  leader.in_isr = true;
  meta.replicas.push_back(leader);

  for (uint32_t i = 1; i <= followers; ++i) {
    ReplicaState rs{};
    rs.broker_id = i;
    rs.in_isr = true;
    meta.replicas.push_back(rs);
  }
}

class ReplicaSendFixture : public benchmark::Fixture {
public:
  PartitionMeta meta;
  ClusterConfig cfg;
  ReplicaManager *rm = nullptr;
  std::vector<std::pair<socket_t, socket_t>> follower_pairs;
  std::vector<CommittedRecord> template_records;
  size_t expected_bytes_per_follower = 0;

  void SetUp(const ::benchmark::State &state) override {
    if (state.thread_index() != 0)
      return;

    platform::platform_net_init();
    const uint32_t followers = static_cast<uint32_t>(state.range(0));
    const uint32_t batch_size = static_cast<uint32_t>(state.range(1));
    const size_t payload_size = static_cast<size_t>(state.range(2));

    init_meta(meta, followers);
    cfg.broker_id = 0;
    cfg.max_lag_messages = 1000;
    rm = new ReplicaManager(meta, cfg);
    expected_bytes_per_follower = 0;

    follower_pairs.clear();
    for (uint32_t follower = 1; follower <= followers; ++follower) {
      socket_t read_fd = INVALID_SOCKET_T;
      socket_t write_fd = INVALID_SOCKET_T;
      platform::create_notify_pair(&read_fd, &write_fd);
      follower_pairs.push_back({read_fd, write_fd});
      rm->set_follower_fd(follower, static_cast<int>(write_fd));
    }

    template_records.clear();
    template_records.reserve(batch_size);
    const std::vector<uint8_t> payload(payload_size, 0xAB);
    for (uint32_t i = 0; i < batch_size; ++i) {
      template_records.push_back({i, 1000 + i, payload});
      expected_bytes_per_follower += 4 + 1 + sizeof(ReplicaSyncHeader) + payload_size;
    }

    rm->start();
  }

  void TearDown(const ::benchmark::State &state) override {
    if (state.thread_index() != 0)
      return;

    if (rm) {
      rm->stop();
      delete rm;
      rm = nullptr;
    }
    for (auto &[read_fd, write_fd] : follower_pairs) {
      platform::close_socket(read_fd);
      platform::close_socket(write_fd);
    }
    follower_pairs.clear();
    template_records.clear();
    expected_bytes_per_follower = 0;
    platform::platform_net_cleanup();
  }
};

BENCHMARK_DEFINE_F(ReplicaSendFixture, LeaderCommittedBatchFanout)(benchmark::State &state) {
  std::vector<uint8_t> drain(expected_bytes_per_follower);
  uint64_t base_offset = 0;

  for (auto _ : state) {
    for (size_t i = 0; i < template_records.size(); ++i) {
      template_records[i].offset = base_offset + i;
    }
    rm->enqueue_committed(base_offset, template_records);
    for (auto &[read_fd, write_fd] : follower_pairs) {
      (void)write_fd;
      read_exact(read_fd, drain.data(), drain.size());
    }
    base_offset += template_records.size();
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(expected_bytes_per_follower) *
                          static_cast<int64_t>(follower_pairs.size()));
}

BENCHMARK_REGISTER_F(ReplicaSendFixture, LeaderCommittedBatchFanout)
    ->Args({1, 1, 64})
    ->Args({1, 32, 64})
    ->Args({2, 1, 256})
    ->Args({2, 32, 1024});

class FollowerApplyFixture : public benchmark::Fixture {
public:
  TopicRegistry *registry = nullptr;
  Partition *partition = nullptr;
  socket_t notify_read_fd = INVALID_SOCKET_T;
  socket_t notify_write_fd = INVALID_SOCKET_T;
  uint64_t next_offset = 0;

  void SetUp(const ::benchmark::State &state) override {
    if (state.thread_index() != 0)
      return;

    safe_remove_all("bench-repl-follower");
    platform::platform_net_init();

    storage::StorageOptions opts;
    opts.batch_wait_us = 0;
    registry = new TopicRegistry("bench-repl-follower", opts);
    registry->create_topic("bench-repl", 1);
    partition = registry->get_partition("bench-repl", 0);

    platform::create_notify_pair(&notify_read_fd, &notify_write_fd);
    next_offset = 0;
  }

  void TearDown(const ::benchmark::State &state) override {
    if (state.thread_index() != 0)
      return;

    if (notify_read_fd != INVALID_SOCKET_T)
      platform::close_socket(notify_read_fd);
    if (notify_write_fd != INVALID_SOCKET_T)
      platform::close_socket(notify_write_fd);
    notify_read_fd = INVALID_SOCKET_T;
    notify_write_fd = INVALID_SOCKET_T;

    delete registry;
    registry = nullptr;
    partition = nullptr;

    safe_remove_all("bench-repl-follower");
    platform::platform_net_cleanup();
  }
};

BENCHMARK_DEFINE_F(FollowerApplyFixture, ApplyAndAckCallbacks)(benchmark::State &state) {
  const uint32_t batch_size = static_cast<uint32_t>(state.range(0));
  const size_t payload_size = static_cast<size_t>(state.range(1));
  const std::vector<uint8_t> payload(payload_size, 0xCD);
  std::vector<uint8_t> notify_buf(batch_size);

  for (auto _ : state) {
    for (uint32_t i = 0; i < batch_size; ++i) {
      const uint64_t expected_offset = next_offset++;
      partition->set_once_commit_callback(expected_offset, [write_fd = notify_write_fd]() {
        uint8_t byte = 1;
        platform::send_data(write_fd, &byte, 1);
      });
      auto result = partition->append(payload.data(), static_cast<uint32_t>(payload.size()));
      benchmark::DoNotOptimize(result);
    }

    read_exact(notify_read_fd, notify_buf.data(), notify_buf.size());
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(batch_size) * static_cast<int64_t>(payload_size));
}

BENCHMARK_REGISTER_F(FollowerApplyFixture, ApplyAndAckCallbacks)
    ->Args({1, 64})
    ->Args({1, 1024})
    ->Args({8, 64})
    ->Args({8, 1024});

BENCHMARK_MAIN();
