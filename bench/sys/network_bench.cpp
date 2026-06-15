#include "broker/protocol.hpp"
#include "platform/socket.hpp"
#include "replication/repl_protocol.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#ifdef min
#undef min
#endif

#ifdef max
#undef max
#endif

using namespace fell;

static inline uint16_t swap_be16(uint16_t v) {
  return static_cast<uint16_t>((v >> 8) | (v << 8));
}

static inline uint32_t swap_be32(uint32_t v) {
  return ((v >> 24) & 0x000000FFu) | ((v >> 8) & 0x0000FF00u) | ((v << 8) & 0x00FF0000u) |
         ((v << 24) & 0xFF000000u);
}

static inline uint64_t swap_be64(uint64_t v) {
  return ((v >> 56) & 0x00000000000000FFULL) | ((v >> 40) & 0x000000000000FF00ULL) |
         ((v >> 24) & 0x0000000000FF0000ULL) | ((v >> 8) & 0x00000000FF000000ULL) |
         ((v << 8) & 0x000000FF00000000ULL) | ((v << 24) & 0x0000FF0000000000ULL) |
         ((v << 40) & 0x00FF000000000000ULL) | ((v << 56) & 0xFF00000000000000ULL);
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

static bool write_exact(socket_t fd, const void *buf, size_t len) {
  size_t total = 0;
  const auto *p = static_cast<const char *>(buf);
  while (total < len) {
    const int n = platform::send_data(fd, p + total, len - total);
    if (n < 0)
      return false;
    total += static_cast<size_t>(n);
  }
  return true;
}

static bool write_frame(socket_t fd, Op op, const void *payload, size_t len) {
  uint8_t header[5];
  const uint32_t frame_len_be = swap_be32(1u + static_cast<uint32_t>(len));
  std::memcpy(header, &frame_len_be, 4);
  header[4] = static_cast<uint8_t>(op);

  if (!write_exact(fd, header, sizeof(header)))
    return false;
  if (len > 0 && payload != nullptr)
    return write_exact(fd, payload, len);
  return true;
}

static bool read_frame(socket_t fd, Op &op, std::vector<uint8_t> &payload) {
  uint32_t len_be{};
  if (!read_exact(fd, &len_be, sizeof(len_be)))
    return false;
  const uint32_t len = swap_be32(len_be);
  if (len < 1)
    return false;

  uint8_t op_byte{};
  if (!read_exact(fd, &op_byte, 1))
    return false;
  op = static_cast<Op>(op_byte);

  payload.resize(len - 1);
  if (len > 1)
    return read_exact(fd, payload.data(), len - 1);
  return true;
}

static bool write_repl_frame(socket_t fd, repl::ReplOp op, const void *payload, size_t len) {
  uint8_t header[5];
  const uint32_t frame_len_be = swap_be32(1u + static_cast<uint32_t>(len));
  std::memcpy(header, &frame_len_be, 4);
  header[4] = static_cast<uint8_t>(op);

  if (!write_exact(fd, header, sizeof(header)))
    return false;
  if (len > 0 && payload != nullptr)
    return write_exact(fd, payload, len);
  return true;
}

static bool read_repl_frame(socket_t fd, repl::ReplOp &op, std::vector<uint8_t> &payload) {
  uint32_t len_be{};
  if (!read_exact(fd, &len_be, sizeof(len_be)))
    return false;
  const uint32_t len = swap_be32(len_be);
  if (len < 1)
    return false;

  uint8_t op_byte{};
  if (!read_exact(fd, &op_byte, 1))
    return false;
  op = static_cast<repl::ReplOp>(op_byte);

  payload.resize(len - 1);
  if (len > 1)
    return read_exact(fd, payload.data(), len - 1);
  return true;
}

static socket_t connect_or_fail(const std::string &host, uint16_t port, const char *label) {
  const socket_t fd = platform::connect_socket(host.c_str(), port);
  if (fd == INVALID_SOCKET_T) {
    std::cerr << "[fell-bench] failed to connect to " << label << " at " << host << ":" << port
              << "\n";
  }
  return fd;
}

struct BrokerInfo {
  uint32_t id;
  std::string host;
  uint16_t client_port;
};

static bool fetch_metadata(const std::string &bootstrap_host, uint16_t bootstrap_port,
                           const std::string &topic,
                           std::unordered_map<uint32_t, BrokerInfo> &brokers,
                           std::unordered_map<uint16_t, uint32_t> &partition_leaders) {
  socket_t fd = platform::connect_socket(bootstrap_host.c_str(), bootstrap_port);
  if (fd == INVALID_SOCKET_T)
    return false;

  proto::MetadataReq req = {};
  req.topic_len = static_cast<uint8_t>(std::min(topic.size(), size_t(255)));
  std::memcpy(req.topic, topic.data(), req.topic_len);

  if (!write_frame(fd, Op::METADATA_REQ, &req, sizeof(req))) {
    platform::close_socket(fd);
    return false;
  }

  Op op;
  std::vector<uint8_t> payload;
  if (!read_frame(fd, op, payload)) {
    platform::close_socket(fd);
    return false;
  }

  platform::close_socket(fd);

  if (op == Op::ERR || op != Op::METADATA_RESP) {
    return false;
  }

  size_t offset = 0;
  if (offset + 2 > payload.size())
    return false;
  uint16_t num_brokers = (payload[offset] << 8) | payload[offset + 1];
  offset += 2;

  for (uint16_t i = 0; i < num_brokers; ++i) {
    if (offset + 5 > payload.size())
      return false;
    uint32_t id = (payload[offset] << 24) | (payload[offset + 1] << 16) |
                  (payload[offset + 2] << 8) | payload[offset + 3];
    uint8_t host_len = payload[offset + 4];
    offset += 5;

    if (offset + host_len + 2 > payload.size())
      return false;
    std::string host(reinterpret_cast<char *>(payload.data() + offset), host_len);
    offset += host_len;

    uint16_t port = (payload[offset] << 8) | payload[offset + 1];
    offset += 2;

    brokers[id] = {id, host, port};
  }

  if (offset + 2 > payload.size())
    return false;
  uint16_t num_partitions = (payload[offset] << 8) | payload[offset + 1];
  offset += 2;

  for (uint16_t i = 0; i < num_partitions; ++i) {
    if (offset + 6 > payload.size())
      return false;
    uint16_t part_idx = (payload[offset] << 8) | payload[offset + 1];
    uint32_t leader_id = (payload[offset + 2] << 24) | (payload[offset + 3] << 16) |
                         (payload[offset + 4] << 8) | payload[offset + 5];
    offset += 6;

    partition_leaders[part_idx] = leader_id;
  }

  return true;
}

static socket_t connect_to_leader(const std::string &host, uint16_t port, const std::string &topic,
                                  uint16_t partition) {
  std::unordered_map<uint32_t, BrokerInfo> brokers;
  std::unordered_map<uint16_t, uint32_t> partition_leaders;

  if (!fetch_metadata(host, port, topic, brokers, partition_leaders)) {
    std::cerr << "[fell-bench] failed to fetch metadata for topic " << topic << "\n";
    return INVALID_SOCKET_T;
  }

  auto it = partition_leaders.find(partition);
  if (it == partition_leaders.end()) {
    std::cerr << "[fell-bench] partition " << partition << " not found in metadata\n";
    return INVALID_SOCKET_T;
  }

  auto b_it = brokers.find(it->second);
  if (b_it == brokers.end()) {
    std::cerr << "[fell-bench] leader " << it->second << " not found in brokers\n";
    return INVALID_SOCKET_T;
  }

  const socket_t fd = platform::connect_socket(b_it->second.host.c_str(), b_it->second.client_port);
  if (fd == INVALID_SOCKET_T) {
    std::cerr << "[fell-bench] failed to connect to leader at " << b_it->second.host << ":"
              << b_it->second.client_port << "\n";
  }
  return fd;
}

static uint64_t fnv1a(const char *data, size_t len) {
  uint64_t hash = 14695981039346656037ULL;
  for (size_t i = 0; i < len; ++i) {
    hash ^= static_cast<uint8_t>(data[i]);
    hash *= 1099511628211ULL;
  }
  return hash;
}

static double percentile(const std::vector<double> &sorted, double p) {
  if (sorted.empty())
    return 0.0;
  const size_t idx = static_cast<size_t>(sorted.size() * p);
  return sorted[std::min(idx, sorted.size() - 1)];
}

struct Results {
  std::vector<double> latencies_us;
  size_t completed = 0;
  size_t busy_retries = 0;
};

struct RunConfig {
  std::string host = "127.0.0.1";
  uint16_t leader_port = 7700;
  uint16_t repl_port = 8700;
  std::string replication_mode = "acks1";
  std::string scenario = "producer";
  std::string topic = "bench";
  uint16_t partitions = 1;
  int total_ops = 100000;
  int warmup_ops = 0;
  int num_threads = 4;
  int payload_sz = 256;
  int pipeline = 1;
  bool shared_partition = false;
  std::string routing_key;
  bool retry_on_busy = false;
  bool create_topic = true;
  int expected_followers = 0;
  double baseline_throughput = 0.0;
  double baseline_p50_us = 0.0;
  double baseline_p99_us = 0.0;
};

struct WorkerParams {
  int thread_id = 0;
  int ops = 0;
  int warmup_ops = 0;
  const RunConfig *cfg = nullptr;
  std::mutex *ready_mu = nullptr;
  std::condition_variable *ready_cv = nullptr;
  int *ready_count = nullptr;
  std::mutex *start_mu = nullptr;
  std::condition_variable *start_cv = nullptr;
  bool *start_flag = nullptr;
};

static std::vector<uint8_t> build_publish_payload(const RunConfig &cfg, int partition_idx) {
  const std::string msg_data(static_cast<size_t>(cfg.payload_sz), 'X');
  std::vector<uint8_t> pub_buf;

  if (!cfg.routing_key.empty()) {
    proto::PublishV2Req pub{};
    pub.topic_len = static_cast<uint8_t>(std::min(cfg.topic.size(), size_t(255)));
    std::memcpy(pub.topic, cfg.topic.data(), pub.topic_len);
    pub.partition = swap_be16(0xFFFF);
    pub.key_len = static_cast<uint8_t>(std::min(cfg.routing_key.size(), size_t(255)));
    std::memcpy(pub.key, cfg.routing_key.data(), pub.key_len);
    pub.payload_size = swap_be32(static_cast<uint32_t>(msg_data.size()));

    pub_buf.resize(sizeof(proto::PublishV2Req) + msg_data.size());
    std::memcpy(pub_buf.data(), &pub, sizeof(proto::PublishV2Req));
    std::memcpy(pub_buf.data() + sizeof(proto::PublishV2Req), msg_data.data(), msg_data.size());
  } else {
    proto::PublishReq pub{};
    pub.topic_len = static_cast<uint8_t>(std::min(cfg.topic.size(), size_t(255)));
    std::memcpy(pub.topic, cfg.topic.data(), pub.topic_len);
    pub.partition = swap_be16(static_cast<uint16_t>(partition_idx));
    pub.payload_size = swap_be32(static_cast<uint32_t>(msg_data.size()));

    pub_buf.resize(sizeof(proto::PublishReq) + msg_data.size());
    std::memcpy(pub_buf.data(), &pub, sizeof(proto::PublishReq));
    std::memcpy(pub_buf.data() + sizeof(proto::PublishReq), msg_data.data(), msg_data.size());
  }

  return pub_buf;
}

static bool execute_publish_ops(socket_t fd, int ops, const RunConfig &cfg,
                                const std::vector<uint8_t> &pub_buf, Op send_op,
                                std::vector<double> *latencies_us, size_t &completed,
                                size_t &busy_retries) {
  Op resp_op;
  std::vector<uint8_t> resp_payload;

  if (cfg.pipeline == 1) {
    for (int i = 0; i < ops; ++i) {
      const auto t0 = std::chrono::high_resolution_clock::now();
      int backoff_ms = 10;
      bool success = false;

      while (!success) {
        if (!write_frame(fd, send_op, pub_buf.data(), pub_buf.size()))
          return false;
        if (!read_frame(fd, resp_op, resp_payload))
          return false;

        if (resp_op == Op::ERR && resp_payload.size() >= sizeof(proto::ErrorResp)) {
          const auto *err = reinterpret_cast<const proto::ErrorResp *>(resp_payload.data());
          if (err->code == static_cast<uint8_t>(ErrCode::BUSY) && cfg.retry_on_busy) {
            ++busy_retries;
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(backoff_ms * 2, 1000);
            continue;
          }
          return false;
        }
        success = true;
      }

      const auto t1 = std::chrono::high_resolution_clock::now();
      if (latencies_us) {
        latencies_us->push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
      }
      ++completed;
    }
    return true;
  }

  for (int i = 0; i < ops; i += cfg.pipeline) {
    const int batch = std::min(cfg.pipeline, ops - i);

    for (int w = 0; w < batch; ++w) {
      if (!write_frame(fd, send_op, pub_buf.data(), pub_buf.size()))
        return false;
    }

    for (int w = 0; w < batch; ++w) {
      if (!read_frame(fd, resp_op, resp_payload))
        return false;
      if (resp_op == Op::ERR)
        return false;
    }

    completed += static_cast<size_t>(batch);
  }
  return true;
}

static Results run_worker(const WorkerParams &params) {
  Results res;
  const RunConfig &cfg = *params.cfg;

  auto signal_ready_and_wait_for_start = [&]() {
    {
      std::lock_guard<std::mutex> lk(*params.ready_mu);
      ++(*params.ready_count);
      if (*params.ready_count == cfg.num_threads)
        params.ready_cv->notify_all();
    }

    std::unique_lock<std::mutex> lk(*params.start_mu);
    params.start_cv->wait(lk, [&] { return *params.start_flag; });
  };

  const int partition_idx =
      cfg.routing_key.empty()
          ? static_cast<int>(
                cfg.shared_partition ? 0 : (params.thread_id % std::max(1, int(cfg.partitions))))
          : static_cast<int>(fnv1a(cfg.routing_key.c_str(), cfg.routing_key.size()) %
                             std::max(1, int(cfg.partitions)));

  const socket_t fd =
      connect_to_leader(cfg.host, cfg.leader_port, cfg.topic, static_cast<uint16_t>(partition_idx));
  if (fd == INVALID_SOCKET_T) {
    signal_ready_and_wait_for_start();
    return res;
  }

  const auto pub_buf = build_publish_payload(cfg, partition_idx);
  const Op send_op = cfg.routing_key.empty() ? Op::PUBLISH : Op::PUBLISH_V2;

  if (params.warmup_ops > 0) {
    std::vector<double> ignored_lat;
    size_t ignored_completed = 0;
    size_t ignored_retries = 0;
    if (!execute_publish_ops(fd, params.warmup_ops, cfg, pub_buf, send_op, nullptr,
                             ignored_completed, ignored_retries)) {
      signal_ready_and_wait_for_start();
      platform::close_socket(fd);
      return res;
    }
  }

  signal_ready_and_wait_for_start();

  if (cfg.pipeline == 1) {
    res.latencies_us.reserve(static_cast<size_t>(params.ops));
  }

  execute_publish_ops(fd, params.ops, cfg, pub_buf, send_op,
                      cfg.pipeline == 1 ? &res.latencies_us : nullptr, res.completed,
                      res.busy_retries);

  platform::close_socket(fd);
  return res;
}

class ReplObserver {
public:
  explicit ReplObserver(const RunConfig &cfg) : cfg_(cfg) {
  }

  bool start(uint16_t observed_partitions) {
    observed_partitions_ = std::max<uint16_t>(1, observed_partitions);
    for (uint16_t partition = 0; partition < observed_partitions_; ++partition) {
      socket_t fd = connect_or_fail(cfg_.host, cfg_.repl_port, "replication");
      if (fd == INVALID_SOCKET_T)
        return false;

      repl::FetchLogReq req{};
      req.topic_len = static_cast<uint8_t>(std::min(cfg_.topic.size(), size_t(255)));
      std::memcpy(req.topic, cfg_.topic.data(), req.topic_len);
      req.partition = swap_be16(partition);
      req.start_offset = swap_be64(0);
      req.follower_id = swap_be32(static_cast<uint32_t>(9000 + partition));

      if (!write_repl_frame(fd, repl::ReplOp::FETCH_LOG, &req, sizeof(req))) {
        platform::close_socket(fd);
        return false;
      }

      sockets_.push_back(fd);
      threads_.emplace_back([this, fd]() { this->drain_socket(fd); });
    }
    return true;
  }

  void stop() {
    stopping_.store(true, std::memory_order_release);
    for (socket_t fd : sockets_) {
      platform::close_socket(fd);
    }
    for (auto &t : threads_) {
      if (t.joinable())
        t.join();
    }
    sockets_.clear();
    threads_.clear();
  }

  ~ReplObserver() {
    stop();
  }

  size_t bytes_observed() const {
    return bytes_observed_.load(std::memory_order_relaxed);
  }

  size_t sync_frames() const {
    return sync_frames_.load(std::memory_order_relaxed);
  }

private:
  void drain_socket(socket_t fd) {
    while (!stopping_.load(std::memory_order_acquire)) {
      repl::ReplOp op;
      std::vector<uint8_t> payload;
      if (!read_repl_frame(fd, op, payload))
        break;
      bytes_observed_.fetch_add(payload.size() + 5, std::memory_order_relaxed);
      if (op == repl::ReplOp::REPLICA_SYNC) {
        sync_frames_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  const RunConfig &cfg_;
  uint16_t observed_partitions_ = 1;
  std::atomic<bool> stopping_{false};
  std::atomic<size_t> bytes_observed_{0};
  std::atomic<size_t> sync_frames_{0};
  std::vector<socket_t> sockets_;
  std::vector<std::thread> threads_;
};

static bool create_or_verify_topic(const RunConfig &cfg) {
  const socket_t fd = connect_or_fail(cfg.host, cfg.leader_port, "leader");
  if (fd == INVALID_SOCKET_T)
    return false;

  proto::CreateTopicReq req{};
  req.name_len = static_cast<uint8_t>(std::min(cfg.topic.size(), size_t(255)));
  std::memcpy(req.name, cfg.topic.data(), req.name_len);
  req.num_partitions = swap_be16(cfg.partitions);

  bool ok = write_frame(fd, Op::CREATE_TOPIC, &req, sizeof(req));
  Op op{};
  std::vector<uint8_t> payload;
  ok = ok && read_frame(fd, op, payload);

  if (ok && op == Op::ERR && payload.size() >= sizeof(proto::ErrorResp)) {
    const auto *err = reinterpret_cast<const proto::ErrorResp *>(payload.data());
    const std::string msg(err->msg, err->msg_len);
    if (msg != "Topic already exists") {
      std::cerr << "[fell-bench] topic setup failed: " << msg << "\n";
      ok = false;
    }
  }

  platform::close_socket(fd);
  return ok;
}

int main(int argc, char *argv[]) {
  RunConfig cfg;

  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--host" && i + 1 < argc)
      cfg.host = argv[++i];
    else if ((a == "--port" || a == "--leader-port") && i + 1 < argc)
      cfg.leader_port = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (a == "--repl-port" && i + 1 < argc)
      cfg.repl_port = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (a == "--replication-mode" && i + 1 < argc)
      cfg.replication_mode = argv[++i];
    else if (a == "--scenario" && i + 1 < argc)
      cfg.scenario = argv[++i];
    else if (a == "--topic" && i + 1 < argc)
      cfg.topic = argv[++i];
    else if (a == "--partitions" && i + 1 < argc)
      cfg.partitions = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (a == "--ops" && i + 1 < argc)
      cfg.total_ops = std::stoi(argv[++i]);
    else if (a == "--warmup-ops" && i + 1 < argc)
      cfg.warmup_ops = std::stoi(argv[++i]);
    else if (a == "--threads" && i + 1 < argc)
      cfg.num_threads = std::stoi(argv[++i]);
    else if (a == "--payload-size" && i + 1 < argc)
      cfg.payload_sz = std::stoi(argv[++i]);
    else if (a == "--pipeline" && i + 1 < argc)
      cfg.pipeline = std::stoi(argv[++i]);
    else if (a == "--shared-partition")
      cfg.shared_partition = true;
    else if (a == "--key" && i + 1 < argc)
      cfg.routing_key = argv[++i];
    else if (a == "--retry-on-busy")
      cfg.retry_on_busy = true;
    else if (a == "--create-topic")
      cfg.create_topic = true;
    else if (a == "--expected-followers" && i + 1 < argc)
      cfg.expected_followers = std::stoi(argv[++i]);
    else if (a == "--baseline-throughput" && i + 1 < argc)
      cfg.baseline_throughput = std::stod(argv[++i]);
    else if (a == "--baseline-p50-us" && i + 1 < argc)
      cfg.baseline_p50_us = std::stod(argv[++i]);
    else if (a == "--baseline-p99-us" && i + 1 < argc)
      cfg.baseline_p99_us = std::stod(argv[++i]);
  }

  if (cfg.num_threads <= 0 || cfg.total_ops <= 0 || cfg.pipeline <= 0 || cfg.payload_sz <= 0 ||
      cfg.partitions == 0) {
    std::cerr << "[fell-bench] invalid numeric arguments\n";
    return 1;
  }

  const bool follower_observer = (cfg.scenario == "live-follower");

  std::cout << "[fell-bench] network (external cluster)\n"
            << "  host:              " << cfg.host << "\n"
            << "  leader_port:       " << cfg.leader_port << "\n"
            << "  repl_port:         " << cfg.repl_port << "\n"
            << "  scenario:          " << cfg.scenario << "\n"
            << "  replication_mode:  " << cfg.replication_mode << "\n"
            << "  topic:             " << cfg.topic << "\n"
            << "  partitions:        " << cfg.partitions << "\n"
            << "  expected_followers:" << cfg.expected_followers << "\n"
            << "  ops:               " << cfg.total_ops << "\n"
            << "  warmup_ops/thread: " << cfg.warmup_ops << "\n"
            << "  threads:           " << cfg.num_threads << "\n"
            << "  payload:           " << cfg.payload_sz << " bytes\n"
            << "  partition_mode:    " << (cfg.shared_partition ? "shared" : "dedicated") << "\n"
            << "  pipeline:          " << cfg.pipeline
            << (cfg.pipeline == 1 ? " (synchronous)\n" : " (pipelined)\n")
            << "  routing_key:       " << (cfg.routing_key.empty() ? "(none)" : cfg.routing_key)
            << "\n"
            << "  retry_on_busy:     " << (cfg.retry_on_busy ? "enabled" : "disabled") << "\n"
            << "  create_topic:      " << (cfg.create_topic ? "yes" : "no") << "\n\n";

  platform::platform_net_init();

  if (cfg.create_topic && !create_or_verify_topic(cfg)) {
    platform::platform_net_cleanup();
    return 1;
  }

  std::mutex ready_mu;
  std::condition_variable ready_cv;
  int ready_count = 0;

  std::mutex start_mu;
  std::condition_variable start_cv;
  bool start_flag = false;

  std::vector<std::thread> workers;
  std::vector<Results> results(static_cast<size_t>(cfg.num_threads));

  const int base_ops = cfg.total_ops / cfg.num_threads;
  const int extra_ops = cfg.total_ops % cfg.num_threads;

  for (int t = 0; t < cfg.num_threads; ++t) {
    const int ops = base_ops + (t < extra_ops ? 1 : 0);
    WorkerParams params;
    params.thread_id = t;
    params.ops = ops;
    params.warmup_ops = cfg.warmup_ops;
    params.cfg = &cfg;
    params.ready_mu = &ready_mu;
    params.ready_cv = &ready_cv;
    params.ready_count = &ready_count;
    params.start_mu = &start_mu;
    params.start_cv = &start_cv;
    params.start_flag = &start_flag;

    workers.emplace_back(
        [&, t, params]() { results[static_cast<size_t>(t)] = run_worker(params); });
  }

  {
    std::unique_lock<std::mutex> lk(ready_mu);
    ready_cv.wait(lk, [&] { return ready_count == cfg.num_threads; });
  }

  ReplObserver observer(cfg);
  if (follower_observer) {
    const uint16_t observed_partitions = cfg.shared_partition ? 1 : cfg.partitions;
    if (!observer.start(observed_partitions)) {
      std::cerr << "[fell-bench] failed to start live follower observer\n";
      {
        std::lock_guard<std::mutex> lk(start_mu);
        start_flag = true;
      }
      start_cv.notify_all();
      for (auto &w : workers)
        w.join();
      platform::platform_net_cleanup();
      return 1;
    }
  }

  const auto wall_start = std::chrono::high_resolution_clock::now();
  {
    std::lock_guard<std::mutex> lk(start_mu);
    start_flag = true;
  }
  start_cv.notify_all();

  for (auto &w : workers)
    w.join();

  const auto wall_end = std::chrono::high_resolution_clock::now();
  const double elapsed_s = std::chrono::duration<double>(wall_end - wall_start).count();

  if (follower_observer) {
    observer.stop();
  }

  size_t total_completed = 0;
  size_t total_busy_retries = 0;
  std::vector<double> all_lat;
  all_lat.reserve(static_cast<size_t>(cfg.total_ops));
  for (auto &r : results) {
    total_completed += r.completed;
    total_busy_retries += r.busy_retries;
    all_lat.insert(all_lat.end(), r.latencies_us.begin(), r.latencies_us.end());
  }

  if (total_completed == 0) {
    std::cerr << "[fell-bench] zero completed ops. Verify the external cluster is running and the "
                 "leader port is correct.\n";
    platform::platform_net_cleanup();
    return 1;
  }

  const double req_struct_size =
      cfg.routing_key.empty() ? sizeof(proto::PublishReq) : sizeof(proto::PublishV2Req);
  const double bytes_per_op = 5.0 + req_struct_size + static_cast<double>(cfg.payload_sz);
  const double throughput_ops = static_cast<double>(total_completed) / elapsed_s;
  const double throughput_mb = throughput_ops * bytes_per_op / (1024.0 * 1024.0);

  std::cout << "=================== RESULTS ===================\n"
            << "  Duration:         " << elapsed_s << " s\n"
            << "  Completed ops:    " << total_completed << "\n"
            << "  Busy retries:     " << total_busy_retries << "\n"
            << "  Throughput:       " << throughput_ops << " ops/sec\n"
            << "  Bandwidth:        " << throughput_mb << " MB/sec\n";

  double p50 = 0.0;
  double p99 = 0.0;
  if (cfg.pipeline == 1 && !all_lat.empty()) {
    std::sort(all_lat.begin(), all_lat.end());
    const double sum = std::accumulate(all_lat.begin(), all_lat.end(), 0.0);
    p50 = percentile(all_lat, 0.500);
    p99 = percentile(all_lat, 0.990);

    std::cout << "\n  Latency (per-op RTT, us):\n"
              << "    min:   " << all_lat.front() << "\n"
              << "    mean:  " << sum / all_lat.size() << "\n"
              << "    p50:   " << p50 << "\n"
              << "    p90:   " << percentile(all_lat, 0.900) << "\n"
              << "    p99:   " << p99 << "\n"
              << "    p99.9: " << percentile(all_lat, 0.999) << "\n"
              << "    max:   " << all_lat.back() << "\n";
  } else if (cfg.pipeline > 1) {
    std::cout << "\n  Latency: N/A in pipelined mode.\n"
                 "  Use --pipeline 1 for RTT measurements.\n";
  }

  if (follower_observer) {
    const double repl_mb = static_cast<double>(observer.bytes_observed()) / (1024.0 * 1024.0) /
                           std::max(elapsed_s, 1e-9);
    std::cout << "\n  Live follower observer:\n"
              << "    sync_frames: " << observer.sync_frames() << "\n"
              << "    bytes:       " << observer.bytes_observed() << "\n"
              << "    ingress:     " << repl_mb << " MB/sec\n";
  }

  if (cfg.baseline_throughput > 0.0) {
    std::cout << "\n  Replication delta:\n"
              << "    throughput_ratio_vs_baseline: " << (throughput_ops / cfg.baseline_throughput)
              << "\n";
    if (cfg.pipeline == 1 && cfg.baseline_p50_us > 0.0 && cfg.baseline_p99_us > 0.0) {
      std::cout << "    p50_delta_us:               " << (p50 - cfg.baseline_p50_us) << "\n"
                << "    p99_delta_us:               " << (p99 - cfg.baseline_p99_us) << "\n";
    } else if (cfg.pipeline == 1) {
      std::cout << "    p50/p99 baseline values not supplied; only throughput delta reported.\n";
    }
  }

  std::cout << "===============================================\n";

  platform::platform_net_cleanup();
  return 0;
}
