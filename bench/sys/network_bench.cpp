#include "broker/protocol.hpp"
#include "platform/socket.hpp"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

using namespace fell;

// ── Byte-swap helpers ─────────────────────────────────────────────────────────

static inline uint16_t swap_be16(uint16_t v) {
  return static_cast<uint16_t>((v >> 8) | (v << 8));
}

static inline uint32_t swap_be32(uint32_t v) {
  return ((v >> 24) & 0x000000FFu) | ((v >> 8) & 0x0000FF00u) | ((v << 8) & 0x00FF0000u) |
         ((v << 24) & 0xFF000000u);
}

// ── Socket helpers ────────────────────────────────────────────────────────────

static bool read_exact(int fd, void *buf, size_t len) {
  size_t total = 0;
  auto *p = static_cast<char *>(buf);
  while (total < len) {
    int n = platform::recv_data(fd, p + total, len - total);
    if (n <= 0)
      return false;
    total += static_cast<size_t>(n);
  }
  return true;
}

static bool write_exact(int fd, const void *buf, size_t len) {
  size_t total = 0;
  const auto *p = static_cast<const char *>(buf);
  while (total < len) {
    int n = platform::send_data(fd, p + total, len - total);
    if (n < 0)
      return false;
    total += static_cast<size_t>(n);
  }
  return true;
}

// Stack-allocated header — no heap alloc in the hot path.
static bool write_frame(int fd, Op op, const void *payload, size_t len) {
  uint8_t header[5];
  uint32_t frame_len_be = swap_be32(1u + static_cast<uint32_t>(len));
  std::memcpy(header, &frame_len_be, 4);
  header[4] = static_cast<uint8_t>(op);

  if (!write_exact(fd, header, 5))
    return false;
  if (len > 0 && payload != nullptr)
    return write_exact(fd, payload, len);
  return true;
}

static bool read_frame(int fd, Op &op, std::vector<uint8_t> &payload) {
  uint32_t len_be{};
  if (!read_exact(fd, &len_be, 4))
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

// ── Latency storage ───────────────────────────────────────────────────────────
// In pipelined mode we cannot attribute latency to individual ops — only the
// window time is known. We record nothing for latency in that mode and report
// throughput only. This avoids the synthetic per-op average that the previous
// version produced.

struct Results {
  std::vector<double> latencies_us; // populated in sync mode only
  size_t completed = 0;
};

// ── Worker ────────────────────────────────────────────────────────────────────

static Results run_worker(int thread_id, int num_threads, int ops, int pipeline_window,
                          int payload_size, const std::string &host, uint16_t port,
                          // barrier state (shared across threads)
                          std::mutex &barrier_mu, std::condition_variable &barrier_cv,
                          int &ready_count) {
  Results res;

  const int fd = platform::connect_socket(host.c_str(), port);
  if (fd < 0) {
    std::cerr << "[thread " << thread_id << "] connect failed\n";
    return res;
  }

  // Thread 0 creates the topic. All others wait for the barrier.
  if (thread_id == 0) {
    proto::CreateTopicReq req{};
    req.name_len = 5;
    std::memcpy(req.name, "bench", 5);
    req.num_partitions = swap_be16(static_cast<uint16_t>(num_threads));
    write_frame(fd, Op::CREATE_TOPIC, &req, sizeof(req));
    Op op;
    std::vector<uint8_t> resp;
    read_frame(fd, op, resp); // drain ACK before releasing barrier
  }

  // Signal ready and wait for all threads.
  {
    std::lock_guard<std::mutex> lk(barrier_mu);
    ++ready_count;
    if (ready_count == num_threads)
      barrier_cv.notify_all();
  }
  {
    std::unique_lock<std::mutex> lk(barrier_mu);
    barrier_cv.wait(lk, [&] { return ready_count == num_threads; });
  }

  // Build publish buffer once, reused for every send.
  const std::string msg_data(payload_size, 'X');
  proto::PublishReq pub{};
  pub.topic_len = 5;
  std::memcpy(pub.topic, "bench", 5);
  pub.partition = swap_be16(static_cast<uint16_t>(thread_id));
  pub.payload_size = swap_be32(static_cast<uint32_t>(msg_data.size()));

  std::vector<uint8_t> pub_buf(sizeof(proto::PublishReq) + msg_data.size());
  std::memcpy(pub_buf.data(), &pub, sizeof(proto::PublishReq));
  std::memcpy(pub_buf.data() + sizeof(proto::PublishReq), msg_data.data(), msg_data.size());

  Op resp_op;
  std::vector<uint8_t> resp_payload;

  if (pipeline_window == 1) {
    // ── Synchronous mode ─────────────────────────────────────────────────
    // One send → one recv per iteration.
    // Latency = true per-op round-trip time.
    res.latencies_us.reserve(static_cast<size_t>(ops));

    for (int i = 0; i < ops; ++i) {
      const auto t0 = std::chrono::high_resolution_clock::now();

      if (!write_frame(fd, Op::PUBLISH, pub_buf.data(), pub_buf.size()))
        break;
      if (!read_frame(fd, resp_op, resp_payload))
        break;

      const auto t1 = std::chrono::high_resolution_clock::now();
      res.latencies_us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
      ++res.completed;
    }
  } else {
    // ── Pipelined mode ───────────────────────────────────────────────────
    // Send `pipeline_window` frames, then drain their ACKs.
    // Latency is not recorded: window time cannot be meaningfully split
    // across individual ops. Only throughput (ops/sec) is meaningful here.
    bool failed = false;
    for (int i = 0; i < ops && !failed; i += pipeline_window) {
      const int batch = std::min(pipeline_window, ops - i);

      for (int w = 0; w < batch && !failed; ++w)
        if (!write_frame(fd, Op::PUBLISH, pub_buf.data(), pub_buf.size()))
          failed = true;

      for (int w = 0; w < batch && !failed; ++w)
        if (!read_frame(fd, resp_op, resp_payload))
          failed = true;

      res.completed += static_cast<size_t>(!failed ? batch : 0);
    }
  }

  platform::close_socket(fd);
  return res;
}

// ── Percentile helper ─────────────────────────────────────────────────────────
// Assumes vec is sorted.

static double percentile(const std::vector<double> &sorted, double p) {
  if (sorted.empty())
    return 0.0;
  const size_t idx = static_cast<size_t>(sorted.size() * p);
  return sorted[std::min(idx, sorted.size() - 1)];
}

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char *argv[]) {
  std::string host = "127.0.0.1";
  uint16_t port = 7700;
  int total_ops = 100000;
  int num_threads = 4;
  int payload_sz = 256;
  int pipeline = 1;

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--host" && i + 1 < argc)
      host = argv[++i];
    else if (a == "--port" && i + 1 < argc)
      port = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (a == "--ops" && i + 1 < argc)
      total_ops = std::stoi(argv[++i]);
    else if (a == "--threads" && i + 1 < argc)
      num_threads = std::stoi(argv[++i]);
    else if (a == "--payload-size" && i + 1 < argc)
      payload_sz = std::stoi(argv[++i]);
    else if (a == "--pipeline" && i + 1 < argc)
      pipeline = std::stoi(argv[++i]);
  }

  std::cout << "[fell-bench] network\n"
            << "  host:     " << host << ":" << port << "\n"
            << "  ops:      " << total_ops << "\n"
            << "  threads:  " << num_threads << "\n"
            << "  payload:  " << payload_sz << " bytes\n"
            << "  pipeline: " << pipeline << (pipeline == 1 ? " (synchronous)\n" : " (pipelined)\n")
            << "\n";

  platform::platform_net_init();

  const int ops_per_thread = total_ops / num_threads;

  std::mutex barrier_mu;
  std::condition_variable barrier_cv;
  int ready_count = 0;

  std::vector<std::thread> workers;
  std::vector<Results> results(static_cast<size_t>(num_threads));

  const auto wall_start = std::chrono::high_resolution_clock::now();

  for (int t = 0; t < num_threads; ++t) {
    workers.emplace_back([&, t]() {
      results[static_cast<size_t>(t)] =
          run_worker(t, num_threads, ops_per_thread, pipeline, payload_sz, host, port, barrier_mu,
                     barrier_cv, ready_count);
    });
  }
  for (auto &w : workers)
    w.join();

  const auto wall_end = std::chrono::high_resolution_clock::now();
  const double elapsed_s = std::chrono::duration<double>(wall_end - wall_start).count();

  // Aggregate
  size_t total_completed = 0;
  std::vector<double> all_lat;
  all_lat.reserve(static_cast<size_t>(total_ops));

  for (auto &r : results) {
    total_completed += r.completed;
    all_lat.insert(all_lat.end(), r.latencies_us.begin(), r.latencies_us.end());
  }

  if (total_completed == 0) {
    std::cerr << "zero completed ops\n";
    platform::platform_net_cleanup();
    return 1;
  }

  // Bytes: 5 (frame header) + sizeof(PublishReq) + payload
  const double bytes_per_op = 5.0 + sizeof(proto::PublishReq) + static_cast<double>(payload_sz);
  const double throughput_ops = static_cast<double>(total_completed) / elapsed_s;
  const double throughput_mb = throughput_ops * bytes_per_op / (1024.0 * 1024.0);

  std::cout << "=================== RESULTS ===================\n"
            << "  Duration:       " << elapsed_s << " s\n"
            << "  Completed ops:  " << total_completed << "\n"
            << "  Throughput:     " << throughput_ops << " ops/sec\n"
            << "  Bandwidth:      " << throughput_mb << " MB/sec\n";

  if (pipeline == 1 && !all_lat.empty()) {
    std::sort(all_lat.begin(), all_lat.end());
    const double sum = std::accumulate(all_lat.begin(), all_lat.end(), 0.0);
    std::cout << "\n  Latency (per-op RTT, µs):\n"
              << "    min:   " << all_lat.front() << "\n"
              << "    mean:  " << sum / all_lat.size() << "\n"
              << "    p50:   " << percentile(all_lat, 0.500) << "\n"
              << "    p90:   " << percentile(all_lat, 0.900) << "\n"
              << "    p99:   " << percentile(all_lat, 0.990) << "\n"
              << "    p99.9: " << percentile(all_lat, 0.999) << "\n"
              << "    max:   " << all_lat.back() << "\n";
  } else if (pipeline > 1) {
    std::cout << "\n  Latency: N/A in pipelined mode"
                 "window time cannot be split per op.\n"
                 "  Use --pipeline 1 for RTT measurements.\n";
  }

  std::cout << "===============================================\n";

  platform::platform_net_cleanup();
  return 0;
}