#include "broker/protocol.hpp"
#include "platform/socket.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

using namespace fell;

// Symmetric big-endian byte-swapping helpers
static inline uint16_t swap_be16(uint16_t val) {
  return (val >> 8) | (val << 8);
}

static inline uint32_t swap_be32(uint32_t val) {
  return ((val >> 24) & 0x000000FF) | ((val >> 8) & 0x0000FF00) | ((val << 8) & 0x00FF0000) |
         ((val << 24) & 0xFF000000);
}

// Blocking helper to read exactly N bytes from the socket
bool read_exact(int fd, void *buf, size_t len) {
  size_t total = 0;
  char *p = static_cast<char *>(buf);
  while (total < len) {
    int n = platform::recv_data(fd, p + total, len - total);
    if (n <= 0)
      return false;
    total += n;
  }
  return true;
}

// Blocking helper to write exactly N bytes to the socket
bool write_exact(int fd, const void *buf, size_t len) {
  size_t total = 0;
  const char *p = static_cast<const char *>(buf);
  while (total < len) {
    int n = platform::send_data(fd, p + total, len - total);
    if (n < 0)
      return false;
    total += n;
  }
  return true;
}

bool write_frame(int fd, Op op, const void *payload, size_t len) {
  std::vector<uint8_t> header(5);
  uint32_t frame_len = 1 + static_cast<uint32_t>(len);

  uint32_t len_be = swap_be32(frame_len);
  std::memcpy(header.data(), &len_be, 4);
  header[4] = static_cast<uint8_t>(op);

  if (!write_exact(fd, header.data(), 5))
    return false;
  if (len > 0 && payload != nullptr) {
    if (!write_exact(fd, payload, len))
      return false;
  }
  return true;
}

bool read_frame(int fd, Op &op, std::vector<uint8_t> &payload) {
  uint32_t len_be;
  if (!read_exact(fd, &len_be, 4))
    return false;
  uint32_t len = swap_be32(len_be);

  if (len < 1)
    return false;

  uint8_t op_byte;
  if (!read_exact(fd, &op_byte, 1))
    return false;
  op = static_cast<Op>(op_byte);

  payload.resize(len - 1);
  if (len > 1) {
    if (!read_exact(fd, payload.data(), len - 1))
      return false;
  }
  return true;
}

int main(int argc, char *argv[]) {
  std::string host = "127.0.0.1";
  uint16_t port = 7700;
  int total_ops = 50000;
  int num_threads = 4;
  int payload_size = 128;

  // CLI argument parsing
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--host" && i + 1 < argc)
      host = argv[++i];
    else if (arg == "--port" && i + 1 < argc)
      port = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (arg == "--ops" && i + 1 < argc)
      total_ops = std::stoi(argv[++i]);
    else if (arg == "--threads" && i + 1 < argc)
      num_threads = std::stoi(argv[++i]);
    else if (arg == "--payload-size" && i + 1 < argc)
      payload_size = std::stoi(argv[++i]);
  }

  std::cout << "[SystemBench] Starting benchmark..." << std::endl;
  std::cout << "  Broker Address: " << host << ":" << port << std::endl;
  std::cout << "  Total Ops:      " << total_ops << std::endl;
  std::cout << "  Concurrency:    " << num_threads << " threads" << std::endl;
  std::cout << "  Payload Size:   " << payload_size << " bytes" << std::endl << std::endl;

  platform::platform_net_init();

  int ops_per_thread = total_ops / num_threads;
  std::vector<std::thread> workers;
  std::vector<std::vector<double>> thread_latencies(num_threads);
  std::mutex console_mu;

  // Thread synchronization barrier variables
  std::mutex barrier_mu;
  std::condition_variable barrier_cv;
  int active_connections = 0;

  auto global_start_time = std::chrono::high_resolution_clock::now();

  for (int t = 0; t < num_threads; ++t) {
    workers.emplace_back([&, t]() {
      int fd = platform::connect_socket(host.c_str(), port);
      if (fd < 0) {
        std::lock_guard<std::mutex> lock(console_mu);
        std::cerr << "Thread " << t << " failed to connect to broker." << std::endl;
        return;
      }

      // Pre-create the topic once (gracefully ignore duplicate errors)
      if (t == 0) {
        proto::CreateTopicReq req = {};
        req.name_len = 5;
        std::memcpy(req.name, "bench", 5);
        req.num_partitions = swap_be16(static_cast<uint16_t>(num_threads));
        write_frame(fd, Op::CREATE_TOPIC, &req, sizeof(req));
        Op op;
        std::vector<uint8_t> payload;
        read_frame(fd, op, payload);
      }

      // Signal connection readiness
      {
        std::lock_guard<std::mutex> lock(barrier_mu);
        active_connections++;
        if (active_connections == num_threads) {
          barrier_cv.notify_all();
        }
      }

      // Wait for all threads to establish connections
      {
        std::unique_lock<std::mutex> lock(barrier_mu);
        barrier_cv.wait(lock, [&]() { return active_connections == num_threads; });
      }

      // Generate message payload
      std::string msg_data(payload_size, 'X');
      proto::PublishReq pub = {};
      pub.topic_len = 5;
      std::memcpy(pub.topic, "bench", 5);
      pub.partition = swap_be16(static_cast<uint16_t>(t)); // Send to thread-specific partition
      pub.payload_size = swap_be32(static_cast<uint32_t>(msg_data.size()));

      std::vector<uint8_t> pub_buf(sizeof(proto::PublishReq) + msg_data.size());
      std::memcpy(pub_buf.data(), &pub, sizeof(proto::PublishReq));
      std::memcpy(pub_buf.data() + sizeof(proto::PublishReq), msg_data.data(), msg_data.size());

      thread_latencies[t].reserve(ops_per_thread);

      // Perform load generations
      for (int i = 0; i < ops_per_thread; ++i) {
        auto start = std::chrono::high_resolution_clock::now();

        if (!write_frame(fd, Op::PUBLISH, pub_buf.data(), pub_buf.size())) {
          break;
        }

        Op op;
        std::vector<uint8_t> resp;
        if (!read_frame(fd, op, resp)) {
          break;
        }

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed_us = std::chrono::duration<double, std::micro>(end - start).count();
        thread_latencies[t].push_back(elapsed_us);
      }

      platform::close_socket(fd);
    });
  }

  for (auto &w : workers) {
    w.join();
  }

  auto global_end_time = std::chrono::high_resolution_clock::now();
  double total_elapsed_s =
      std::chrono::duration<double>(global_end_time - global_start_time).count();

  // Combine and analyze latencies
  std::vector<double> all_latencies;
  all_latencies.reserve(total_ops);
  for (const auto &lat : thread_latencies) {
    all_latencies.insert(all_latencies.end(), lat.begin(), lat.end());
  }

  if (all_latencies.empty()) {
    std::cerr << "Benchmark finished with zero completed operations." << std::endl;
    platform::platform_net_cleanup();
    return 1;
  }

  std::sort(all_latencies.begin(), all_latencies.end());

  double sum = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0);
  double mean = sum / all_latencies.size();
  double min = all_latencies.front();
  double max = all_latencies.back();
  double p50 = all_latencies[static_cast<size_t>(all_latencies.size() * 0.50)];
  double p90 = all_latencies[static_cast<size_t>(all_latencies.size() * 0.90)];
  double p99 = all_latencies[static_cast<size_t>(all_latencies.size() * 0.99)];

  double throughput_ops = all_latencies.size() / total_elapsed_s;
  double throughput_mb = (all_latencies.size() * (payload_size + sizeof(proto::PublishReq) + 5)) /
                         (1024.0 * 1024.0) / total_elapsed_s;

  std::cout << "=================== RESULTS ===================" << std::endl;
  std::cout << "  Duration:          " << total_elapsed_s << " seconds" << std::endl;
  std::cout << "  Completed Ops:     " << all_latencies.size() << std::endl;
  std::cout << "  Throughput:        " << throughput_ops << " ops/sec" << std::endl;
  std::cout << "  Network Bandwidth: " << throughput_mb << " MB/sec" << std::endl << std::endl;
  std::cout << "  Latency Metrics (Microseconds):" << std::endl;
  std::cout << "    Min:             " << min << " us" << std::endl;
  std::cout << "    Mean:            " << mean << " us" << std::endl;
  std::cout << "    p50 (Median):    " << p50 << " us" << std::endl;
  std::cout << "    p90:             " << p90 << " us" << std::endl;
  std::cout << "    p99:             " << p99 << " us" << std::endl;
  std::cout << "    Max:             " << max << " us" << std::endl;
  std::cout << "===============================================" << std::endl;

  platform::platform_net_cleanup();
  return 0;
}
