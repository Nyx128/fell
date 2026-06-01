#include "broker/protocol.hpp"
#include "platform/socket.hpp"
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

using namespace fell;

// Symmetric big-endian byte-swapping helpers
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

struct Frame {
  Op op;
  std::vector<uint8_t> payload;
};

// Blocking helper to read exactly N bytes from the socket
bool read_exact(socket_t fd, void *buf, size_t len) {
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
bool write_exact(socket_t fd, const void *buf, size_t len) {
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

bool write_frame(socket_t fd, Op op, const void *payload, size_t len) {
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

bool read_frame(socket_t fd, Frame &frame) {
  uint32_t len_be;
  if (!read_exact(fd, &len_be, 4))
    return false;
  uint32_t len = swap_be32(len_be);

  if (len < 1)
    return false;

  uint8_t op_byte;
  if (!read_exact(fd, &op_byte, 1))
    return false;
  frame.op = static_cast<Op>(op_byte);

  frame.payload.resize(len - 1);
  if (len > 1) {
    if (!read_exact(fd, frame.payload.data(), len - 1))
      return false;
  }
  return true;
}

int main(int argc, char *argv[]) {
  std::string host = "127.0.0.1";
  uint16_t port = 7700;
  std::string topic = "";
  bool create = false;
  uint16_t partitions = 1;
  std::string message = "hello fell";
  int count = 1;
  bool retry_on_busy = false;
  std::string key = "";

  // Manual CLI parsing
  // TODO: Replace with a proper CLI parsing library for robustness and help messages
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--host" && i + 1 < argc)
      host = argv[++i];
    else if (arg == "--port" && i + 1 < argc)
      port = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (arg == "--topic" && i + 1 < argc)
      topic = argv[++i];
    else if (arg == "--create")
      create = true;
    else if (arg == "--partitions" && i + 1 < argc)
      partitions = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (arg == "--message" && i + 1 < argc)
      message = argv[++i];
    else if (arg == "--count" && i + 1 < argc)
      count = std::stoi(argv[++i]);
    else if (arg == "--retry-on-busy")
      retry_on_busy = true;
    else if (arg == "--key" && i + 1 < argc)
      key = argv[++i];
  }

  if (topic.empty()) {
    std::cerr << "Error: --topic is required." << std::endl;
    return 1;
  }

  if (topic.size() > 255) {
    std::cerr << "Error: Topic name exceeds 255 character limit." << std::endl;
    return 1;
  }

  platform::platform_net_init();

  socket_t fd = platform::connect_socket(host.c_str(), port);
  if (fd == INVALID_SOCKET_T) {
    std::cerr << "Failed to connect to broker at " << host << ":" << port << std::endl;
    platform::platform_net_cleanup();
    return 1;
  }

  std::cout << "Connected to broker at " << host << ":" << port << std::endl;

  // If --create: send CREATE_TOPIC request
  if (create) {
    proto::CreateTopicReq req = {};
    req.name_len = static_cast<uint8_t>(topic.size());
    std::memcpy(req.name, topic.data(), topic.size());
    req.num_partitions = swap_be16(partitions);

    if (!write_frame(fd, Op::CREATE_TOPIC, &req, sizeof(req))) {
      std::cerr << "Failed to send CREATE_TOPIC request" << std::endl;
      platform::close_socket(fd);
      platform::platform_net_cleanup();
      return 1;
    }

    Frame resp;
    if (!read_frame(fd, resp)) {
      std::cerr << "Failed to read CREATE_TOPIC response" << std::endl;
      platform::close_socket(fd);
      platform::platform_net_cleanup();
      return 1;
    }

    if (resp.op == Op::ERR) {
      const auto *err = reinterpret_cast<const proto::ErrorResp *>(resp.payload.data());
      std::string msg(err->msg, err->msg_len);
      std::cerr << "CREATE_TOPIC failed: " << msg << std::endl;
      platform::close_socket(fd);
      platform::platform_net_cleanup();
      return 1;
    }
    std::cout << "Topic '" << topic << "' created/verified successfully." << std::endl;
  }

  // Publish messages
  for (int i = 0; i < count; ++i) {
    std::string payload = message;
    if (count > 1) {
      payload += " #" + std::to_string(i);
    }

    proto::PublishV2Req pub = {};
    pub.topic_len = static_cast<uint8_t>(topic.size());
    std::memcpy(pub.topic, topic.data(), topic.size());
    pub.partition = swap_be16(0xFFFF); // broker round-robin or key fallback
    pub.key_len = static_cast<uint8_t>(key.size());
    if (pub.key_len > 0) {
      std::memcpy(pub.key, key.data(), key.size());
    }
    pub.payload_size = swap_be32(static_cast<uint32_t>(payload.size()));

    std::vector<uint8_t> pub_buf(sizeof(proto::PublishV2Req) + payload.size());
    std::memcpy(pub_buf.data(), &pub, sizeof(proto::PublishV2Req));
    std::memcpy(pub_buf.data() + sizeof(proto::PublishV2Req), payload.data(), payload.size());

    int backoff_ms = 10;
    bool success = false;

    while (!success) {
      if (!write_frame(fd, Op::PUBLISH_V2, pub_buf.data(), pub_buf.size())) {
        std::cerr << "Failed to send PUBLISH_V2 request on message " << i << std::endl;
        break; // socket error
      }

      Frame resp;
      if (!read_frame(fd, resp)) {
        std::cerr << "Failed to read PUBLISH_V2 response on message " << i << std::endl;
        break; // socket error
      }

      if (resp.op == Op::ERR) {
        const auto *err = reinterpret_cast<const proto::ErrorResp *>(resp.payload.data());
        std::string msg(err->msg, err->msg_len);

        if (retry_on_busy && err->code == static_cast<uint8_t>(ErrCode::BUSY)) {
          std::cout << "Broker BUSY, sleeping for " << backoff_ms << "ms..." << std::endl;
          std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
          backoff_ms = std::min(backoff_ms * 2, 1000);
          continue; // retry loop
        }

        std::cerr << "PUBLISH_V2 failed: " << msg << std::endl;
        break; // non-retryable or retries disabled
      }

      const auto *ack = reinterpret_cast<const proto::AckResp *>(resp.payload.data());
      uint64_t offset = swap_be64(ack->value);
      std::cout << "Published message: '" << payload << "' -> Assigned offset: " << offset
                << std::endl;
      success = true;
    }
    
    if (!success) {
      break; // Abort further publishing if an unrecoverable error occurred
    }
  }

  platform::close_socket(fd);
  platform::platform_net_cleanup();
  return 0;
}
