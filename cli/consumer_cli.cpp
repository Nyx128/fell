#include "broker/protocol.hpp"
#include "cli_util.hpp"
#include "platform/socket.hpp"
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace fell;

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
  std::string bootstrap_host = "127.0.0.1";
  uint16_t bootstrap_port = 7700;
  std::string topic = "";
  uint16_t partition = 0;
  uint64_t offset = 0;
  int count = -1;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if ((arg == "--host" || arg == "--bootstrap-host") && i + 1 < argc)
      bootstrap_host = argv[++i];
    else if ((arg == "--port" || arg == "--bootstrap-port") && i + 1 < argc)
      bootstrap_port = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (arg == "--topic" && i + 1 < argc)
      topic = argv[++i];
    else if (arg == "--partition" && i + 1 < argc)
      partition = static_cast<uint16_t>(std::stoi(argv[++i]));
    else if (arg == "--offset" && i + 1 < argc) {
      std::string offset_str = argv[++i];
      if (offset_str == "latest") {
        offset = 0xFFFFFFFFFFFFFFFFULL;
      } else {
        offset = std::stoull(offset_str);
      }
    } else if (arg == "--count" && i + 1 < argc)
      count = std::stoi(argv[++i]);
  }

  if (topic.empty() || topic.size() > 255) {
    std::cerr << "Error: valid --topic is required." << std::endl;
    return 1;
  }

  platform::platform_net_init();

  int read_count = 0;

  while (count < 0 || read_count < count) {
    socket_t fd = connect_to_leader(bootstrap_host, bootstrap_port, topic, partition);
    if (fd == INVALID_SOCKET_T) {
      std::cerr << "Failed to resolve leader for " << topic << "[" << partition
                << "]. Retrying in 1s..." << std::endl;
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    std::cout << "Connected to leader for " << topic << "[" << partition << "]." << std::endl;

    proto::SubscribeReq sub = {};
    sub.topic_len = static_cast<uint8_t>(topic.size());
    std::memcpy(sub.topic, topic.data(), topic.size());
    sub.partition = swap_be16(partition);
    sub.start_offset = swap_be64(offset);

    if (!write_frame(fd, Op::SUBSCRIBE, &sub, sizeof(sub))) {
      platform::close_socket(fd);
      continue;
    }

    Frame resp;
    if (!read_frame(fd, resp)) {
      platform::close_socket(fd);
      continue;
    }

    if (resp.op == Op::ERR) {
      auto err = reinterpret_cast<const fell::proto::ErrorResp *>(resp.payload.data());
      if (err->code == static_cast<uint8_t>(ErrCode::NOT_LEADER)) {
        std::cout << "Leader moved, reconnecting..." << std::endl;
        platform::close_socket(fd);
        continue;
      }
      std::string msg(err->msg, err->msg_len);
      std::cerr << "SUBSCRIBE failed: " << msg << std::endl;
      platform::close_socket(fd);
      return 1;
    }

    if (resp.op == Op::ACK && resp.payload.size() >= sizeof(proto::AckResp)) {
      const auto *ack = reinterpret_cast<const proto::AckResp *>(resp.payload.data());
      uint64_t resolved_offset = swap_be64(ack->value);
      if (offset == 0xFFFFFFFFFFFFFFFFULL) {
        offset = resolved_offset;
      }
    }

    bool fetching = true;
    while (fetching && (count < 0 || read_count < count)) {
      proto::FetchReq fetch = {};
      fetch.topic_len = static_cast<uint8_t>(topic.size());
      std::memcpy(fetch.topic, topic.data(), topic.size());
      fetch.partition = swap_be16(partition);
      fetch.offset = swap_be64(offset);
      fetch.max_messages = swap_be16(1);

      if (!write_frame(fd, Op::FETCH, &fetch, sizeof(fetch)))
        break;

      Frame fetch_resp;
      if (!read_frame(fd, fetch_resp))
        break;

      if (fetch_resp.op == Op::ERR) {
        auto err = reinterpret_cast<const fell::proto::ErrorResp *>(fetch_resp.payload.data());
        if (err->code == static_cast<uint8_t>(ErrCode::NOT_LEADER)) {
          std::cout << "Leader moved, reconnecting..." << std::endl;
          fetching = false;
        } else {
          std::string msg(err->msg, err->msg_len);
          std::cerr << "FETCH failed: " << msg << std::endl;
          platform::close_socket(fd);
          return 1;
        }
      } else if (fetch_resp.op == Op::ACK) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      } else if (fetch_resp.op == Op::FETCH_RESPONSE) {
        if (fetch_resp.payload.size() < sizeof(proto::FetchResponseHeader))
          break;
        const auto *header =
            reinterpret_cast<const proto::FetchResponseHeader *>(fetch_resp.payload.data());
        uint64_t msg_offset = swap_be64(header->offset);
        uint64_t msg_timestamp = swap_be64(header->timestamp_ms);
        uint32_t payload_size = swap_be32(header->payload_size);

        if (fetch_resp.payload.size() != sizeof(proto::FetchResponseHeader) + payload_size)
          break;

        std::string msg_payload(reinterpret_cast<const char *>(fetch_resp.payload.data() +
                                                               sizeof(proto::FetchResponseHeader)),
                                payload_size);
        std::cout << "[" << msg_timestamp << "] Offset " << msg_offset << ": " << msg_payload
                  << std::endl;

        offset = msg_offset + 1;
        read_count++;
      } else {
        break;
      }
    }
    platform::close_socket(fd);
  }

  platform::platform_net_cleanup();
  return 0;
}
