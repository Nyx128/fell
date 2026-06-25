#pragma once
#include "broker/protocol.hpp"
#include "platform/socket.hpp"
#include <cstring>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace fell {

  struct BrokerInfo {
    uint32_t id;
    std::string host;
    uint16_t client_port;
  };

  inline bool fetch_metadata(const std::string &bootstrap_host, uint16_t bootstrap_port,
                             const std::string &topic,
                             std::unordered_map<uint32_t, BrokerInfo> &brokers,
                             std::unordered_map<uint16_t, uint32_t> &partition_leaders) {
    socket_t fd = platform::connect_socket(bootstrap_host.c_str(), bootstrap_port);
    if (fd == INVALID_SOCKET_T)
      return false;

    proto::MetadataReq req = {};
    req.topic_len = static_cast<uint8_t>(std::min(topic.size(), size_t(255)));
    std::memcpy(req.topic, topic.data(), req.topic_len);

    uint8_t header[5];
    uint32_t req_len = static_cast<uint32_t>(sizeof(req));
    uint32_t frame_len = req_len + 1;
    uint32_t frame_len_be = ((frame_len >> 24) & 0xFF) | ((frame_len >> 8) & 0xFF00) |
                            ((frame_len << 8) & 0xFF0000) | ((frame_len << 24) & 0xFF000000);
    std::memcpy(header, &frame_len_be, 4);
    header[4] = static_cast<uint8_t>(Op::METADATA_REQ);

    platform::send_data(fd, reinterpret_cast<char *>(header), 5);
    platform::send_data(fd, reinterpret_cast<char *>(&req), sizeof(req));

    uint32_t resp_len_be;
    if (platform::recv_data(fd, reinterpret_cast<char *>(&resp_len_be), 4) != 4) {
      platform::close_socket(fd);
      return false;
    }
    uint32_t resp_len = ((resp_len_be >> 24) & 0xFF) | ((resp_len_be >> 8) & 0xFF00) |
                        ((resp_len_be << 8) & 0xFF0000) | ((resp_len_be << 24) & 0xFF000000);
    if (resp_len < 1) {
      platform::close_socket(fd);
      return false;
    }

    uint8_t op;
    if (platform::recv_data(fd, reinterpret_cast<char *>(&op), 1) != 1) {
      platform::close_socket(fd);
      return false;
    }

    std::vector<uint8_t> payload(resp_len - 1);
    size_t total = 0;
    while (total < payload.size()) {
      int n = platform::recv_data(fd, reinterpret_cast<char *>(payload.data() + total),
                                  payload.size() - total);
      if (n <= 0)
        break;
      total += n;
    }
    platform::close_socket(fd);

    if (op == static_cast<uint8_t>(Op::ERR)) {
      std::cerr << "Metadata request failed." << std::endl;
      return false;
    }

    if (op != static_cast<uint8_t>(Op::METADATA_RESP)) {
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

  inline socket_t connect_to_leader(const std::string &bootstrap_host, uint16_t bootstrap_port,
                                    const std::string &topic, uint16_t partition) {
    std::unordered_map<uint32_t, BrokerInfo> brokers;
    std::unordered_map<uint16_t, uint32_t> partition_leaders;

    if (!fetch_metadata(bootstrap_host, bootstrap_port, topic, brokers, partition_leaders)) {
      return INVALID_SOCKET_T;
    }

    auto it = partition_leaders.find(partition);
    if (it == partition_leaders.end()) {
      return INVALID_SOCKET_T;
    }

    uint32_t leader_id = it->second;
    auto b_it = brokers.find(leader_id);
    if (b_it == brokers.end()) {
      return INVALID_SOCKET_T;
    }

    return platform::connect_socket(b_it->second.host.c_str(), b_it->second.client_port);
  }

} // namespace fell
