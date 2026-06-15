#include "replication/cluster_config.hpp"
#include <fstream>
#include <inipp/inipp.h>
#include <iostream>

namespace fell::repl {

  ClusterConfig ClusterConfig::load(const std::string &path) {
    ClusterConfig cfg;
    cfg.broker_id = 0;
    cfg.data_dir = "fell-data";
    cfg.client_port = 7700;
    cfg.repl_port = 8700;

    cfg.replication_factor = 3;
    cfg.acks = 1;
    cfg.heartbeat_interval_ms = 500;
    cfg.heartbeat_timeout_ms = 1500;
    cfg.max_lag_messages = 1000;

    if (path.empty()) {
      cfg.peers.push_back({0, "127.0.0.1", 8700, 7700});
      cfg.peers.push_back({1, "127.0.0.1", 8701, 7701});
      cfg.peers.push_back({2, "127.0.0.1", 8702, 7702});
      return cfg;
    }

    inipp::Ini<char> ini;
    std::ifstream is(path);
    if (!is.is_open()) {
      throw std::runtime_error("Failed to open config file: " + path);
    }
    ini.parse(is);

    inipp::extract(ini.sections["broker"]["id"], cfg.broker_id);
    inipp::extract(ini.sections["broker"]["data_dir"], cfg.data_dir);
    inipp::extract(ini.sections["broker"]["client_port"], cfg.client_port);
    inipp::extract(ini.sections["broker"]["repl_port"], cfg.repl_port);

    inipp::extract(ini.sections["cluster"]["replication_factor"], cfg.replication_factor);
    inipp::extract(ini.sections["cluster"]["acks"], cfg.acks);
    inipp::extract(ini.sections["cluster"]["heartbeat_interval_ms"], cfg.heartbeat_interval_ms);
    inipp::extract(ini.sections["cluster"]["heartbeat_timeout_ms"], cfg.heartbeat_timeout_ms);
    inipp::extract(ini.sections["cluster"]["max_lag_messages"], cfg.max_lag_messages);

    for (const auto &peer : ini.sections["peers"]) {
      std::string peer_str = peer.second;
      size_t colon1 = peer_str.find(':');
      size_t colon2 = peer_str.find(':', colon1 + 1);
      if (colon1 != std::string::npos && colon2 != std::string::npos) {
        BrokerAddr addr;
        addr.broker_id = std::stoul(peer.first);
        addr.host = peer_str.substr(0, colon1);
        addr.repl_port = std::stoul(peer_str.substr(colon1 + 1, colon2 - colon1 - 1));
        addr.client_port = std::stoul(peer_str.substr(colon2 + 1));
        cfg.peers.push_back(addr);
      }
    }

    return cfg;
  }

} // namespace fell::repl
