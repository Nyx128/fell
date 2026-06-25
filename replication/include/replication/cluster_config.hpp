#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace fell::repl {

  /// Address of a broker peer, carrying both the replication and client-facing ports.
  struct BrokerAddr {
    uint32_t broker_id;   ///< Unique broker identifier (assigned in the INI config).
    std::string host;     ///< Hostname or IP address.
    uint16_t repl_port;   ///< Port used for inter-broker replication traffic.
    uint16_t client_port; ///< Port exposed to producers and consumers.
  };

  /**
   * @struct ClusterConfig
   * @brief Runtime configuration for a broker node.
   *
   * Loaded from an INI file at startup.  A single `ClusterConfig` fully describes
   * both the local broker and the wider peer topology, so every subsystem
   * (replication, request routing, leader election) can derive what it needs from
   * one authoritative source.
   */
  struct ClusterConfig {
    // --- Local broker identity ---
    uint32_t broker_id;   ///< Unique id of this broker within the cluster.
    std::string data_dir; ///< Root directory for persistent segment storage.
    uint16_t client_port; ///< TCP port for client (producer/consumer) connections.
    uint16_t repl_port;   ///< TCP port for replication connections from followers.

    // --- Cluster topology ---
    std::vector<BrokerAddr> peers; ///< All brokers in the cluster, including this one.

    // --- Replication policy ---
    uint32_t replication_factor;    ///< Number of replicas each partition must have.
    int acks;                       ///< Acknowledgement semantics: 1 = leader only, -1 = all ISR.
    uint32_t heartbeat_interval_ms; ///< How often followers send heartbeats to the leader.
    uint32_t heartbeat_timeout_ms;  ///< Milliseconds without a heartbeat before a follower is
                                    ///< evicted from the ISR.
    uint64_t
        max_lag_messages; ///< Maximum allowed offset lag before a replica is removed from the ISR.

    /**
     * @brief Loads a `ClusterConfig` from an INI file.
     *
     * If @p path is empty a sensible single-node development default is returned
     * (broker 0, `fell-data`, ports 7700/8700).
     *
     * @param path Filesystem path to the `.ini` configuration file.
     * @throws std::runtime_error if @p path is non-empty but cannot be opened.
     */
    static ClusterConfig load(const std::string &path);
  };

} // namespace fell::repl
