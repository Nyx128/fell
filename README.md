# Fell

Fell is a high-performance, lightweight, and clustered distributed message broker written entirely in modern C++ (C++17). Designed to be a leaner, bare-metal alternative to traditional JVM-based message queues, Fell provides durable, partitioned append-only storage with strong replication semantics, while operating within an extremely small resource footprint.

## Design Philosophy

Fell was built under the premise that message brokering is fundamentally an I/O and network bound problem, and that modern hardware capabilities can be fully leveraged without heavy runtimes or garbage collection pauses.

A single Fell daemon (`felld`) operates with remarkable memory efficiency:
* **~2 MB** memory footprint when idle.
* **4-5 MB** memory footprint under maximum sustained throughput.

By avoiding unbounded allocations and leveraging memory-mapped I/O and zero-copy techniques, Fell maximizes raw throughput and minimizes tail latency.

## Architecture Deep Dive

The system is composed of several core subsystems designed for efficiency and high concurrency.

### Storage Engine
Fell uses an append-only log architecture segmented by topics and partitions. 
* **Segments and Indices:** Each partition consists of sequential log segments and corresponding index files. When a log segment reaches a predefined size, it is sealed, and a new segment is rolled out. The index files map sequence offsets to physical byte locations within the segment, allowing `O(1)` offset lookups.
* **I/O Efficiency:** Storage is heavily optimized for sequential disk I/O. Records are batched in memory and flushed aggressively.

### Network Reactor
The communication layer is a non-blocking, event-driven network reactor built on system polling (epoll, kqueue, or IOCP, depending on the underlying platform). 
* It can handle thousands of concurrent producer and consumer TCP connections with minimal thread contention.
* Protocol framing is strictly binary and big-endian, avoiding the overhead of JSON or string parsing on the critical path.

### Clustering & Replication
Fell guarantees fault tolerance through a strict leader-follower replication protocol.
* **Topic Partitions:** Topics are split into independent partitions. Each partition has a designated leader broker and a set of follower brokers.
* **Replication Streams:** Replication is handled via dedicated inter-broker TCP connections, completely decoupling internal cluster synchronization traffic from client-facing produce and consume operations.
* **Log Fetching:** Followers asynchronously fetch logs from the leader starting from their last committed offset, continuously streaming `REPLICA_SYNC` frames.

### Quorum, ISR, and Durability
Fell ensures data consistency through an In-Sync Replicas (ISR) model.
* **ISR Management:** Leaders track the progress of every follower. If a follower falls behind by more than `max_lag_messages` or fails to send a heartbeat within `heartbeat_timeout_ms`, it is evicted from the ISR.
* **Durability Semantics (`acks`):** Producers can tune their durability requirements. 
  * `acks=1`: The leader acknowledges the write as soon as it commits to its local disk.
  * `acks=all` (or `-1`): The leader defers the acknowledgement (`PendingAck`) until every replica currently in the ISR has acknowledged receiving and committing the record.

### Metadata Routing
Clients are completely cluster-aware. A client can connect to any bootstrap broker in the cluster to issue a `METADATA_REQ`. The broker responds with the current cluster topology, mapping partitions to their active leader IDs. Clients then establish direct sockets to the appropriate leaders to produce or consume data, eliminating extra network hops.

## Building Fell

Fell utilizes CMake as its build system. A C++17 compatible compiler (GCC, Clang, or MSVC) is required.

```bash
# Generate build files
cmake -B build-debug -DCMAKE_BUILD_TYPE=Debug

# Compile the broker, CLI tools, and benchmarks
cmake --build build-debug --target felld fell-produce fell-consume bench-network
```

## Running a Cluster

Fell clusters are configured using standard INI files. Each node requires a unique `broker_id`, dedicated ports for client and replication traffic, and a comprehensive list of peer addresses.

Example configuration (`b0.ini`):
```ini
[cluster]
broker_id=0
data_dir=fell-data-0
client_port=7700
repl_port=8700
peers=0:127.0.0.1:8700:7700,1:127.0.0.1:8701:7701,2:127.0.0.1:8702:7702

[replication]
replication_factor=3
acks=1
heartbeat_interval_ms=500
heartbeat_timeout_ms=2000
max_lag_messages=1000
```

Start the daemon:
```bash
./build-debug/felld --config b0.ini
```

To run a complete cluster locally, start multiple instances pointing to different configuration files containing the same peer list but different respective `broker_id` and `data_dir` settings.

## Command Line Interface (CLI)

Fell provides dedicated binary tools for interacting with the cluster. The CLI tools automatically handle metadata fetching, parsing the routing tables, and connecting to the appropriate partition leaders.

### Producer

Publish messages to a specific topic. If the topic does not exist, the `--create` flag will initialize it across the cluster based on the default `replication_factor`.

```bash
# Create a topic with 4 partitions and publish a single message
./build-debug/fell-produce --host 127.0.0.1 --port 7700 --topic metrics --create --partitions 4 --message "cpu_usage: 45%"

# Publish multiple messages using a routing key.
# Fell uses an FNV-1a hash of the routing key to ensure all messages 
# with the same key are deterministically routed to the same partition leader.
./build-debug/fell-produce --host 127.0.0.1 --port 7700 --topic metrics --key "server-1" --message "cpu_usage: 50%" --count 100
```

### Consumer

Consume messages from a specific partition starting at a given exact offset.

```bash
# Consume 10 messages from partition 0 starting at offset 0
./build-debug/fell-consume --host 127.0.0.1 --port 7700 --topic metrics --partition 0 --offset 0 --count 10
```

## Benchmarking

Fell includes a robust, multi-threaded benchmarking suite designed to test various internal and external subsystems under extreme load. 

The `bench-network` tool tests the full end-to-end throughput of the network reactor and storage layer against a running cluster. It simulates highly concurrent producers or consumers to saturate the broker.

```bash
# Run a producer benchmark against the external cluster:
# - 100,000 operations total
# - 256-byte payload sizes
# - Distributed across 4 concurrent threads
./build-debug/bench-network --host 127.0.0.1 --leader-port 7700 --scenario producer --topic bench --partitions 1 --ops 100000 --threads 4
```

Micro-benchmarks are also compiled via CMake and are available for testing targeted internal components:
* `storage_bench`: Tests raw disk I/O throughput for the append-only log, bypassing the network.
* `decoder_bench`: Tests the binary protocol framing and serialization performance.
* `registry_bench`: Tests the thread-safe concurrent partition registry lookups under heavy lock contention.
* `replication_bench`: Validates the internal replication state machine, ISR tracking, and quorum release mechanisms.
