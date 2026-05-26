# Benchmarking fell

Two benchmark suites:

| Suite | Executable | What it measures |
|-------|-----------|-----------------|
| **Micro** | `bench-swapping`, `bench-decoder`, `bench-registry` | In-process throughput of byte-swap helpers, frame decoder, and topic registry via Google Benchmark |
| **Network** | `bench-network` | End-to-end publish throughput and latency over a real TCP connection to a running `felld` instance |

> **Always benchmark against Release builds.** Debug builds have no optimizations and their numbers are meaningless.

---

## Prerequisites

- A configured and built Release tree. See [build.md](build.md).
- For the network benchmark: `felld` must be running and reachable.
- Internet access on the first configure (downloads google-benchmark v1.9.1 via `FetchContent`).

---

## Path 1 — CMake + Ninja + Clang

### Step 1 — Build the benchmark targets

```bat
cmake --build build-release --target bench-swapping bench-decoder bench-registry bench-network
```

Or build everything at once:

```bat
cmake --build build-release
```

### Step 2 — Micro benchmarks (no broker needed)

Run any micro benchmark directly:

```bat
build-release\bench\micro\bench-swapping.exe
build-release\bench\micro\bench-decoder.exe
build-release\bench\micro\bench-registry.exe
```

#### Useful Google Benchmark flags

```bat
:: Run only benchmarks matching a pattern
bench-registry.exe --benchmark_filter=Append

:: Set the minimum time per benchmark (default 1s)
bench-registry.exe --benchmark_min_time=3s

:: Output as JSON (for scripting or CI)
bench-registry.exe --benchmark_format=json --benchmark_out=results.json

:: List available benchmarks without running them
bench-registry.exe --benchmark_list_tests
```

#### Example output

```
Running bench-registry.exe
Run on (16 X 3200 MHz CPU s)
-----------------------------------------------------------------------
Benchmark                             Time             CPU   Iterations
-----------------------------------------------------------------------
BM_TopicRegistry_Append/1           83.4 ns         83.1 ns      8412893
BM_TopicRegistry_Append/8           95.2 ns         94.9 ns      7361042
BM_TopicRegistry_Fetch/1            51.3 ns         51.1 ns     13690021
```

### Step 3 — Network benchmark

The network benchmark measures real TCP round-trip throughput. It requires `felld` to be running first.

#### 3a. Start the broker

In a separate terminal:

```bat
build-release\felld.exe
```

The broker listens on port **7700** by default.

#### 3b. Run the benchmark

```bat
build-release\bench\sys\bench-network.exe [options]
```

#### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | Broker hostname or IP |
| `--port` | `7700` | Broker port |
| `--ops` | `100000` | Total publish operations |
| `--threads` | `4` | Number of concurrent client threads |
| `--payload-size` | `256` | Message payload size in bytes |
| `--pipeline` | `1` | Pipeline window size (`1` = synchronous, `>1` = pipelined) |

#### Examples

```bat
:: Synchronous, 100k ops, 4 threads, 256-byte payload
bench-network.exe --ops 100000 --threads 4 --payload-size 256 --pipeline 1

:: Pipelined (window=32), 200k ops, 8 threads, 1KB payload
bench-network.exe --ops 200000 --threads 8 --payload-size 1024 --pipeline 32
```

#### Example output

```
[fell-bench] network
  host:     127.0.0.1:7700
  ops:      100000
  threads:  4
  payload:  256 bytes
  pipeline: 1 (synchronous)

=================== RESULTS ===================
  Duration:       2.14 s
  Completed ops:  100000
  Throughput:     46728 ops/sec
  Bandwidth:      12.3 MB/sec

  Latency (per-op RTT, µs):
    min:   47.2
    mean:  85.4
    p50:   78.1
    p90:   131.6
    p99:   298.4
    p99.9: 612.0
    max:   1847.3
===============================================
```

> **Latency** is only reported in synchronous mode (`--pipeline 1`). In pipelined mode the window time cannot be attributed to individual ops, so only throughput is shown.

#### 3c. Sweep script

A helper script runs a matrix of configurations and prints a throughput table:

```bat
scripts\network_bench_table.bat
```

It sweeps:
- Pipeline sizes: 1, 4, 8, 16, 32, 64, 128 (1 thread)
- Thread counts: 1, 2, 4, 8, 16 (pipeline=32)
- Payload sizes: 64, 256, 1024, 8192, 65536 bytes (4 threads, pipeline=32)

The broker must be running before executing this script. Edit the `BENCH`, `OPS`, and `PAYLOAD` variables at the top of the script to adjust paths or parameters.

---

## Path 2 — Visual Studio 2022

### Building micro benchmarks

In the Solution Explorer, right-click `bench/micro/CMakeLists.txt` → **Build**, or use **Build → Build All**.

Alternatively select individual targets (`bench-swapping.exe`, `bench-decoder.exe`, `bench-registry.exe`) from the startup item dropdown and build.

### Running micro benchmarks from VS

Select the desired benchmark in the startup item dropdown and press **Ctrl+F5**. Output appears in the terminal window that opens. Pass flags via **Debug → Command Arguments** in the target's launch settings.

### Running the network benchmark from VS

1. Start `felld.exe` first (select it from the startup dropdown, press **Ctrl+F5**).
2. Open a separate **Developer PowerShell** or **cmd** and run:

```bat
out\build\x64-Release\bench\sys\bench-network.exe --ops 100000 --threads 4
```

> The network benchmark is a multi-threaded console program; running it inside the VS debugger is not recommended.

---

## Choosing benchmark parameters

| Goal | Recommended settings |
|------|---------------------|
| Measure raw single-core throughput | `--threads 1 --pipeline 1` |
| Saturate the broker (max ops/sec) | `--threads 8 --pipeline 32` or higher |
| Measure per-op latency (RTT) | `--pipeline 1` (any thread count) |
| Test large message handling | `--payload-size 65536 --pipeline 1` |
| Stress test over time | `--ops 1000000 --threads 8 --pipeline 64` |

## Notes

- Always close other applications that use significant CPU or network before benchmarking.
- Run multiple times and take the median; first-run results may include OS warm-up effects.
- The micro benchmarks run entirely in-process — no broker is needed and results are deterministic.
- The network benchmark creates one TCP connection per thread. Each thread independently publishes to a dedicated partition on the `bench` topic (created by thread 0 before the barrier).
