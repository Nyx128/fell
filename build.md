# Building fell

Two supported build paths:
1. **CMake + Ninja + Clang** — recommended for development and CI (fast, cross-platform)
2. **Visual Studio 2022** — Windows-only, via the built-in CMake support

---

## Prerequisites

### Common (both paths)
| Tool | Version | Notes |
|------|---------|-------|
| CMake | ≥ 3.24 | Must be on `PATH` |
| Git | Any | Required by `FetchContent` to download google-benchmark at configure time |
| Internet access | — | First configure fetches benchmark v1.9.1 from GitHub |

### Path 1 — CMake + Ninja + Clang
| Tool | Version | Notes |
|------|---------|-------|
| Clang / clang++ | ≥ 15 | Must be on `PATH` |
| Ninja | Any | Must be on `PATH` |

> **Windows note:** Install LLVM from https://releases.llvm.org and Ninja from https://github.com/ninja-build/ninja/releases. Add both to `PATH`. No MSVC installation required for this path.

### Path 2 — Visual Studio 2022
| Tool | Version | Notes |
|------|---------|-------|
| Visual Studio 2022 | ≥ 17.x | Community edition is fine |
| "Desktop development with C++" workload | — | Must include CMake tools component |

---

## Path 1 — CMake + Ninja + Clang

All commands are run from the repo root (`c:\dev\fell` on Windows, the clone directory on Linux/macOS).

### 1. Configure both build trees

A helper script is provided:

```bat
scripts\config.bat
```

It is equivalent to running:

```bat
:: Debug
cmake -B build-debug -G Ninja ^
    -DCMAKE_BUILD_TYPE=Debug ^
    -DCMAKE_CXX_COMPILER=clang++ ^
    -DCMAKE_C_COMPILER=clang

:: Release
cmake -B build-release -G Ninja ^
    -DCMAKE_BUILD_TYPE=Release ^
    -DCMAKE_CXX_COMPILER=clang++ ^
    -DCMAKE_C_COMPILER=clang
```

> The first run downloads google-benchmark (~30 MB) into `build-<type>/_deps/`. Subsequent runs use the cache and are instant.

### 2. Build

```bat
:: Debug
cmake --build build-debug

:: Release
cmake --build build-release

:: Build a specific target only
cmake --build build-release --target felld
```

To use all CPU cores explicitly:

```bat
cmake --build build-release -- -j
```

### 3. Output locations

| Target | Debug path | Release path |
|--------|-----------|--------------|
| `felld` (broker daemon) | `build-debug\felld.exe` | `build-release\felld.exe` |
| `fell-produce` | `build-debug\fell-produce.exe` | `build-release\fell-produce.exe` |
| `fell-consume` | `build-debug\fell-consume.exe` | `build-release\fell-consume.exe` |
| `fell-tests` | `build-debug\tests\fell-tests.exe` | `build-release\tests\fell-tests.exe` |
| `bench-swapping` | `build-debug\bench\micro\` | `build-release\bench\micro\` |
| `bench-decoder` | `build-debug\bench\micro\` | `build-release\bench\micro\` |
| `bench-registry` | `build-debug\bench\micro\` | `build-release\bench\micro\` |
| `bench-network` | `build-debug\bench\sys\` | `build-release\bench\sys\` |

### 4. Rebuild from scratch

Delete the build directory and re-run configure:

```bat
rmdir /s /q build-debug
cmake -B build-debug -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang
cmake --build build-debug
```

---

## Path 2 — Visual Studio 2022

VS 2022 uses its own embedded CMake integration. It reads `CMakeLists.txt` directly and manages build directories under `out\build\`.

### 1. Open the project

**File → Open → Folder…** and select the repo root (`c:\dev\fell`). VS will detect `CMakeLists.txt` automatically.

### 2. Configure

VS auto-configures on open. To manually trigger or pick a configuration:

- **Project → Configure fell** — runs CMake with the active preset
- **Project → CMake Settings for fell** (`CMakeSettings.json`) — choose compiler, build type, etc.

The default configurations installed by VS are `x64-Debug` and `x64-Release`, both using MSVC (`cl.exe`).

> **Important:** The `-Wno-c2y-extensions` flag used by the benchmark build is MSVC-incompatible and is now guarded in `bench/CMakeLists.txt`. If you see `D8021: invalid numeric argument` errors, delete the cache (step below) and reconfigure.

### 3. Build

Use the standard build toolbar or:

- **Build → Build All** (`Ctrl+Shift+B`)
- **Build → Build \<target\>** to build a single target

### 4. Selecting a target to run

Use the startup item dropdown (next to the green play button) to select `felld.exe`, `fell-produce.exe`, `fell-consume.exe`, etc.

### 5. Delete cache and reconfigure

If the configure step is stale (e.g. after changing `CMakeLists.txt` significantly):

**Project → Delete Cache and Reconfigure**

This wipes `out\build\<config>\` and reruns CMake from scratch, including re-downloading dependencies.

---

## CMake options reference

| Option | Default | Description |
|--------|---------|-------------|
| `FELL_BUILD_BENCHMARKS` | `ON` | Build the benchmark suite. Set to `OFF` to skip. |

Example — build without benchmarks:

```bat
cmake -B build-release -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_C_COMPILER=clang -DFELL_BUILD_BENCHMARKS=OFF
```
