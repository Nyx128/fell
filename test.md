# Testing fell

The test suite is a single self-contained executable (`fell-tests`) that uses `assert`-based checks — no external test framework required. Tests cover `FrameDecoder`, `TopicRegistry`, and `RequestHandler` in-process without a running broker.

---

## Prerequisites

A configured and built Debug or Release tree. See [build.md](build.md) if you haven't done this yet.

> Run tests against the **Debug** build by default. `NDEBUG` is explicitly undefined in `tests/main.cpp` so `assert` is always active, even in Release builds of the test executable.

---

## Path 1 — CMake + Ninja + Clang

### 1. Build the test executable

From the repo root:

```bat
cmake --build build-debug --target fell-tests
```

### 2. Run the tests

```bat
build-debug\tests\fell-tests.exe
```

Expected output:

```
[Test] Running FrameDecoder tests...
[Test] FrameDecoder tests passed!
[Test] Running TopicRegistry tests...
[Test] TopicRegistry tests passed!
[Test] Running RequestHandler tests...
[Test] RequestHandler tests passed!
All unit tests completed successfully!
```

A non-zero exit code means at least one `assert` fired. The failing assertion and source location are printed to stderr by the C runtime.

### 3. Run as part of a full build

Build everything at once and then run:

```bat
cmake --build build-debug
build-debug\tests\fell-tests.exe
```

---

## Path 2 — Visual Studio 2022

### 1. Build the test target

In the startup item dropdown (next to the play button), select **fell-tests.exe** and press **Build → Build All**, or right-click the `tests/CMakeLists.txt` in Solution Explorer and choose **Build**.

### 2. Run the tests

#### Option A — Run from VS

Select `fell-tests.exe` in the startup item dropdown and press **Ctrl+F5** (run without debugger) or **F5** (with debugger). Output appears in the **Output** window.

#### Option B — Run from a terminal

Locate the binary under `out\build\x64-Debug\tests\fell-tests.exe` and run it:

```bat
out\build\x64-Debug\tests\fell-tests.exe
```

---

## What is tested

| Suite | File | What it covers |
|-------|------|----------------|
| `test_frame_decoder` | `tests/main.cpp` | Complete frames, fragmented multi-chunk input, multiple frames in one buffer |
| `test_topic_registry` | `tests/main.cpp` | Topic creation, duplicate rejection, partition lookup, append/fetch |
| `test_request_handler` | `tests/main.cpp` | Malformed-payload rejection, CREATE_TOPIC, duplicate CREATE_TOPIC, PUBLISH with ACK |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `Assertion failed` printed and exit code non-zero | A test assertion failed | Read the file/line in the message and check recent changes to the relevant component |
| Binary not found | Test target wasn't built | Run `cmake --build build-debug --target fell-tests` |
| Compiler errors during build | Missing include or type mismatch | Rebuild after cleaning: `cmake --build build-debug --clean-first` |
