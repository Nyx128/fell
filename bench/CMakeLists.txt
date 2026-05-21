include(FetchContent)

# Set configuration options for Google Benchmark before bringing it in
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Disable benchmark tests" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Disable gtest in benchmark" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "Disable installation of benchmark" FORCE)

# 1. Fetch Google Benchmark
FetchContent_Declare(
  google_benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG        v1.8.3
)
FetchContent_MakeAvailable(google_benchmark)

# 2. Add subdirectories for modular benchmarks
add_subdirectory(micro)
add_subdirectory(sys)
