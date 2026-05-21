include(FetchContent)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)

# IMPORTANT
set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_PEDANTIC OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  google_benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.9.1
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-c2y-extensions")
FetchContent_MakeAvailable(google_benchmark)

add_subdirectory(micro)
add_subdirectory(sys)