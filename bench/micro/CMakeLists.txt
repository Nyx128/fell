# Include header directories needed by targets
include_directories(${PROJECT_SOURCE_DIR}/platform/include)
include_directories(${PROJECT_SOURCE_DIR}/broker/include)

# bench-swapping target
add_executable(bench-swapping swapping_bench.cpp)
target_link_libraries(bench-swapping PRIVATE platform benchmark::benchmark)

# bench-decoder target
add_executable(bench-decoder decoder_bench.cpp)
target_link_libraries(bench-decoder PRIVATE broker_lib platform benchmark::benchmark)

# bench-registry target
add_executable(bench-registry registry_bench.cpp)
target_link_libraries(bench-registry PRIVATE broker_lib platform benchmark::benchmark)
