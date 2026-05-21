# Include header directories needed by targets
include_directories(${PROJECT_SOURCE_DIR}/platform/include)
include_directories(${PROJECT_SOURCE_DIR}/broker/include)

# bench-network target
add_executable(bench-network network_bench.cpp)
target_link_libraries(bench-network PRIVATE platform)
