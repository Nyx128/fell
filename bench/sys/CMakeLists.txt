# bench-network target
add_executable(bench-network network_bench.cpp)
target_link_libraries(bench-network PRIVATE platform broker_lib)
