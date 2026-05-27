# bench-swapping target
add_executable(bench-swapping swapping_bench.cpp)
target_link_libraries(bench-swapping PRIVATE platform benchmark::benchmark)

# bench-decoder target
add_executable(bench-decoder decoder_bench.cpp)
target_link_libraries(bench-decoder PRIVATE broker_lib platform benchmark::benchmark)

# bench-registry target
add_executable(bench-registry registry_bench.cpp)
target_link_libraries(bench-registry PRIVATE broker_lib platform benchmark::benchmark)
target_compile_definitions(bench-registry PRIVATE FELL_DEBUG_MUTEX)

# bench-storage target
add_executable(bench-storage storage_bench.cpp)
target_link_libraries(bench-storage PRIVATE storage_lib platform benchmark::benchmark)

# clang-specific benchmark warning suppression
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(bench-swapping PRIVATE
        -Wno-c2y-extensions
    )

    target_compile_options(bench-decoder PRIVATE
        -Wno-c2y-extensions
    )

    target_compile_options(bench-registry PRIVATE
        -Wno-c2y-extensions
    )
endif()