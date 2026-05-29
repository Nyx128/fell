cmake_minimum_required(VERSION 3.24)
project(fell LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # for clangd / IDE support

include(cmake/CompilerFlags.cmake)

option(FELL_ENABLE_STORAGE_TIMING "Collect internal storage timing counters" OFF)

#platform layer
add_library(platform STATIC)
target_include_directories(platform PUBLIC platform/include)

if(WIN32)
    target_sources(platform PRIVATE
        platform/wsapoll_poller.cpp
        platform/socket_win.cpp
        platform/file_win.cpp
    )
    target_link_libraries(platform PUBLIC ws2_32)
    target_compile_definitions(platform PUBLIC FELL_PLATFORM_WINDOWS)
    if(MSVC)
        target_compile_options(platform PRIVATE /WX)
    else()
        target_compile_options(platform PRIVATE -Werror)
    endif()
else()
    target_sources(platform PRIVATE
        platform/epoll_poller.cpp
        platform/socket_posix.cpp
        platform/file_posix.cpp
    )
    target_compile_definitions(platform PUBLIC FELL_PLATFORM_LINUX)
    if(MSVC)
        target_compile_options(platform PRIVATE /WX)
    else()
        target_compile_options(platform PRIVATE -Werror)
    endif()
endif()

#storage library
add_library(storage_lib STATIC
    storage/segment_writer.cpp
    storage/segment_reader.cpp
    storage/offset_index.cpp
    storage/log_recovery.cpp
    storage/partition_store.cpp
)
target_include_directories(storage_lib PUBLIC storage/include)

target_compile_definitions(storage_lib PUBLIC
    $<$<CONFIG:Debug>:FELL_DEBUG_MUTEX>
    $<$<BOOL:${FELL_ENABLE_STORAGE_TIMING}>:FELL_ENABLE_STORAGE_TIMING>
)

target_link_libraries(storage_lib PUBLIC platform)
if(MSVC)
    target_compile_options(storage_lib PRIVATE /WX)
else()
    target_compile_options(storage_lib PRIVATE -Werror)
endif()

#broker library
add_library(broker_lib STATIC
    broker/acceptor.cpp
    broker/connection_manager.cpp
    broker/frame_decoder.cpp
    broker/request_handler.cpp
    broker/topic_registry.cpp
    broker/broker.cpp
)
target_include_directories(broker_lib PUBLIC broker/include)
target_link_libraries(broker_lib PUBLIC platform storage_lib)
if(MSVC)
    target_compile_options(broker_lib PRIVATE /WX)
else()
    target_compile_options(broker_lib PRIVATE -Werror)
endif()

#executables
add_executable(felld felld/main.cpp)
target_link_libraries(felld PRIVATE broker_lib)
if(MSVC)
    target_compile_options(felld PRIVATE /WX)
else()
    target_compile_options(felld PRIVATE -Werror)
endif()

add_executable(fell-produce cli/producer_cli.cpp)
target_include_directories(fell-produce PRIVATE broker/include)
target_link_libraries(fell-produce PRIVATE platform)
if(MSVC)
    target_compile_options(fell-produce PRIVATE /WX)
else()
    target_compile_options(fell-produce PRIVATE -Werror)
endif()

add_executable(fell-consume cli/consumer_cli.cpp)
target_include_directories(fell-consume PRIVATE broker/include)
target_link_libraries(fell-consume PRIVATE platform)
if(MSVC)
    target_compile_options(fell-consume PRIVATE /WX)
else()
    target_compile_options(fell-consume PRIVATE -Werror)
endif()

enable_testing()
add_subdirectory(tests)

# Benchmarks
option(FELL_BUILD_BENCHMARKS "Build the performance benchmark suite" ON)
if(FELL_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
