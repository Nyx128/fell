include(FetchContent)

# Ensure GTest doesn't cause warning/error failures
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)

# Suppress warnings in googletest
if(MSVC)
  set(gtest_disable_pthreads ON CACHE BOOL "" FORCE)
endif()

FetchContent_MakeAvailable(googletest)

# Discover all test files
file(GLOB_RECURSE TEST_SOURCES "*.cpp")

# Remove old main.cpp if it's still picked up during transition
list(REMOVE_ITEM TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")

add_executable(fell-tests ${TEST_SOURCES})

target_include_directories(fell-tests PRIVATE 
  ../broker/include 
  ../storage/include 
  ../platform/include
  ../replication/include
)

target_link_libraries(fell-tests PRIVATE 
  broker_lib 
  storage_lib 
  platform 
  gtest_main 
  gtest
)

# Ensure we disable Werror on the tests target just in case, or pass standard warnings
if(MSVC)
  target_compile_options(fell-tests PRIVATE /W3 /WX-)
else()
  target_compile_options(fell-tests PRIVATE -Wall -Wextra -Wno-error)
endif()

include(GoogleTest)
gtest_discover_tests(fell-tests)

