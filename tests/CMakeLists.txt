add_executable(fell-tests main.cpp)
target_include_directories(fell-tests PRIVATE ../broker/include)
target_link_libraries(fell-tests PRIVATE broker_lib)
