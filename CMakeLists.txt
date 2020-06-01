cmake_minimum_required(VERSION 3.13)
project(c3_ycsb)

set(CMAKE_CXX_STANDARD 14)

link_libraries(memcached)

add_executable(ycsb ycsb.cpp )

target_link_libraries(ycsb pthread)