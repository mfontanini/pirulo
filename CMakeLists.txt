cmake_minimum_required(VERSION 2.8.1)
project(pirulo)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")
add_subdirectory(src)
add_subdirectory(executables)
