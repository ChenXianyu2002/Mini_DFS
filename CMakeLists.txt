cmake_minimum_required(VERSION 3.16)
project(Mini_DFS)

set(CMAKE_CXX_STANDARD 20)

add_executable(Mini_DFS main.cpp nameServer.h dataServer.h nameServer.cpp utils.h utils.cpp dataServer.cpp)

TARGET_LINK_LIBRARIES(Mini_DFS pthread)