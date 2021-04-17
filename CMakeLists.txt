cmake_minimum_required(VERSION 3.17)
project(LSM_Tree)


file(GLOB_RECURSE SOURCES "Src/*.*" "Inc/*.*")
include_directories(Inc)
set(CMAKE_CXX_STANDARD 14)

add_executable(LSM_Tree persistence.cc ${SOURCES})