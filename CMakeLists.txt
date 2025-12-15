cmake_minimum_required(VERSION 3.15)
project(lsm-tree)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(lsm_lib
    src/LsmTree.cpp
)

target_include_directories(lsm_lib PUBLIC src)

add_executable(lsm src/main.cpp)
target_link_libraries(lsm lsm_lib)


# add_subdirectory(test)
# add_subdirectory(benchmark)
