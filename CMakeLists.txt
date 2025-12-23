
cmake_minimum_required(VERSION 3.15)
project(lsm-storage-engine)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to Release if not specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

include(FetchContent)

FetchContent_Declare(
xxHash
GIT_REPOSITORY https://github.com/Cyan4973/xxHash.git
GIT_TAG        v0.8.2
SOURCE_SUBDIR  cmake_unofficial
)
FetchContent_MakeAvailable(xxHash)

add_library(lsm_lib
  src/LsmTree.cc
  src/MemTable.cc
  src/Wal.cc
  src/SSTable.cc
)

target_include_directories(lsm_lib PUBLIC src)

target_link_libraries(lsm_lib PRIVATE xxHash::xxhash)

# Warnings and errors
target_compile_options(lsm_lib PRIVATE
  -Wall
  -Wextra
  -Wpedantic
  -Werror
  -Wshadow
  -Wconversion
  -Wsign-conversion
  -Wnull-dereference
)

# Debug-specific: no optimization, sanitizers
target_compile_options(lsm_lib PRIVATE
  $<$<CONFIG:Debug>:-O0 -g -fsanitize=address,undefined>
)
target_link_options(lsm_lib PRIVATE
  $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)

# Release-specific: optimize
target_compile_options(lsm_lib PRIVATE
  $<$<CONFIG:Release>:-O3>
)

add_executable(lsm src/main.cc)
target_link_options(lsm PRIVATE
  $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)
target_link_libraries(lsm lsm_lib)

add_subdirectory(test)
# add_subdirectory(benchmark)
