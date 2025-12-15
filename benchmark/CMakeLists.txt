include(FetchContent)
FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
)
FetchContent_MakeAvailable(googlebenchmark)

add_executable(lsm_bench
)
target_link_libraries(lsm_bench lsm_lib benchmark::benchmark_main)

