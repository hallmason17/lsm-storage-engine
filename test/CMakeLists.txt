include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(lsm_test
    MemTableTest.cc
    WalTest.cc
    LsmTreeTest.cc
    SSTableTest.cc
)
target_link_options(lsm_test PRIVATE
  $<$<CONFIG:Debug>:-fsanitize=address,undefined>
)
target_link_libraries(lsm_test lsm_lib xxHash::xxhash gtest_main)

