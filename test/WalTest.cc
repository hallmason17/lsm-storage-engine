#include "Wal.h"
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

using namespace lsm_storage_engine;

class WalTest : public ::testing::Test {
protected:
  std::filesystem::path test_path_ = "test_wal.log";

  void TearDown() override { std::filesystem::remove(test_path_); }
};

TEST_F(WalTest, ConstructorCreatesFile) {
  {
    Wal wal(test_path_);
  }
  EXPECT_TRUE(std::filesystem::exists(test_path_));
}

TEST_F(WalTest, PathReturnsCorrectPath) {
  Wal wal(test_path_);
  EXPECT_EQ(wal.path(), test_path_);
}
