#include "LsmTree.h"
#include <gtest/gtest.h>

using namespace lsm_storage_engine;

class LsmTreeTest : public ::testing::Test {
protected:
  std::filesystem::path wal_path_ = "lsm.wal";

  void TearDown() override {
    std::filesystem::remove(wal_path_);
  }
};

TEST_F(LsmTreeTest, GetReturnsNulloptForMissingKey) {
  LsmTree lsm;
  EXPECT_EQ(lsm.get("nonexistent"), std::nullopt);
}

TEST_F(LsmTreeTest, PutThenGet) {
  LsmTree lsm;
  lsm.put("foo", "bar");
  auto result = lsm.get("foo");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "bar");
}

TEST_F(LsmTreeTest, PutOverwritesExistingKey) {
  LsmTree lsm;
  lsm.put("key", "value1");
  lsm.put("key", "value2");
  EXPECT_EQ(*lsm.get("key"), "value2");
}

TEST_F(LsmTreeTest, MultipleKeyValuePairs) {
  LsmTree lsm;
  lsm.put("a", "1");
  lsm.put("b", "2");
  lsm.put("c", "3");

  EXPECT_EQ(*lsm.get("a"), "1");
  EXPECT_EQ(*lsm.get("b"), "2");
  EXPECT_EQ(*lsm.get("c"), "3");
}

TEST_F(LsmTreeTest, PutWritesToWal) {
  {
    LsmTree lsm;
    lsm.put("key", "value");
  }

  std::ifstream file(wal_path_);
  std::stringstream buffer;
  buffer << file.rdbuf();
  EXPECT_EQ(buffer.str(), "p key value\n");
}
