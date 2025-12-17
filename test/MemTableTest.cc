#include "MemTable.h"
#include <gtest/gtest.h>

using namespace lsm_storage_engine;

TEST(MemTableTest, GetReturnsNulloptForMissingKey) {
  MemTable table;
  EXPECT_EQ(table.get("nonexistent"), std::nullopt);
}

TEST(MemTableTest, PutThenGet) {
  MemTable table;
  table.put("key1", "value1");
  auto result = table.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "value1");
}

TEST(MemTableTest, PutOverwritesExistingKey) {
  MemTable table;
  table.put("key1", "value1");
  table.put("key1", "value2");
  auto result = table.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "value2");
}

TEST(MemTableTest, MultipleKeys) {
  MemTable table;
  table.put("a", "1");
  table.put("b", "2");
  table.put("c", "3");

  EXPECT_EQ(*table.get("a"), "1");
  EXPECT_EQ(*table.get("b"), "2");
  EXPECT_EQ(*table.get("c"), "3");
}

TEST(MemTableTest, EmptyKeyAndValue) {
  MemTable table;
  table.put("", "empty_key");
  table.put("empty_value", "");

  EXPECT_EQ(*table.get(""), "empty_key");
  EXPECT_EQ(*table.get("empty_value"), "");
}
