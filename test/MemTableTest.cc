#include "MemTable.h"
#include "Constants.h"
#include "StorageError.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

using namespace lsm_storage_engine;

class MemTableFlushTest : public ::testing::Test {
protected:
  std::filesystem::path test_path_ = "test_memtable_flush.sst";

  void TearDown() override { std::filesystem::remove(test_path_); }
};

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

TEST(MemTableTest, PutIncrementsSize) {
  MemTable table;
  table.put("key1", "value1");

  EXPECT_EQ(table.size(),
            std::string("key1").size() + std::string("value1").size());
}

TEST(MemTableTest, PutExistingKeyUpdatesSize) {
  MemTable table;
  table.put("key1", "value1");

  EXPECT_EQ(table.size(),
            std::string("key1").size() + std::string("value1").size());

  table.put("key1", "1");
  EXPECT_EQ(table.size(), std::string("key1").size() + std::string("1").size());
}

// --- should_flush() tests ---

TEST(MemTableTest, ShouldFlushReturnsFalseWhenEmpty) {
  MemTable table;
  EXPECT_FALSE(table.should_flush());
}

TEST(MemTableTest, ShouldFlushReturnsFalseWhenBelowThreshold) {
  MemTable table;
  table.put("k", "v");
  EXPECT_FALSE(table.should_flush());
}

TEST(MemTableTest, ShouldFlushReturnsTrueWhenAboveThreshold) {
  MemTable table;
  std::string large_value(lsm_constants::kMemTableFlushThreshold + 4, 'x');
  table.put("key", large_value);
  EXPECT_TRUE(table.should_flush());
}

// --- clear() tests ---

TEST(MemTableTest, ClearRemovesAllEntries) {
  MemTable table;
  table.put("a", "1");
  table.put("b", "2");
  table.clear();

  EXPECT_EQ(table.get("a"), std::nullopt);
  EXPECT_EQ(table.get("b"), std::nullopt);
}

TEST(MemTableTest, ClearResetsSize) {
  MemTable table;
  table.put("key", "value");
  EXPECT_GT(table.size(), 0);

  table.clear();
  EXPECT_EQ(table.size(), 0);
}

TEST(MemTableTest, ClearResetsShouldFlush) {
  MemTable table;
  std::string large_value(300, 'x');
  table.put("key", large_value);
  EXPECT_TRUE(table.should_flush());

  table.clear();
  EXPECT_FALSE(table.should_flush());
}

// --- flush_to_disk() tests ---

TEST_F(MemTableFlushTest, FlushToDiskSucceeds) {
  MemTable table;
  table.put("key1", "value1");

  auto result = table.flush_to_disk(test_path_);
  EXPECT_TRUE(result.has_value());
  EXPECT_TRUE(std::filesystem::exists(test_path_));
}

TEST_F(MemTableFlushTest, FlushToDiskCreatesFile) {
  MemTable table;
  table.put("foo", "bar");

  auto result = table.flush_to_disk(test_path_);
  ASSERT_TRUE(result.has_value());

  std::ifstream file(test_path_, std::ios::binary);
  EXPECT_TRUE(file.good());
}

TEST_F(MemTableFlushTest, FlushToDiskReturnsErrorForInvalidPath) {
  MemTable table;
  table.put("key", "value");

  std::filesystem::path invalid_path = "/nonexistent/directory/file.sst";
  auto result = table.flush_to_disk(invalid_path);

  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().kind, StorageError::Kind::FileOpen);
}

TEST_F(MemTableFlushTest, FlushEmptyTableSucceeds) {
  MemTable table;
  auto result = table.flush_to_disk(test_path_);
  EXPECT_TRUE(result.has_value());
}
