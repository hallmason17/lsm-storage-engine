#include "SSTable.h"
#include "MemTable.h"
#include <filesystem>
#include <gtest/gtest.h>

using namespace lsm_storage_engine;

class SSTableTest : public ::testing::Test {
protected:
  std::filesystem::path test_path_ = "test_sstable.sst";

  void SetUp() override {
    // Clean up any leftover files from previous runs
    std::filesystem::remove(test_path_);
    std::filesystem::remove("lsm.meta");
  }

  void TearDown() override {
    std::filesystem::remove(test_path_);
    std::filesystem::remove("lsm.meta");
  }

  // Helper to write test data to SSTable via MemTable flush
  void write_test_data(
      const std::vector<std::pair<std::string, std::string>> &entries) {
    auto sst = SSTable::open(test_path_).value();
    MemTable mem;
    for (const auto &[key, value] : entries) {
      mem.put(key, value);
    }
    auto result = mem.flush_to_sst(sst);
    ASSERT_TRUE(result.has_value()) << "Failed to flush memtable to disk";
  }
};

// --- Basic get() tests ---

TEST_F(SSTableTest, GetReturnsNulloptForMissingKey) {
  write_test_data({{"key1", "value1"}});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get("nonexistent");
  ASSERT_TRUE(result.has_value()) << "get() returned error";
  EXPECT_FALSE(result->has_value());
}

TEST_F(SSTableTest, GetReturnsValueForExistingKey) {
  write_test_data({{"key1", "value1"}});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get("key1");
  ASSERT_TRUE(result.has_value()) << "get() returned error";
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, "value1");
}

TEST_F(SSTableTest, GetMultipleKeys) {
  write_test_data(
      {{"apple", "red"}, {"banana", "yellow"}, {"grape", "purple"}});
  SSTable sst = SSTable::open(test_path_).value();

  auto r1 = sst.get("apple");
  ASSERT_TRUE(r1.has_value() && r1->has_value());
  EXPECT_EQ(**r1, "red");

  auto r2 = sst.get("banana");
  ASSERT_TRUE(r2.has_value() && r2->has_value());
  EXPECT_EQ(**r2, "yellow");

  auto r3 = sst.get("grape");
  ASSERT_TRUE(r3.has_value() && r3->has_value());
  EXPECT_EQ(**r3, "purple");
}

TEST_F(SSTableTest, GetEmptySSTable) {
  write_test_data({});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get("anykey");
  ASSERT_TRUE(result.has_value()) << "get() returned error";
  EXPECT_FALSE(result->has_value());
}

TEST_F(SSTableTest, GetEmptyKey) {
  write_test_data({{"", "empty_key_value"}});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get("");
  ASSERT_TRUE(result.has_value()) << "get() returned error";
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, "empty_key_value");
}

TEST_F(SSTableTest, GetEmptyValue) {
  write_test_data({{"key_with_empty_value", ""}});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get("key_with_empty_value");
  ASSERT_TRUE(result.has_value()) << "get() returned error";
  ASSERT_TRUE(result->has_value());
  EXPECT_EQ(**result, "");
}

TEST_F(SSTableTest, GetCanBeCalledMultipleTimes) {
  write_test_data({{"key1", "value1"}, {"key2", "value2"}});
  SSTable sst = SSTable::open(test_path_).value();

  // Call get multiple times to ensure lseek resets properly
  for (int i = 0; i < 3; ++i) {
    auto r1 = sst.get("key1");
    ASSERT_TRUE(r1.has_value() && r1->has_value());
    EXPECT_EQ(**r1, "value1");

    auto r2 = sst.get("key2");
    ASSERT_TRUE(r2.has_value() && r2->has_value());
    EXPECT_EQ(**r2, "value2");
  }
}

// --- Path accessor test ---

TEST_F(SSTableTest, PathReturnsCorrectPath) {
  write_test_data({{"key", "value"}});
  SSTable sst = SSTable::open(test_path_).value();

  EXPECT_EQ(sst.path(), test_path_);
}

// --- Data integrity tests ---

TEST_F(SSTableTest, LargeKeyValue) {
  std::string large_key(1000, 'k');
  std::string large_value(5000, 'v');
  write_test_data({{large_key, large_value}});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get(large_key);
  ASSERT_TRUE(result.has_value() && result->has_value());
  EXPECT_EQ(**result, large_value);
}

TEST_F(SSTableTest, SpecialCharactersInKeyValue) {
  std::string key_with_special = "key\x01\x02\xFF";
  std::string value_with_special = "val\x00\x7F\xFE";
  write_test_data({{key_with_special, value_with_special}});
  SSTable sst = SSTable::open(test_path_).value();

  auto result = sst.get(key_with_special);
  ASSERT_TRUE(result.has_value() && result->has_value());
  EXPECT_EQ(**result, value_with_special);
}

TEST_F(SSTableTest, ManyEntries) {
  std::vector<std::pair<std::string, std::string>> entries;
  for (int i = 0; i < 100; ++i) {
    entries.emplace_back("key" + std::to_string(i),
                         "value" + std::to_string(i));
  }
  write_test_data(entries);
  SSTable sst = SSTable::open(test_path_).value();

  // Check a few entries
  auto r0 = sst.get("key0");
  ASSERT_TRUE(r0.has_value() && r0->has_value());
  EXPECT_EQ(**r0, "value0");

  auto r50 = sst.get("key50");
  ASSERT_TRUE(r50.has_value() && r50->has_value());
  EXPECT_EQ(**r50, "value50");

  auto r99 = sst.get("key99");
  ASSERT_TRUE(r99.has_value() && r99->has_value());
  EXPECT_EQ(**r99, "value99");

  // Nonexistent key
  auto missing = sst.get("key100");
  ASSERT_TRUE(missing.has_value());
  EXPECT_FALSE(missing->has_value());
}
