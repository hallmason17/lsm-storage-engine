#include "LsmTree.h"
#include "Constants.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

using namespace lsm_storage_engine;

class LsmTreeTest : public ::testing::Test {
protected:
  std::filesystem::path wal_path_ = "lsm.wal";

  void SetUp() override {
    // Clean up any leftover files from previous runs
    cleanup_test_files();
  }

  void TearDown() override { cleanup_test_files(); }

private:
  void cleanup_test_files() {
    std::filesystem::remove(wal_path_);
    // Remove any SST files created during tests
    for (const auto &entry :
         std::filesystem::directory_iterator(std::filesystem::current_path())) {
      if (entry.path().extension() == ".sst") {
        std::filesystem::remove(entry.path());
      }
    }
    std::filesystem::remove("lsm.meta");
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

// --- SSTable integration tests ---

TEST_F(LsmTreeTest, DataRetrievableFromSSTableAfterFlush) {
  LsmTree lsm;

  for (int i = 0; i < lsm_constants::kMemTableFlushThreshold; ++i) {
    lsm.put("key" + std::to_string(i), "value" + std::to_string(i));
  }

  // Verify data is still retrievable after it may have been flushed to SSTable
  EXPECT_EQ(*lsm.get("key0"), "value0");
  EXPECT_EQ(*lsm.get("key15"), "value15");
  EXPECT_EQ(*lsm.get("key29"), "value29");
}

TEST_F(LsmTreeTest, MemTableTakesPrecedenceOverSSTable) {
  LsmTree lsm;

  // Put enough data to trigger a flush
  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');
  lsm.put("key1", large_value);

  // This should trigger flush, and then add new data to memtable
  lsm.put("key1", "updated_value");

  // The memtable value should take precedence
  auto result = lsm.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "updated_value");
}

TEST_F(LsmTreeTest, MultipleFlushesMaintainData) {
  LsmTree lsm;

  // Trigger multiple flushes
  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  lsm.put("batch1_key", "batch1_value");
  lsm.put("trigger1", large_value); // Triggers first flush

  lsm.put("batch2_key", "batch2_value");
  lsm.put("trigger2", large_value); // Triggers second flush

  lsm.put("batch3_key", "batch3_value");

  // All data should be retrievable
  EXPECT_EQ(*lsm.get("batch1_key"), "batch1_value");
  EXPECT_EQ(*lsm.get("batch2_key"), "batch2_value");
  EXPECT_EQ(*lsm.get("batch3_key"), "batch3_value");
}

TEST_F(LsmTreeTest, NewerSSTableTakesPrecedence) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  // Put key with value1, then trigger flush
  lsm.put("shared_key", "value1");
  lsm.put("trigger1", large_value);

  // Put same key with value2, then trigger another flush
  lsm.put("shared_key", "value2");
  lsm.put("trigger2", large_value);

  // The value from the newer SSTable should be returned
  auto result = lsm.get("shared_key");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "value2");
}

TEST_F(LsmTreeTest, GetMissingKeyAfterFlush) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');
  lsm.put("exists", large_value); // Triggers flush

  // Key that was never inserted should return nullopt
  auto result = lsm.get("nonexistent");
  EXPECT_FALSE(result.has_value());
}
