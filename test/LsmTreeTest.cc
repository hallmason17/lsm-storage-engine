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

// --- Compaction tests ---

TEST_F(LsmTreeTest, CompactionTriggersAfterFourSSTables) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  // Create 4 SSTables to trigger compaction
  lsm.put("key1", "value1");
  lsm.put("trigger1", large_value);

  lsm.put("key2", "value2");
  lsm.put("trigger2", large_value);

  lsm.put("key3", "value3");
  lsm.put("trigger3", large_value);

  lsm.put("key4", "value4");
  lsm.put("trigger4", large_value);

  // All data should still be retrievable after compaction
  EXPECT_EQ(*lsm.get("key1"), "value1");
  EXPECT_EQ(*lsm.get("key2"), "value2");
  EXPECT_EQ(*lsm.get("key3"), "value3");
  EXPECT_EQ(*lsm.get("key4"), "value4");
}

TEST_F(LsmTreeTest, CompactionPreservesAllKeys) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  // Insert unique keys across multiple SSTables
  for (int batch = 0; batch < 4; ++batch) {
    for (int i = 0; i < 10; ++i) {
      std::string key =
          "batch" + std::to_string(batch) + "_key" + std::to_string(i);
      std::string value =
          "value_" + std::to_string(batch) + "_" + std::to_string(i);
      lsm.put(key, value);
    }
    lsm.put("trigger" + std::to_string(batch), large_value);
  }

  // Verify all keys are still accessible after compaction
  for (int batch = 0; batch < 4; ++batch) {
    for (int i = 0; i < 10; ++i) {
      std::string key =
          "batch" + std::to_string(batch) + "_key" + std::to_string(i);
      std::string expected =
          "value_" + std::to_string(batch) + "_" + std::to_string(i);
      auto result = lsm.get(key);
      ASSERT_TRUE(result.has_value()) << "Missing key: " << key;
      EXPECT_EQ(*result, expected) << "Wrong value for key: " << key;
    }
  }
}

TEST_F(LsmTreeTest, CompactionKeepsNewerValueOnKeyCollision) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  // Write same key with different values across SSTables
  lsm.put("shared_key", "oldest_value");
  lsm.put("trigger1", large_value);

  lsm.put("shared_key", "middle_value");
  lsm.put("trigger2", large_value);

  lsm.put("shared_key", "newer_value");
  lsm.put("trigger3", large_value);

  lsm.put("shared_key", "newest_value");
  lsm.put("trigger4", large_value);

  // After compaction, the newest value should be preserved
  auto result = lsm.get("shared_key");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "newest_value");
}

TEST_F(LsmTreeTest, CompactionHandlesMixedNewAndOldKeys) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  // SSTable 1: keys a, b, c
  lsm.put("a", "a_v1");
  lsm.put("b", "b_v1");
  lsm.put("c", "c_v1");
  lsm.put("trigger1", large_value);

  // SSTable 2: keys b, c, d (b and c are updates)
  lsm.put("b", "b_v2");
  lsm.put("c", "c_v2");
  lsm.put("d", "d_v1");
  lsm.put("trigger2", large_value);

  // SSTable 3: keys c, d, e (c and d are updates)
  lsm.put("c", "c_v3");
  lsm.put("d", "d_v2");
  lsm.put("e", "e_v1");
  lsm.put("trigger3", large_value);

  // SSTable 4: keys d, e, f (d and e are updates)
  lsm.put("d", "d_v3");
  lsm.put("e", "e_v2");
  lsm.put("f", "f_v1");
  lsm.put("trigger4", large_value);

  // Verify each key has its most recent value
  EXPECT_EQ(*lsm.get("a"), "a_v1");
  EXPECT_EQ(*lsm.get("b"), "b_v2");
  EXPECT_EQ(*lsm.get("c"), "c_v3");
  EXPECT_EQ(*lsm.get("d"), "d_v3");
  EXPECT_EQ(*lsm.get("e"), "e_v2");
  EXPECT_EQ(*lsm.get("f"), "f_v1");
}

TEST_F(LsmTreeTest, CompactionReducesSSTableCount) {
  LsmTree lsm;

  std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

  // Create 4 SSTables
  for (int i = 0; i < 4; ++i) {
    lsm.put("key" + std::to_string(i), "value" + std::to_string(i));
    lsm.put("trigger" + std::to_string(i), large_value);
  }

  // Count SST files after compaction
  int sst_count = 0;
  for (const auto &entry :
       std::filesystem::directory_iterator(std::filesystem::current_path())) {
    if (entry.path().extension() == ".sst") {
      ++sst_count;
    }
  }

  // After compaction of 4 SSTables merging in pairs, should have 2
  EXPECT_EQ(sst_count, 2) << "Expected 2 SSTables after compacting 4";
}

TEST_F(LsmTreeTest, DataSurvivesRestartAfterCompaction) {
  // First session: create data and trigger compaction
  {
    LsmTree lsm;

    std::string large_value(lsm_constants::kMemTableFlushThreshold, 'x');

    lsm.put("persistent_key1", "persistent_value1");
    lsm.put("trigger1", large_value);

    lsm.put("persistent_key2", "persistent_value2");
    lsm.put("trigger2", large_value);

    lsm.put("persistent_key3", "persistent_value3");
    lsm.put("trigger3", large_value);

    lsm.put("persistent_key4", "persistent_value4");
    lsm.put("trigger4", large_value); // Triggers compaction
  }

  // Second session: verify data persisted
  {
    LsmTree lsm;

    EXPECT_EQ(*lsm.get("persistent_key1"), "persistent_value1");
    EXPECT_EQ(*lsm.get("persistent_key2"), "persistent_value2");
    EXPECT_EQ(*lsm.get("persistent_key3"), "persistent_value3");
    EXPECT_EQ(*lsm.get("persistent_key4"), "persistent_value4");
  }
}
