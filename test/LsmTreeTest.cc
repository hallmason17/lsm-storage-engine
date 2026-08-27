#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "Constants.h"
#include "LsmTree.h"
#include "TestUtil.h"

using namespace lsm;

class LsmTreeTest : public ::testing::Test {
 protected:
  std::filesystem::path database_name_{};
  std::filesystem::path wal_path_{};
  std::filesystem::path metadata_path_{};
  lsm::test::TempDir temp_dir_;

  void SetUp() override {
    database_name_ = "test";
    wal_path_ = "test.wal";
    metadata_path_ = "test.meta";
  }
};

TEST_F(LsmTreeTest, GetReturnsNulloptForMissingKey) {
  LsmTree lsm(database_name_);
  EXPECT_EQ(lsm.get("nonexistent"), std::nullopt);
}

TEST_F(LsmTreeTest, PutThenGet) {
  LsmTree lsm(database_name_);
  lsm.put("foo", "bar");
  auto result = lsm.get("foo");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "bar");
}

TEST_F(LsmTreeTest, PutOverwritesExistingKey) {
  LsmTree lsm(database_name_);
  lsm.put("key", "value1");
  lsm.put("key", "value2");
  EXPECT_EQ(*lsm.get("key"), "value2");
}

TEST_F(LsmTreeTest, MultipleKeyValuePairs) {
  LsmTree lsm(database_name_);
  lsm.put("a", "1");
  lsm.put("b", "2");
  lsm.put("c", "3");

  EXPECT_EQ(*lsm.get("a"), "1");
  EXPECT_EQ(*lsm.get("b"), "2");
  EXPECT_EQ(*lsm.get("c"), "3");
}

TEST_F(LsmTreeTest, PutWritesToWal) {
  {
    LsmTree lsm(database_name_);
    lsm.put("key", "value");
  }

  // WAL uses binary format: [keylen:4][valuelen:4][key][value][checksum:4]
  std::ifstream file(wal_path_, std::ios::binary);
  ASSERT_TRUE(file.good());

  uint32_t keylen = 0, valuelen = 0;
  file.read(reinterpret_cast<char*>(&keylen), sizeof(keylen));
  file.read(reinterpret_cast<char*>(&valuelen), sizeof(valuelen));

  EXPECT_EQ(keylen, 3);    // "key"
  EXPECT_EQ(valuelen, 5);  // "value"

  std::string key(keylen, '\0');
  std::string value(valuelen, '\0');
  file.read(key.data(), keylen);
  file.read(value.data(), valuelen);

  EXPECT_EQ(key, "key");
  EXPECT_EQ(value, "value");
}

// --- SSTable integration tests ---

TEST_F(LsmTreeTest, MemTableTakesPrecedenceOverSSTable) {
  LsmTree lsm(database_name_);

  // Put enough data to trigger a flush
  std::string large_value(constants::kMemTableFlushThreshold, 'x');
  lsm.put("key1", large_value);

  // This should trigger flush, and then add new data to memtable
  lsm.put("key1", "updated_value");

  // The memtable value should take precedence
  auto result = lsm.get("key1");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "updated_value");
}

TEST_F(LsmTreeTest, MultipleFlushesMaintainData) {
  LsmTree lsm(database_name_);

  // Trigger multiple flushes
  std::string large_value(constants::kMemTableFlushThreshold, 'x');

  lsm.put("batch1_key", "batch1_value");
  lsm.put("trigger1", large_value);  // Triggers first flush

  lsm.put("batch2_key", "batch2_value");
  lsm.put("trigger2", large_value);  // Triggers second flush

  lsm.put("batch3_key", "batch3_value");

  // All data should be retrievable
  EXPECT_EQ(*lsm.get("batch1_key"), "batch1_value");
  EXPECT_EQ(*lsm.get("batch2_key"), "batch2_value");
  EXPECT_EQ(*lsm.get("batch3_key"), "batch3_value");
}

TEST_F(LsmTreeTest, NewerSSTableTakesPrecedence) {
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');

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
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');
  lsm.put("exists", large_value);  // Triggers flush

  // Key that was never inserted should return nullopt
  auto result = lsm.get("nonexistent");
  EXPECT_FALSE(result.has_value());
}

// --- Compaction tests ---

TEST_F(LsmTreeTest, CompactionTriggersAfterFourSSTables) {
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');

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
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');

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
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');

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
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');

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
  LsmTree lsm(database_name_);

  std::string large_value(constants::kMemTableFlushThreshold, 'x');

  // Create 4 SSTables
  for (int i = 0; i < 4; ++i) {
    lsm.put("key" + std::to_string(i), "value" + std::to_string(i));
    lsm.put("trigger" + std::to_string(i), large_value);
  }

  // Count this test's SST files after compaction (isolated in temp dir)
  int sst_count = 0;
  for (const auto& entry :
       std::filesystem::directory_iterator(std::filesystem::current_path())) {
    if (entry.path().extension() == ".sst") {
      ++sst_count;
    }
  }

  // After compaction of 4 SSTables merging, should have 1
  EXPECT_EQ(sst_count, 1) << "Expected 1 SSTable after compacting 4";
}

TEST_F(LsmTreeTest, DataSurvivesRestartAfterCompaction) {
  // First session: create data and trigger compaction
  {
    LsmTree lsm(database_name_);

    std::string large_value(constants::kMemTableFlushThreshold, 'x');

    lsm.put("persistent_key1", "persistent_value1");
    lsm.put("trigger1", large_value);

    lsm.put("persistent_key2", "persistent_value2");
    lsm.put("trigger2", large_value);

    lsm.put("persistent_key3", "persistent_value3");
    lsm.put("trigger3", large_value);

    lsm.put("persistent_key4", "persistent_value4");
    lsm.put("trigger4", large_value);  // Triggers compaction
  }

  // Second session: verify data persisted
  {
    LsmTree lsm(database_name_);

    EXPECT_EQ(*lsm.get("persistent_key1"), "persistent_value1");
    EXPECT_EQ(*lsm.get("persistent_key2"), "persistent_value2");
    EXPECT_EQ(*lsm.get("persistent_key3"), "persistent_value3");
    EXPECT_EQ(*lsm.get("persistent_key4"), "persistent_value4");
  }
}
