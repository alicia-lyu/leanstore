#include "RocksDB.hpp"
#include "leanstore/utils/JumpMU.hpp"

void RocksDB::set_options()
{
   // OPTIONS
   wo.disableWAL = true;
   wo.sync = false;
   iterator_ro.snapshot = nullptr;  // Snapshot from pinning resources
   db_options.use_direct_reads = true;
   db_options.use_direct_io_for_flush_and_compaction = true;
   db_options.db_write_buffer_size = 0;  // disabled
   // db_options.write_buffer_size = 64 * 1024 * 1024; keep the default
   db_options.create_if_missing = true;
   db_options.create_missing_column_families = true;
   // db_options.manual_wal_flush = true;
   db_options.compression = rocksdb::CompressionType::kNoCompression;
   db_options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleLevel;
   db_options.row_cache = rocksdb::NewLRUCache(FLAGS_dram_gib * 1024 * 1024 * 1024);
   rocksdb::BlockBasedTableOptions table_options;
   table_options.filter_policy.reset(
       rocksdb::NewBloomFilterPolicy(10, false));  // As of RocksDB 7.0, the use_block_based_builder parameter is ignored.
   db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
   db_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(u32)));  // ID
   db_options.statistics = rocksdb::CreateDBStatistics();
   db_options.stats_dump_period_sec = 1;
}

bool RocksDB::Put(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, const rocksdb::Slice& value)
{
   if (txn == nullptr) {
      Status s = tx_db->Put(wo, cf_handle, key, value);
      return s.ok();
   } else {
      Status s = txn->Put(cf_handle, key, value);
      if (!s.ok()) {
         txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
}

bool RocksDB::Get(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, PinnableSlice* value)
{
   if (txn == nullptr) {
      Status s = tx_db->Get(ro, cf_handle, key, value);
      return s.ok();
   } else {
      Status s = txn->Get(ro, cf_handle, key, value);
      if (!s.ok()) {
         txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
}

bool RocksDB::GetForUpdate(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, PinnableSlice* value)
{
   if (txn == nullptr) {
      Status s = tx_db->Get(ro, cf_handle, key, value);
      return s.ok();
   } else {
      Status s = txn->GetForUpdate(ro, cf_handle, key, value);
      if (!s.ok()) {
         txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
}

bool RocksDB::Merge(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, const rocksdb::Slice& value)
{
   if (txn == nullptr) {
      Status s = tx_db->Merge(wo, cf_handle, key, value);
      return s.ok();
   } else {
      Status s = txn->Merge(cf_handle, key, value);
      if (!s.ok()) {
         txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
}

bool RocksDB::Delete(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key)
{
   if (txn == nullptr) {
      Status s = tx_db->Delete(wo, cf_handle, key);
      return s.ok();
   } else {
      Status s = txn->Delete(cf_handle, key);
      if (!s.ok()) {
         txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
}