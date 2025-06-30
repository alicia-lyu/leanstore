#include "RocksDB.hpp"
#include <rocksdb/cache.h>
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/table.h>
#include "leanstore/utils/JumpMU.hpp"

using ROCKSDB_NAMESPACE::CacheEntryRole;
using ROCKSDB_NAMESPACE::CacheEntryRoleOptions;
using ROCKSDB_NAMESPACE::CacheUsageOptions;

void RocksDB::set_options()
{
   // OPTIONS
   iterator_ro.fill_cache = false;

   db_options.use_direct_reads = true; // otherwise, OS page cache is used, and we cannot set its size
   db_options.use_direct_io_for_flush_and_compaction = true;
   db_options.create_if_missing = true;
   db_options.create_missing_column_families = true;
   // Other durability-related settings:
   db_options.WAL_ttl_seconds = 0;    // keep WAL indefinitely (until manual deletion)
   db_options.WAL_size_limit_MB = 0;  // no size-based WAL deletion

   db_options.compression = rocksdb::CompressionType::kNoCompression;
   db_options.OptimizeLevelStyleCompaction();
   db_options.compaction_style = rocksdb::kCompactionStyleLevel;

   // table_opts.block_cache = cache;

   db_options.max_write_buffer_number = 2;

   table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));  // As of RocksDB 7.0, the use_block_based_builder parameter is ignored.

   db_options.statistics = rocksdb::CreateDBStatistics();
   db_options.stats_dump_period_sec = 1;

   // charge all memory usage to the block cache except memtables
   size_t memtable_budget = memtable_share * total_cache_bytes;
   table_opts.block_cache = cache;
   CacheUsageOptions cache_opts;
   CacheEntryRoleOptions cache_entry_opts;
   cache_entry_opts.charged = CacheEntryRoleOptions::Decision::kEnabled;
   cache_opts.options_overrides = {{CacheEntryRole::kCompressionDictionaryBuildingBuffer, cache_entry_opts},  // entry opts will be copied
                                   {CacheEntryRole::kFilterConstruction, cache_entry_opts},
                                   {CacheEntryRole::kBlockBasedTableReader, cache_entry_opts},
                                   {CacheEntryRole::kFileMetadata, cache_entry_opts}};
   
   // memtables
   db_options.max_write_buffer_number = 2;
   db_options.write_buffer_size = memtable_budget / 2;
   db_options.min_write_buffer_number_to_merge = 1;
}

bool RocksDB::Put(ColumnFamilyHandle* cf_handle, const rocksdb::Slice& key, const rocksdb::Slice& value)
{
   if (txn == nullptr) {
      Status s = tx_db->Put(wo, cf_handle, key, value);
      if (!s.ok()) {
         std::cerr << "RocksDB::Put failed: " << s.ToString() << std::endl;
         return false;
      }
      return true;
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
      if (!s.ok()) {
         std::cerr << "RocksDB::Get failed: " << s.ToString() << std::endl;
         return false;
      }
      return true;
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
      // return s.ok();
      if (!s.ok()) {
         std::cerr << "RocksDB::GetForUpdate failed: " << s.ToString() << std::endl;
         return false;
      }
      return true;
   } else {
      Status s = txn->GetForUpdate(ro, cf_handle, key, value);
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
      // return s.ok();
      if (!s.ok()) {
         std::cerr << "RocksDB::Delete failed: " << s.ToString() << std::endl;
         return false;
      }
      return true;
   } else {
      Status s = txn->Delete(cf_handle, key);
      if (!s.ok()) {
         txn->Rollback();
         jumpmu::jump();
      }
      return true;
   }
}

double RocksDB::get_size(ColumnFamilyHandle* cf_handle, const int max_fold_len, const std::string& name)
{
   std::vector<u8> min_key(1, 0);  // min key
   auto start_slice = RSlice(min_key.data(), min_key.size());
   std::vector<u8> max_key(max_fold_len, 255);  // max key
   auto limit_slice = RSlice(max_key.data(), max_key.size());

   std::cout << "RocksDB::get_size: Compacting " << name << "...";
   auto compact_options = rocksdb::CompactRangeOptions();
   compact_options.change_level = true;
   auto ret = tx_db->CompactRange(compact_options, cf_handle, &start_slice, &limit_slice);
   assert(ret.ok());

   std::array<u64, 1> sizes;
   std::array<Range, 1> ranges;
   ranges[0].start = start_slice;
   ranges[0].limit = limit_slice;
   rocksdb::SizeApproximationOptions size_options;
   size_options.include_memtables = true;
   size_options.include_files = true;
   size_options.files_size_error_margin = 0.1;
   tx_db->GetApproximateSizes(size_options, cf_handle, ranges.data(), ranges.size(), sizes.data());
   return (double)sizes[0] / 1024.0 / 1024.0;  // convert to MB
}