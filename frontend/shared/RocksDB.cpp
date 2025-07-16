#include "RocksDB.hpp"
#include <rocksdb/advanced_options.h>
#include <rocksdb/cache.h>
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/table.h>
#include "leanstore/utils/JumpMU.hpp"

using ROCKSDB_NAMESPACE::CacheEntryRole;
using ROCKSDB_NAMESPACE::CacheEntryRoleOptions;
using ROCKSDB_NAMESPACE::CacheUsageOptions;

void RocksDB::set_options()
{
   db_options.use_direct_reads = true;  // otherwise, OS page cache is used, and we cannot set its size
   db_options.use_direct_io_for_flush_and_compaction = true;
   db_options.create_if_missing = true;
   db_options.create_missing_column_families = true;

   db_options.compression = rocksdb::CompressionType::kNoCompression;
   db_options.OptimizeLevelStyleCompaction();
   db_options.compaction_style = rocksdb::kCompactionStyleUniversal;

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

   std::array<u64, 1> sizes;
   std::array<Range, 1> ranges;
   ranges[0].start = start_slice;
   ranges[0].limit = limit_slice;
   rocksdb::SizeApproximationOptions size_options;
   size_options.include_memtables = true;
   size_options.include_files = true;
   size_options.files_size_error_margin = 0.1;
   tx_db->GetApproximateSizes(size_options, cf_handle, ranges.data(), ranges.size(), sizes.data());
   std::string SSTables;
   tx_db->GetProperty(cf_handle, "rocksdb.sstables", &SSTables);
   std::cout << name << " SSTables: " << SSTables << std::endl;
   rocksdb::ColumnFamilyMetaData cf_meta;
   tx_db->GetColumnFamilyMetaData(cf_handle, &cf_meta);

   std::cout << "\n--- Column Family: " << cf_meta.name << " Metadata ---" << std::endl;
   std::cout << "  Size: " << static_cast<double>(cf_meta.size) / 1024.0 / 1024.0 << " MB" << std::endl;
   std::cout << "  File Count: " << cf_meta.file_count << std::endl;
   std::cout << "  Levels: " << cf_meta.levels.size() << std::endl;

   for (const auto& level_meta : cf_meta.levels) {
      std::cout << "  --- Level " << level_meta.level << " ---" << std::endl;
      std::cout << "    Level Size: " << static_cast<double>(level_meta.size) / 1024.0 / 1024.0 << " MB" << std::endl;
      std::cout << "    Files at this Level: " << level_meta.files.size() << std::endl;

      for (const auto& sst_file_meta : level_meta.files) {
         std::cout << "      File Number: " << sst_file_meta.file_number << std::endl;
         std::cout << "      Size: " << static_cast<double>(sst_file_meta.size) / 1024.0 / 1024.0 << " MB" << std::endl;
         std::cout << "      Smallest Key: '" << sst_file_meta.smallestkey << "'" << std::endl;
         std::cout << "      Largest Key: '" << sst_file_meta.largestkey << "'" << std::endl;
         std::cout << "      Number of Entries: " << sst_file_meta.num_entries << std::endl;
         std::cout << "      Number of Deletions: " << sst_file_meta.num_deletions << std::endl;
         std::cout << "      Being Compacted: " << (sst_file_meta.being_compacted ? "Yes" : "No") << std::endl;
         // You can get more fields from sst_file_meta if needed (e.g., smallest_seqno, largest_seqno, etc.)
      }
   }
   return (double)sizes[0] / 1024.0 / 1024.0;  // convert to MB
}