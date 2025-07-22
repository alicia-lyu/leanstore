#include "RocksDB.hpp"
#include <rocksdb/advanced_options.h>
#include <rocksdb/cache.h>
#include <rocksdb/options.h>
#include <rocksdb/rocksdb_namespace.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>
#include "leanstore/utils/JumpMU.hpp"

using ROCKSDB_NAMESPACE::CacheEntryRole;
using ROCKSDB_NAMESPACE::CacheEntryRoleOptions;
using ROCKSDB_NAMESPACE::CacheUsageOptions;
using ROCKSDB_NAMESPACE::NewGenericRateLimiter;

DEFINE_int32(active_column_families, 1, "Number of active column families in RocksDB");  // default is large memtable budget for every column family

void RocksDB::set_options()
{
   db_options.use_direct_reads = true;  // otherwise, OS page cache is used, and we cannot set its size
   db_options.use_direct_io_for_flush_and_compaction = true;
   db_options.create_if_missing = true;
   db_options.create_missing_column_families = true;
   db_options.statistics = rocksdb::CreateDBStatistics();
   db_options.stats_dump_period_sec = 1;
   db_options.max_background_jobs = 1;  // only one background thread (compaction or flush) for better transparency
   db_options.rate_limiter.reset(NewGenericRateLimiter(10 * 1024 * 1024)); // ensure not to saturate the disk with background writes

   // COMPACTION & COMPRESSION
   db_options.compression = rocksdb::CompressionType::kNoCompression;
   db_options.OptimizeLevelStyleCompaction();
   db_options.compaction_style = rocksdb::kCompactionStyleLevel;
   db_options.target_file_size_base = 1 * 1024 * 1024;  // default is 64 MB, but our dataset is smaller
   db_options.target_file_size_multiplier = 2;

   
   table_opts.block_cache = cache;
   // charge all memory usage to the block cache except memtables
   // FILTER & INDEX
   table_opts.metadata_block_size = 64 * 1024;  // default 4096 is too small for modern hardware
   table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
   table_opts.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
   table_opts.cache_index_and_filter_blocks = true;
   table_opts.pin_l0_filter_and_index_blocks_in_cache = true;
   table_opts.partition_filters = true;
   // MEMORY ACCOUNTING
   CacheUsageOptions cache_opts;
   CacheEntryRoleOptions cache_entry_opts;
   cache_entry_opts.charged = CacheEntryRoleOptions::Decision::kEnabled;
   cache_opts.options_overrides = {{CacheEntryRole::kCompressionDictionaryBuildingBuffer, cache_entry_opts},  // entry opts will be copied
                                   {CacheEntryRole::kFilterConstruction, cache_entry_opts},
                                   {CacheEntryRole::kBlockBasedTableReader, cache_entry_opts},
                                   {CacheEntryRole::kFileMetadata, cache_entry_opts}};

   // MEMTABLES
   size_t memtable_budget = memtable_share * total_cache_bytes;
   std::cout << "RocksDB: memtable_budget = " << memtable_budget << " for " << FLAGS_active_column_families << " column families." << std::endl;
   db_options.max_total_wal_size = memtable_budget * 0.1;  // 10% of the memtable budget
   // Total budget for all memtables across all CFs
   size_t write_buffer_budget = memtable_budget * 0.9;
   db_options.write_buffer_manager = std::make_shared<rocksdb::WriteBufferManager>(write_buffer_budget, cache);
   // With a WriteBufferManager, you still set a base write_buffer_size,
   // but the manager enforces the global limit.
   db_options.write_buffer_size = 64 * 1024 * 1024;  // A reasonable base size
   db_options.max_write_buffer_number = 4;           // slightly higher
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

double RocksDB::get_size(ColumnFamilyHandle* cf_handle, const int, const std::string& name)
{
   // compact so that every experiment starts with a clean slate for fair comparison
   std::cout << "Compacting " << name << "..." << std::flush;
   rocksdb::CompactRangeOptions cr_options;
   cr_options.exclusive_manual_compaction = true;
   cr_options.change_level = true;
   cr_options.target_level = -1;  // force compaction to the bottommost level
   cr_options.allow_write_stall = true;
   cr_options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
   tx_db->CompactRange(cr_options, cf_handle, nullptr, nullptr);
   rocksdb::WaitForCompactOptions wfc_options;
   tx_db->WaitForCompact(wfc_options);
   std::cout << " done." << std::endl;

   std::string total_sstables_size;  // kTotalSstFilesSize
   tx_db->GetProperty(cf_handle, "rocksdb.total-sst-files-size", &total_sstables_size);
   std::string live_sstables_size;  // kLiveSstFilesSize
   tx_db->GetProperty(cf_handle, "rocksdb.live-sst-files-size", &live_sstables_size);
   std::string live_data_size;  // kEstimateLiveDataSize
   tx_db->GetProperty(cf_handle, "rocksdb.estimate-live-data-size", &live_data_size);
   std::string num_keys;  // kEstimateNumKeys
   tx_db->GetProperty(cf_handle, "rocksdb.estimate-num-keys", &num_keys);

   rocksdb::ColumnFamilyMetaData cf_meta;
   tx_db->GetColumnFamilyMetaData(cf_handle, &cf_meta);

   long total_num_deletions = 0;
   for (const auto& level_meta : cf_meta.levels) {
      std::cout << "Level " << level_meta.level << ": " << level_meta.files.size() << " files; ";
      for (const auto& sst_file_meta : level_meta.files) {
         total_num_deletions += sst_file_meta.num_deletions;
      }
   }
   std::cout << std::endl;

   long long live_data_size_bytes = std::stoll(live_data_size);
   long long total_sstables_size_bytes = std::stoll(total_sstables_size);
   long long live_sstables_size_bytes = std::stoll(live_sstables_size);

   if (live_data_size_bytes != live_sstables_size_bytes || live_sstables_size_bytes != total_sstables_size_bytes) {
      std::cerr << "Error: Live data size, live SSTables size, and total SSTables size do not match!" << std::endl;
      std::cerr << "Live Data Size: " << live_data_size_bytes << " bytes" << std::endl;
      std::cerr << "Live SSTables Size: " << live_sstables_size_bytes << " bytes" << std::endl;
      std::cerr << "Total SSTables Size: " << total_sstables_size_bytes << " bytes" << std::endl << std::flush;
   }

   std::ofstream sstable_csv;
   sstable_csv.open(std::filesystem::path(FLAGS_csv_path).parent_path() / "sstables.csv", std::ios::app);
   if (sstable_csv.tellp() == 0) {
      sstable_csv << "tableid,size (MiB),file count,levels,num keys,total deletions" << std::endl;
   }
   std::cout << "tableid,size (MiB),file count,levels,num keys,total deletions" << std::endl;
   std::vector<std::ostream*> out = {&std::cout, &sstable_csv};
   for (std::ostream* o : out) {
      *o << name << "," << (double)live_data_size_bytes / 1024.0 / 1024.0 << "," << cf_meta.file_count << "," << cf_meta.levels.size() << ","
         << num_keys << "," << total_num_deletions << std::endl;
   }
   sstable_csv.close();

   return live_data_size_bytes / 1024.0 / 1024.0;
}