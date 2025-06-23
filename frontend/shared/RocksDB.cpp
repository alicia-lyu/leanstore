#include "RocksDB.hpp"

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