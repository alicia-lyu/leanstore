#pragma once
#include <rocksdb/db.h>
#include "logger.hpp"

#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/wide_columns.h>
#include "../shared/RocksDB.hpp"

struct RocksDBStats {
   u64 sst_read_us;
   u64 sst_write_us;
   u64 compaction_us;

   RocksDBStats() = default;

   RocksDBStats(rocksdb::Statistics& stats) { update(stats); }

   void update(rocksdb::Statistics& stats)
   {
      rocksdb::HistogramData sst_read_hist;
      stats.histogramData(rocksdb::Histograms::SST_READ_MICROS, &sst_read_hist);
      sst_read_us = sst_read_hist.sum;

      rocksdb::HistogramData compaction_time;
      stats.histogramData(rocksdb::Histograms::COMPACTION_TIME, &compaction_time);
      compaction_us = compaction_time.sum;

      rocksdb::HistogramData sst_write_hist;
      stats.histogramData(rocksdb::Histograms::SST_WRITE_MICROS, &sst_write_hist);
      sst_write_us = sst_write_hist.sum;
   }

   void update()
   {
      sst_read_us = 0;
      sst_write_us = 0;
      compaction_us = 0;
   }
};

struct RocksDBLogger : public Logger {
   RocksDBStats prev_stats;
   RocksDBStats curr_stats;
   std::shared_ptr<rocksdb::Statistics> rocksdb_stats_ptr;

   RocksDBLogger(RocksDB& db) : rocksdb_stats_ptr(db.tx_db->GetDBOptions().statistics) { prev_stats = RocksDBStats(*rocksdb_stats_ptr); }

   ~RocksDBLogger() { std::cout << "RocksDB logs written to " << csv_runtime << std::endl; }

   void summarize_other_stats() override;
   
   void reset() override
   {
      Logger::reset();
      prev_stats.update(*rocksdb_stats_ptr);
      curr_stats.update();
   }

   void log_details() override
   {
      // no details to log for RocksDBLogger
   }

   void prepare() override
   {
      // No preparation needed for RocksDBLogger
   }
};