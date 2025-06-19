#pragma once
#include "../shared/RocksDB.hpp"
#include "logger.hpp"
#include "rocksdb/db.h"

#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/wide_columns.h>
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"

struct RocksDBLogger : public Logger {
   RocksDBLogger(RocksDB& db)
   {
      uint64_t sst_read_prev = 0, sst_write_prev = 0;
      std::shared_ptr<rocksdb::Statistics> stats = db->GetDBOptions().statistics;
      rocksdb::HistogramData sst_read_hist;
      stats->histogramData(rocksdb::Histograms::SST_READ_MICROS, &sst_read_hist);
      rocksdb::HistogramData sst_write_hist;
      stats->histogramData(rocksdb::Histograms::SST_WRITE_MICROS, &sst_write_hist);
      if (total_aborted + total_committed > 0) {
         csv << (sst_read_hist.sum - sst_read_prev) / (total_aborted + total_committed) << ","
             << (sst_write_hist.sum - sst_write_prev) / (total_aborted + total_committed) << ",";
         sst_read_prev = sst_read_hist.sum;
         sst_write_prev = sst_write_hist.sum;
      } else {
         csv << "0,0,";
      }
   }
};