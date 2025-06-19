#include "rocksdb_logger.hpp"

void RocksDBLogger::summarize_other_stats()
{
   curr_stats.update(*rocksdb_stats_ptr);
   u64 sst_read_us = curr_stats.sst_read_us - prev_stats.sst_read_us;
   u64 sst_write_us = curr_stats.sst_write_us - prev_stats.sst_write_us;
   switch (stats.column_name) {
      case ColumnName::ELAPSED:
         stats.header.push_back("SSTRead(us)");
         stats.data.push_back(std::to_string(sst_read_us));
         stats.header.push_back("SSTWrite(us)");
         stats.data.push_back(std::to_string(sst_write_us));
         break;
      case ColumnName::TPUT:
         stats.header.push_back("SSTRead(us) / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed((double)sst_read_us / stats.tx_count) : "NaN");
         stats.header.push_back("SSTWrite(us) / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed((double)sst_write_us / stats.tx_count) : "NaN");
         break;
      default:
         break;
   }
   stats.header.push_back("Compaction(us)");
   stats.data.push_back(std::to_string(curr_stats.compaction_us - prev_stats.compaction_us));
}