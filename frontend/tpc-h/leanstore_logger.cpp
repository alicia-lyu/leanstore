#include "leanstore_logger.hpp"
#include <gflags/gflags.h>
#include <string>
#include <tabulate/table.hpp>
#include "logger.hpp"
#include "../shared/Types.hpp"

void LeanStoreLogger::summarize_other_stats()
{

   switch (stats.column_name) {
      case ColumnName::ELAPSED:
         stats.header.push_back("W MiB");
         stats.data.push_back(bm_table.get("0", "w_mib"));

         stats.header.push_back("R MiB");
         stats.data.push_back(bm_table.get("0", "r_mib"));
         break;
      case ColumnName::TPUT:
         stats.header.push_back("W MiB / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed(stod(bm_table.get("0", "w_mib")) / stats.tx_count) : "NaN");

         stats.header.push_back("R MiB / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed(stod(bm_table.get("0", "r_mib")) / stats.tx_count) : "NaN");
         break;
      default:
         break;
   }
}

void LeanStoreLogger::prepare()
{
   [[maybe_unused]] Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
   Integer h_id = 0;
   leanstore::WorkerCounters::myCounters().variable_for_workload = h_id;
}