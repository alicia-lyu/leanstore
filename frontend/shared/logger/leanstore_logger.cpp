#include "leanstore_logger.hpp"
#include <gflags/gflags.h>
#include <string>
#include <tabulate/table.hpp>
#include "logger.hpp"
#include "../Types.hpp"

void LeanStoreLogger::summarize_other_stats()
{
   // Advance bm/dt/cr to the window END. The base Logger::log() advances only
   // cpu+configs, so without this the bm columns reported the stale
   // window-START cumulative snapshot taken at reset() (e.g. "0 evictions" with
   // an 8.6 GiB structure in a 1 GiB pool). cpu_table is already advanced by
   // the base log() — do NOT advance it here (would double-count).
   bm_table.next();
   dt_table.next();
   cr_table.next();

   // Counters are absolute-cumulative; subtract the reset()-time start snapshot
   // to get the per-window delta (the IO actually done by this query loop).
   const double w_mib = stod(bm_table.get("0", "w_mib")) - bm_w_mib_start;
   const double r_mib = stod(bm_table.get("0", "r_mib")) - bm_r_mib_start;
   const double e_mib = stod(bm_table.get("0", "evicted_mib")) - bm_evicted_mib_start;

   switch (stats.column_name) {
      case ColumnName::ELAPSED:
         stats.header.push_back("W MiB");
         stats.data.push_back(to_fixed(w_mib));
         stats.header.push_back("R MiB");
         stats.data.push_back(to_fixed(r_mib));
         stats.header.push_back("Evicted MiB");
         stats.data.push_back(to_fixed(e_mib));
         break;
      case ColumnName::TPUT:
         stats.header.push_back("W MiB / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed(w_mib / stats.tx_count) : "NaN");
         stats.header.push_back("R MiB / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed(r_mib / stats.tx_count) : "NaN");
         stats.header.push_back("Evicted MiB / TX");
         stats.data.push_back(stats.tx_count > 0 ? to_fixed(e_mib / stats.tx_count) : "NaN");
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