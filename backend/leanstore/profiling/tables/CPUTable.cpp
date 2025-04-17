#include "CPUTable.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/utils/ThreadLocalAggregator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using leanstore::utils::threadlocal::sum;
namespace leanstore
{
namespace profiling
{
// -------------------------------------------------------------------------------------
std::string CPUTable::getName()
{
   return "cpu";
}
// -------------------------------------------------------------------------------------
void CPUTable::open()
{
   PerfEvent e; // Only for getting the names
   for (const auto& event_name : e.getEventsName()) {
      workers_agg_events[event_name] = 0;
      pp_agg_events[event_name] = 0;
      ww_agg_events[event_name] = 0;
      for (auto& [_, counters] : CPUCounters::threads) {
         auto& t_name = counters.name;
         workers_events[t_name][event_name] = 0;
      }
      columns.emplace(event_name, [](Column&) {});
   }
   columns.emplace("key", [](Column&) {});
}
// -------------------------------------------------------------------------------------
void CPUTable::next()
{
   clear();
   // -------------------------------------------------------------------------------------
   for (auto& c : workers_agg_events) {
      c.second = 0;
   }
   for (auto& c : pp_agg_events) {
      c.second = 0;
   }
   for (auto& c : ww_agg_events) {
      c.second = 0;
   }
   for (auto& [_, w] : workers_events) {
      for (auto& c : w) {
         c.second = 0;
      }
   }
   // -------------------------------------------------------------------------------------
   {
      std::unique_lock guard(CPUCounters::mutex);
      for (auto& [t_id, counters] : CPUCounters::threads) {
         counters.e->stopCounters();
         auto events_map = counters.e->getCountersMap();
         auto& t_name = counters.name;
         columns.at("key") << t_name;
         for (auto& event : events_map) {
            double event_value;
            if (std::isnan(event.second)) {
               event_value = 0;
            } else {
               event_value = event.second;
            }
            workers_events[t_name][event.first] += event_value;
            if (t_name.rfind("worker", 0) == 0) {
               workers_agg_events[event.first] += event_value;
            } else if (t_name.rfind("pp", 0) == 0) {
               pp_agg_events[event.first] += event_value;
            } else if (t_name.rfind("ww") == 0) {
               ww_agg_events[event.first] += event_value;
            }
            columns.at(event.first) << event.second;
         }
         counters.e->startCounters();
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace profiling
}  // namespace leanstore
