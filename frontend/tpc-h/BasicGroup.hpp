#pragma once

// #include "Logger.hpp"
// #include "Merge.hpp"
#include <optional>
#include "BasicGroupViews.hpp"
#include "Logger.hpp"
#include "TPCHWorkload.hpp"
#include "Tables.hpp"
// #include "Tables.hpp"
// #include "BasicJoinViews.hpp"

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

namespace basic_group
{
template <template <typename> class AdapterType, class MergedAdapterType>
class BasicGroup
{
   using TPCH = TPCHWorkload<AdapterType, MergedAdapterType>;
   TPCH& workload;
   AdapterType<view_t>& view;
   AdapterType<count_partsupp_t>& count_partsupp;
   AdapterType<sum_supplycost_t>& sum_supplycost;
   AdapterType<partsupp_t>& partsupp;
   MergedAdapterType& mergedBasicGroup;

   Logger& logger;

  public:
   BasicGroup(TPCH& workload, MergedAdapterType& mbg, AdapterType<view_t>& v, AdapterType<partsupp_t>& ps)
       : workload(workload), mergedBasicGroup(mbg), view(v), partsupp(ps), logger(workload.logger)
   {
   }

   // point lookups


   // queries
   

   // maintenance
   

   // loading
   void loadBaseTables() { workload.load(); }

   void loadAllOptions()
   {
      auto partsupp_scanner = workload.partsupp.getScanner();
      Integer count = 0;
      Numeric supplycost_sum = 0;
      Integer curr_partkey = 0;
      while (true) {
         auto kv = partsupp_scanner->next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         if (k.partkey == curr_partkey) {
            count++;
            supplycost_sum += v.ps_supplycost;
         } else {
            if (curr_partkey != 0) {
               view.insert(view_t::Key({curr_partkey}), view_t({count, supplycost_sum}));
               mergedBasicGroup.insert(count_partsupp_t::Key({curr_partkey}), count_partsupp_t({count}));
               mergedBasicGroup.insert(sum_supplycost_t::Key({curr_partkey}), sum_supplycost_t({supplycost_sum}));
               count_partsupp.insert(count_partsupp_t::Key({curr_partkey}), count_partsupp_t({count}));
               sum_supplycost.insert(sum_supplycost_t::Key({curr_partkey}), sum_supplycost_t({supplycost_sum}));
            }
            curr_partkey = k.partkey;
            count = 1;
            supplycost_sum = v.ps_supplycost;
         }
      }
   }
};
}  // namespace basic_group