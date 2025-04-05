#pragma once

// #include "Logger.hpp"
// #include "Merge.hpp"
#include "BasicGroupViews.hpp"
#include "Logger.hpp"
#include "TPCHWorkload.hpp"
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
   AdapterType<count_part_t>& count_part;
   AdapterType<avg_supplycost_t>& avg_supplycost;
   AdapterType<part_t>& part;
   MergedAdapterType& mergedBasicGroup;

   Logger& logger;

  public:
   BasicGroup(TPCH& workload, MergedAdapterType& mbg, AdapterType<count_part_t>& cp, AdapterType<avg_supplycost_t>& asc, AdapterType<part_t>& p)
       : workload(workload), mergedBasicGroup(mbg), count_part(cp), avg_supplycost(asc), part(p), logger(workload.logger)
   {
   }
};
}  // namespace basic_group