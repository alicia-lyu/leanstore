#pragma once

// #include "Logger.hpp"
// #include "Merge.hpp"
#include "TPCHWorkload.hpp"
// #include "Tables.hpp"
// #include "BasicJoinViews.hpp"

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

template <template <typename> class AdapterType, class MergedAdapterType>
class BasicJoin
{
   using TPCH = TPCHWorkload<AdapterType, MergedAdapterType>;
   TPCH& workload;
   

};