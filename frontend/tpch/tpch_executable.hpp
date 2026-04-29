#pragma once

// Shared structure-dispatch helper for TPC-H Tier 1 executables.
//
// Each per-query executable (`q{N}/executable_{rocksdb,leanstore}.cpp`) keeps
// its backend-specific init (RocksDB ctor / LeanStore crm callback) and
// per-query workload construction local. Once the workload is built, the
// executable calls `dispatch_storage_structure<...>(workload, result)` to
// run the per-structure switch.
//
// The four wrapper templates are passed as template-template parameters so
// any per-query namespace (q12, q3, q9) plugs in its own
// {Base,View,Merged,Hash}QN structs.

#include <gflags/gflags.h>
#include <iostream>
#include <vector>

#include "tpch_flags.hpp"  // DECLARE_int32(storage_structure)

namespace tpch
{

template <template <typename> class Base,
          template <typename> class View,
          template <typename> class Merged,
          template <typename> class Hash,
          typename Backend,
          typename QWorkload,
          typename AggRow>
int dispatch_storage_structure(QWorkload& q, std::vector<AggRow>& out)
{
   switch (FLAGS_storage_structure) {
      case 1: { Base<Backend>   wrapper{q}; wrapper.query(out); break; }
      case 2: { View<Backend>   wrapper{q}; wrapper.query(out); break; }
      case 3: { Merged<Backend> wrapper{q}; wrapper.query(out); break; }
      case 4: { Hash<Backend>   wrapper{q}; wrapper.query(out); break; }
      default:
         std::cerr << "Invalid storage_structure: "
                   << FLAGS_storage_structure << std::endl;
         return 1;
   }
   return 0;
}

}  // namespace tpch
