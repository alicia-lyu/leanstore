#pragma once

// Q3Workload<Backend>: holds all adapters needed by Q3 and declares the
// per-structure query / load / size methods.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join  -> query_by_base
//   2 = intermediate pipeline view        -> query_by_view
//   3 = MI[0] only (PremergedJoin)        -> query_by_merged
//   4 = traditional indexes + hash join   -> query_by_hash
//
// Q3 adds a CUSTOMER adapter to Q12's shape. CUSTOMER is joined at query time
// via merge join on o_custkey = c_custkey after the ORDERS x LINEITEM aggregate
// (monolithic post-join style per q3/CLAUDE.md §Execution Style).

#include <vector>

#include "../backend.hpp"
#include "../ol_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q3
{

template <typename Backend>
class Q3Workload
{
   // The full TPC-H base-table workload (owns part, supplier, etc.).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Shared ORDERS x LINEITEM pipeline: wraps the three join drivers.
   OrdersLineitemPipeline<Backend> ol;

   // CUSTOMER adapter: used at query time for the c_mktsegment filter and
   // c_custkey = o_custkey join. Not part of any merged index.
   typename Backend::template Adapter<customerh_t>& customer;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q3_pipeline_view_t>& pipeline_view;

   // Substitution parameters; populated by ctor from gflags / defaults.
   Params params;

  public:
   Q3Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<customerh_t>& customer,
       typename Backend::template Adapter<q3_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Each returns the number of result rows written into `out`
   // (at most 10, the LIMIT 10 of Q3).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q3_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q3_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q3_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q3_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   // Dispatches on FLAGS_storage_structure to call tpch.load() plus
   // whichever secondary structure load is needed.
   void load();

   // Returns the size (MiB) of the secondary structure for the active
   // storage structure (0 for structures 1 and 4, which have no extras).
   double get_size() const;
};

}  // namespace tpch::q3

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
