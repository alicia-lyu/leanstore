#pragma once

// Q12Workload<Backend>: holds all adapters needed by Q12 and declares the
// per-structure query / load / size methods.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join  -> query_by_base
//   2 = intermediate pipeline view        -> query_by_view
//   3 = MI[0] only (PremergedJoin)        -> query_by_merged
//   4 = traditional indexes + hash join   -> query_by_hash
//
// The shared ORDERS x LINEITEM join drivers live in OrdersLineitemPipeline;
// this class composes them with Q12-specific filter/project/aggregate logic
// supplied as lambdas (monolithic post-join style per q12/CLAUDE.md
// §Execution Style).

#include <vector>

#include "../backend.hpp"
#include "../ol_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q12
{

template <typename Backend>
class Q12Workload
{
   // The full TPC-H base-table workload (owns part, supplier, etc.).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Shared ORDERS x LINEITEM pipeline: wraps the three join drivers.
   OrdersLineitemPipeline<Backend> ol;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view;

   // Substitution parameters; populated by ctor from gflags / defaults.
   Params params;

  public:
   Q12Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Each returns the number of result rows written into `out`
   // (at most 7, the shipmode cardinality).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q12_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q12_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q12_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q12_agg_row_t>& out);  // structure 4

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

}  // namespace tpch::q12

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
