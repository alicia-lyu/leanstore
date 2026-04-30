#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §3 op 7 (Downstream HashJoin)      — Q9's 4 dim joins use HashJoin
//   §5 (Q9 sketch)                     — outside-pipeline strategy
//   §6 (comparison-integrity rules)    — read before changing join strategy
//   §7 (HashJoin downstream)           — all 4 dim joins use HashJoin, not MergeJoin
//
// Q9Workload<Backend>: holds all adapters needed by Q9 and declares the
// per-structure query / load / size methods.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join  -> query_by_base
//   2 = intermediate pipeline view        -> query_by_view
//   3 = MI[0] only (PremergedJoin)        -> query_by_merged
//   4 = traditional indexes + hash join   -> query_by_hash
//
// Q9 joins 6 tables. The ORDERS x LINEITEM pair goes into the shared MI.
// The four remaining dimension tables (NATION, SUPPLIER, PART, PARTSUPP)
// are accessed as independent adapters at query time — NATION and SUPPLIER
// are small enough to load into in-memory hashmaps; PART and PARTSUPP are
// merge-joined. None of the four are placed into merged indexes.
// See: frontend/tpch/q9/CLAUDE.md §Execution Style: Monolithic vs Cascade.

#include <vector>

#include "../backend.hpp"
#include "../ol_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q9
{

template <typename Backend>
class Q9Workload
{
   // The full TPC-H base-table workload (owns all 8 base tables).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Shared ORDERS x LINEITEM pipeline: wraps the three join drivers.
   OrdersLineitemPipeline<Backend> ol;

   // Dimension table adapters used at query time (not in any merged index).
   typename Backend::template Adapter<nation_t>&    nation;    // 25 rows — loaded into hashmap
   typename Backend::template Adapter<supplier_t>&  supplier;  // ~10K rows at SF=1 — hashmap
   typename Backend::template Adapter<part_t>&      part;      // merge-joined on l_partkey
   typename Backend::template Adapter<partsupp_t>&  partsupp;  // merge-joined on (l_partkey, l_suppkey)

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   // Remaining 4 joins happen at query time.
   typename Backend::template Adapter<q9_pipeline_view_t>& pipeline_view;

   // Substitution parameters; populated by ctor from gflags / defaults.
   Params params;

  public:
   Q9Workload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<nation_t>& nation,
       typename Backend::template Adapter<supplier_t>& supplier,
       typename Backend::template Adapter<part_t>& part,
       typename Backend::template Adapter<partsupp_t>& partsupp,
       typename Backend::template Adapter<q9_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Each returns the number of result rows written into `out`
   // (one per distinct (n_name, o_year) pair — up to 25 nations × ~7 years).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q9_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q9_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q9_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q9_agg_row_t>& out);  // structure 4

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

}  // namespace tpch::q9

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
