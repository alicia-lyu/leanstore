#pragma once

// Q3FamilyStats: counters that every Q3-family query (Q3, Q3I, …) needs.
//
// Scope: only counters that Q3 (3-table: CUSTOMER × ORDERS × LINEITEM) will
// also increment.  Invoice-specific counters, per-structure chain-join output
// rows, aCOLI-specific scans, backend-perf fields, and seek-skip event
// counters belong in the per-query stats struct (e.g. Q3IStats).
//
// Per-path stage attribution policy (same rules apply to every Q3-family
// query — see q3i/workload.hpp §Coverage gaps for fidelity caveats):
//
//   S1 (merge)   — scan + filter + aggregate all attributed to stage_us_join.
//                  stage_us_scan_filter and stage_us_aggregator are zero.
//   S2 (view)    — scan + filter attributed to stage_us_scan_filter.
//                  stage_us_join and stage_us_aggregator are zero.
//   S3 (merged)  — entire fused walk attributed to stage_us_join.
//                  stage_us_scan_filter and stage_us_aggregator are zero.
//   S4 (hash)    — entire HJ chain attributed to stage_us_join.
//                  stage_us_scan_filter and stage_us_aggregator are zero.

namespace tpch::q3_family
{

struct Q3FamilyStats {
   // -----------------------------------------------------------------------
   // Per-table scan counts (base-table rows visited).
   long customers_scanned  = 0;
   long orders_scanned     = 0;
   long lineitems_scanned  = 0;

   // -----------------------------------------------------------------------
   // Per-table rows passing their single-table filter.
   long customers_passing_filter  = 0;  // post c_mktsegment filter
   long orders_passing_filter     = 0;  // post o_orderdate filter
   long lineitems_passing_filter  = 0;  // post l_shipdate filter

   // -----------------------------------------------------------------------
   // Join / aggregator pipeline cardinalities.
   long join_callbacks      = 0;  // rows entering the final join / accumulation
   long aggregator_rows_out = 0;  // distinct (orderkey) groups emitted pre-topN
   long topN_candidates     = 0;  // rows entering apply_topN (= aggregator_rows_out)

   // -----------------------------------------------------------------------
   // MI walk counters — bumped by Q3FamilyVisitor::on_record_visited /
   // on_group_skipped during S3 (col_group_walk / coli_group_walk).
   long mi_records_visited = 0;
   long mi_groups_skipped  = 0;

   // -----------------------------------------------------------------------
   // Per-stage wall-clock (microseconds).  Filled by std::chrono brackets
   // around the matching stage; zero for stages that do not exist on a path.
   long stage_us_scan_filter = 0;
   long stage_us_aggregator  = 0;
   long stage_us_join        = 0;
   long stage_us_topN        = 0;

   void reset() { *this = Q3FamilyStats{}; }
};

}  // namespace tpch::q3_family
