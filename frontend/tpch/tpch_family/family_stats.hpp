#pragma once

// tpch_family/family_stats.hpp — workload-agnostic counters shared by all
// TPC-H COL-family queries (Q3, Q5, Q10, …).
//
// TPCHFamilyStats holds only counters that are meaningful across the full
// vanilla query set.  Query-specific counters (e.g. topN_candidates for Q3,
// invoice-specific counts for Q3I) belong in the per-query or per-pair stats
// struct that derives from this base.
//
// Derivation hierarchy:
//   TPCHFamilyStats          ← this file (tpch_family/)
//     Q3FamilyStats          ← q3_family/stats.hpp  (adds topN_candidates,
//                                                      stage_us_topN)
//       Q3IStats             ← q3i/workload.hpp      (adds invoice / aCOLI
//                                                      / backend-perf fields)
//     Q5Stats (future)       ← q5/workload.hpp       (adds Q5-specific fields)

namespace tpch
{

struct TPCHFamilyStats {
   // -----------------------------------------------------------------------
   // Per-table scan counts (base-table rows visited).
   long customers_scanned  = 0;
   long orders_scanned     = 0;
   long lineitems_scanned  = 0;

   // -----------------------------------------------------------------------
   // Per-table rows passing their single-table filter.
   long customers_passing_filter  = 0;
   long orders_passing_filter     = 0;
   long lineitems_passing_filter  = 0;

   // -----------------------------------------------------------------------
   // Join / aggregator pipeline cardinalities.
   long join_callbacks      = 0;  // rows entering the final join / accumulation
   long aggregator_rows_out = 0;  // distinct groups emitted pre-topN (or total)

   // -----------------------------------------------------------------------
   // MI walk counters — bumped by the shared group-walk visitor during S3
   // (col_group_walk / coli_group_walk).
   long mi_records_visited = 0;
   long mi_groups_skipped  = 0;

   // -----------------------------------------------------------------------
   // Per-stage wall-clock (microseconds).  Filled by std::chrono brackets
   // around the matching stage; zero for stages that do not exist on a path.
   long stage_us_scan_filter = 0;
   long stage_us_aggregator  = 0;
   long stage_us_join        = 0;

   void reset() { *this = TPCHFamilyStats{}; }
};

}  // namespace tpch
