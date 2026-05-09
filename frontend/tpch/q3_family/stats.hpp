#pragma once

// Q3FamilyStats: counters specific to the Q3 query family (Q3, Q3I, …).
//
// Workload-agnostic counters (scan/filter/join/stage timing/MI walk) now live
// in the base struct TPCHFamilyStats (tpch_family/family_stats.hpp) so that
// Q5 and future queries can derive from it without pulling in Q3 machinery.
//
// Q3FamilyStats adds the Q3-specific topN counters that have no equivalent in
// queries without a LIMIT clause (Q5 has no LIMIT; its per-n_name result
// cardinality is bounded by |nation_set| ≈ 5).
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

#include "../tpch_family/family_stats.hpp"

namespace tpch::q3_family
{

struct Q3FamilyStats : public tpch::TPCHFamilyStats {
   // -----------------------------------------------------------------------
   // Q3-family TopN counters (Q5 has no LIMIT — these stay here, not in base).
   long topN_candidates = 0;  // rows entering apply_topN (= aggregator_rows_out)

   // Per-stage wall-clock for the TopN sort step (microseconds).
   long stage_us_topN = 0;

   void reset() { *this = Q3FamilyStats{}; }
};

}  // namespace tpch::q3_family
