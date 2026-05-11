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

   // -----------------------------------------------------------------------
   // S2 (query_by_view) per-customer seek-skip counter — mirror of
   // mi_groups_skipped (S3).  Bumped when the view scanner seeks past an
   // entire custkey range after a c_mktsegment mismatch on the first row
   // of that custkey group.  Makes the S2-vs-S3 comparison apples-to-apples
   // wrt per-customer skip locality.
   long view_groups_skipped = 0;

   // S4 (query_by_hash) per-qualifying-orderkey seek counter.  Bumped once
   // per physical seek into the lineitem scanner during the index-NL probe
   // pass (post-orders-map-build).  Should equal the size of the qualifying
   // orders map.  Mirror of mi_groups_skipped (S3) / view_groups_skipped
   // (S2) — makes the S1/S2/S3/S4 access-pattern comparison apples-to-apples
   // (every path now exercises physical seek mechanics, not just filter
   // pushdown).
   long s4_orderkey_seeks = 0;

   // S1 (query_by_base) per-custkey seek-skip counters — mirror of
   // mi_groups_skipped (S3) and view_groups_skipped (S2).  Apples-to-apples
   // physical-seek parity for the BMJ chain over custkey-sorted split
   // indexes:
   //   • s1_groups_skipped — bumped each time fetch_ord physically seeks
   //     its scanner past rejected-customer custkey range(s), AND each time
   //     the lineitem aggregator's inner scanner does the same.  Because
   //     S1 has two physical streams (orders + lineitem) the per-customer
   //     skip event can bump this twice.  This is the cost of two physical
   //     streams in S1; what matters is that no physical seek is omitted.
   //   • s1_orders_skipped — reserved counter for a future order-date-miss
   //     seek; currently zero (S3's SkipOrder is logical/next()-based too,
   //     so there is no I/O fairness gap to close).  Declared here so the
   //     harness print site is stable.
   long s1_groups_skipped = 0;
   long s1_orders_skipped = 0;

   void reset() { *this = Q3FamilyStats{}; }
};

}  // namespace tpch::q3_family
