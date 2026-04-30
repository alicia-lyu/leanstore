// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §3 op 6 (hash-aggregate outside)   — group-by unrelated to pipeline sort key
//   §3 op 7 (Downstream HashJoin)      — Q9's 4 dim joins use HashJoin
//   §5 (Q9 sketch)                     — outside-pipeline strategy
//   §6 (comparison-integrity rules)    — read before changing join strategy
//
// Template method bodies for Q9Workload<Backend> query methods,
// the four per-structure wrapper query() methods, and predicate functions.
// All bodies are TODO stubs. Each comment cites the CLAUDE.md section
// or infrastructure header the implementer should consult.

#pragma once

namespace tpch::q9
{

// ---------------------------------------------------------------------------
// Predicate implementations

// Applied to a fully-assembled joined_ol_t (structures 2 and 3) before the
// per-lineitem dimension lookups. The LIKE filter is on p_name (PART), which
// is only available after the PART join, so this function is a no-op placeholder.
// See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions — the LIKE filter is
//      applied post-projection in all plan variants.
inline bool q9_predicate_joined(const joined_ol_t& /* j */, const Params& /* p */)
{
   // TODO(skeleton): Currently always returns true — the LIKE filter on p_name
   // cannot be applied until after the PART join. Apply it inline after probing
   // the PART adapter/hashmap in query_by_merged / query_by_view.
   // See: frontend/tpch/q9/CLAUDE.md §Column Index Mapping, p_name=$26.
   return true;
}

// ---------------------------------------------------------------------------
// Q9Workload query methods
//
// All four methods share a common 4-step post-join pattern:
//   1. Produce joined_ol_t rows (OL join via the chosen driver).
//   2. For each OL row: probe PART (apply LIKE filter), PARTSUPP, SUPPLIER,
//      NATION hash tables (all 4 built once before scan). See OPERATORS.md §3 op 7.
//   3. Compute profit = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity.
//   4. Accumulate into a (n_name, o_year) -> sum_profit map; emit sorted output.
// See: frontend/tpch/q9/CLAUDE.md §Execution Style: Monolithic vs Cascade.

template <typename Backend>
long Q9Workload<Backend>::query_by_base(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Build PART (filtered by LIKE), PARTSUPP, SUPPLIER, NATION
   // hash tables once before scan. Drive ol.merge_join_base(lambda) for the OL
   // pair. Inside the callback, probe each hash table per-row:
   //   PART (apply LIKE filter) -> PARTSUPP -> SUPPLIER -> NATION
   // Compute profit per lineitem, aggregate by (n_name, EXTRACT(YEAR, o_orderdate)).
   // See: OPERATORS.md §3 op 4 (S1), §3 op 7 (downstream HashJoin), §5 Q9.
   out.clear();
   return 0;
}

template <typename Backend>
long Q9Workload<Backend>::query_by_view(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Build PART (filtered by LIKE), PARTSUPP, SUPPLIER, NATION
   // hash tables once. Scan pipeline_view adapter, probe each hash table per-row.
   // Compute profit, aggregate by (n_name, o_year).
   // See: OPERATORS.md §3 op 4 (S2), §3 op 7 (downstream HashJoin), §5 Q9.
   out.clear();
   return 0;
}

template <typename Backend>
long Q9Workload<Backend>::query_by_merged(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Build PART (filtered by LIKE), PARTSUPP, SUPPLIER, NATION
   // hash tables once before scan. Drive ol.scan_merged(lambda) for the OL pair
   // via PremergedJoin. Inside the callback, probe each hash table per-row:
   //   PART (apply LIKE filter) -> PARTSUPP -> SUPPLIER -> NATION
   // Compute profit, accumulate into (n_name, o_year) map.
   // See: OPERATORS.md §3 op 4 (S3), §3 op 7 (downstream HashJoin), §5 Q9.
   out.clear();
   return 0;
}

template <typename Backend>
long Q9Workload<Backend>::query_by_hash(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Build PART (filtered by LIKE), PARTSUPP, SUPPLIER, NATION
   // hash tables once. Drive ol.hash_join_base(lambda) for the OL pair.
   // Inside the callback, probe each hash table per-row (all 5 joins are HashJoin).
   // Compute profit, aggregate by (n_name, o_year).
   // See: OPERATORS.md §3 op 4 (S4 baseline), §3 op 7 (downstream HashJoin).
   out.clear();
   return 0;
}

// Per-structure wrapper query() and get_size() methods are defined as inline
// forwarders in frontend/tpch/per_structure_workload.hpp (shared template).
// No per-query definitions needed here.

}  // namespace tpch::q9
