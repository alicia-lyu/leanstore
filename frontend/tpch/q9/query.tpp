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
//   2. For each OL row: look up PART (apply LIKE filter), PARTSUPP, SUPPLIER,
//      NATION using hashmaps for NATION+SUPPLIER and merge join for PART+PARTSUPP.
//   3. Compute profit = l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity.
//   4. Accumulate into a (n_name, o_year) -> sum_profit map; emit sorted output.
// See: frontend/tpch/q9/CLAUDE.md §Execution Style: Monolithic vs Cascade.

template <typename Backend>
long Q9Workload<Backend>::query_by_base(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.merge_join_base(lambda) for the OL pair.
   // Inside the lambda, perform 4 additional joins:
   //   - Sort OL output by l_partkey -> MergeJoin with PART scan (apply LIKE filter)
   //   - Sort by (l_partkey, l_suppkey) -> MergeJoin with PARTSUPP scan
   //   - Load SUPPLIER into hashmap, look up by l_suppkey
   //   - Load NATION into hashmap, look up by s_nationkey
   // Compute profit per lineitem, aggregate by (n_name, EXTRACT(YEAR, o_orderdate)).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 1 (5 merge joins).
   out.clear();
   return 0;
}

template <typename Backend>
long Q9Workload<Backend>::query_by_view(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Scan pipeline_view adapter for joined_ol_t rows.
   // Apply the same 4-join post-processing as query_by_base:
   //   PART (with LIKE filter), PARTSUPP (merge join), SUPPLIER+NATION (hashmaps).
   // Compute profit, aggregate by (n_name, o_year).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 2 & 4.
   out.clear();
   return 0;
}

template <typename Backend>
long Q9Workload<Backend>::query_by_merged(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.scan_merged(lambda) for the OL pair via PremergedJoin.
   // Build in-memory hashmaps for NATION (25 rows) and SUPPLIER (~10K at SF=1)
   // before the scan — these are too small to warrant merge-join infrastructure.
   // Inside the callback, for each joined_ol_t:
   //   - Merge-join with PART by l_partkey (apply LIKE filter on p_name)
   //   - Merge-join with PARTSUPP by (l_partkey, l_suppkey)
   //   - Hash-lookup SUPPLIER by l_suppkey -> then NATION by s_nationkey
   //   - Compute profit, accumulate into (n_name, o_year) map
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 3;
   //      §Execution Style: Monolithic vs Cascade (hash-lookup sketch for
   //      NATION and SUPPLIER, merge join for PART and PARTSUPP).
   out.clear();
   return 0;
}

template <typename Backend>
long Q9Workload<Backend>::query_by_hash(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Drive ol.hash_join_base(lambda) for the OL pair.
   // Apply the same 4-join post-processing as query_by_base but using hash joins
   // for PART and PARTSUPP as well (all 5 joins are hash joins).
   // Build PART, PARTSUPP, SUPPLIER, NATION hashmaps before the OL scan.
   // Compute profit, aggregate by (n_name, o_year).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 2 & 4
   //      (hash join variant — all 5 joins hash-joined).
   out.clear();
   return 0;
}

// ---------------------------------------------------------------------------
// Per-structure wrapper query() and get_size() methods

template <typename Backend>
long BaseQ9<Backend>::query(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_base(out).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 1.
   out.clear();
   return 0;
}

template <typename Backend>
double BaseQ9<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long ViewQ9<Backend>::query(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_view(out).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 2 & 4.
   out.clear();
   return 0;
}

template <typename Backend>
double ViewQ9<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long MergedQ9<Backend>::query(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_merged(out).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 3.
   out.clear();
   return 0;
}

template <typename Backend>
double MergedQ9<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

template <typename Backend>
long HashQ9<Backend>::query(std::vector<q9_agg_row_t>& out)
{
   // TODO(skeleton): Forward to w.query_by_hash(out).
   // See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions, Structure 2 & 4
   //      (hash join variant).
   out.clear();
   return 0;
}

template <typename Backend>
double HashQ9<Backend>::get_size() const
{
   // TODO(skeleton): Forward to w.get_size().
   return 0;
}

}  // namespace tpch::q9
