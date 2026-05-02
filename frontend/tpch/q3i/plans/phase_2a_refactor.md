# Q3I Phase 2A — Refactor & shared substrate

Pre-work for Phase 2B (new query paths) and 2C (harness + docs).
This plan touches no new operator graphs — only renames, shared
helpers, view-schema widening, and an S3 internal cleanup.

## Context

Phase 1 (commit `eb357510`) shipped `query_by_merged` (S3) using a
`coli_group_walk` Visitor over a 4-table COLI MergedAdapter, with
two namespace-scope accumulator structs
(`CustomerOpenDueAccumulator`, `LineitemRevenueAccumulator`)
factored for Phase 2 reuse. Three reference DOT plans now govern
Phase 2 design:

- `family_logical.dot` — shared logical for S1/S2/S3 (custkey-sorted
  pipeline, two SortedAggregates, threshold fused with per-customer
  agg)
- `family_s3_physical.dot` — single `coli_group_walk` subsumes 9
  logical operators
- `baseline_s4.dot` — chain of 3 HashJoins (will be refreshed in 2B)

Key principle (per `q3i/CLAUDE.md §Plan Descriptions`): every
parameterised filter pushes as far down as possible, stopping only
at secondary structures so they stay reusable across param sets.

This plan (2A) lands the substrate. Plans 2B / 2C land the bodies
and integration.

## Steps

### Step 0 — Rename `secondary` → `split` across COLI pipeline

A record's contribution to the COLI MI **is** a secondary index of
its base table — whether stored merged or split. Reserving
"secondary" for the per-relation storage variant is misleading.

Rename through the codebase. Mechanical, no behaviour change.

| Old | New |
|-----|-----|
| `populate_secondaries()` | `populate_split()` |
| `get_secondaries_size()` | `get_split_size()` |
| `coli.orders_secondary()` | `coli.split_orders()` |
| `coli.lineitem_secondary()` | `coli.split_lineitem()` |
| `coli.invoice_secondary()` | `coli.split_invoice()` |
| `orders_secondary` member | `split_orders` |
| `lineitem_secondary` member | `split_lineitem` |
| `invoice_secondary` member | `split_invoice` |

Files: `frontend/tpch/coli_pipeline.{hpp,tpp}`,
`frontend/tpch/q3i/load.tpp`,
`frontend/tpch/q3i/executable_{rocksdb,leanstore}.cpp`,
`frontend/tpch/tests/q3i/test_query_q3i_phase1_rocksdb.cpp`. The
merged side keeps its names (`populate_merged`, `merged_adapter`).

**Commit**: "Rename COLI secondary → split (disambiguate from
base-table secondary indexes)".

**Verification**: `make q12_lsm test_query_q12_lsm test_load_coli_lsm
test_query_q3i_phase1_lsm` all green.

### Step 1 — Add shared TPC-H operator helpers

Three small helpers that 2B / 2C will consume across multiple paths
and which future queries (Q5I, Q10I, vanilla Q3 once written) can
also reuse. Land them in shared locations now so consumers don't
have to be rewritten later.

```cpp
// frontend/shared/scanner_helpers.hpp (new file)
template <typename Scanner, typename Predicate>
auto make_filtered_scanner(Scanner s, Predicate p);
//   wraps `s` so next() skips kv pairs where p(kv.second) == false

template <typename R>
auto scan_of(std::vector<R>& v);                 // mutable
template <typename R>
auto scan_of(const std::vector<R>& v);           // const
//   produces a scanner-shaped view over an in-memory vector

// frontend/tpch/operators.hpp (new file) OR appended to view_templates.hpp
template <typename R, typename Cmp>
inline void apply_topN(std::vector<R>& out, size_t K, Cmp cmp) {
   if (out.size() > K) {
      std::partial_sort(out.begin(), out.begin() + K, out.end(), cmp);
      out.resize(K);
   } else {
      std::sort(out.begin(), out.end(), cmp);
   }
}
```

No callers in this commit — just smoke-build. Plans 2B and 2C wire
them in.

**Commit**: "Add shared TPC-H operator helpers (apply_topN,
make_filtered_scanner, scan_of)".

### Step 2 — Widen `q3i_pipeline_view_t` + retroactive doc fix

Phase 1's `q3i_pipeline_view_t` aliased to `joined_ol_t`. Per
`family_logical.dot`, the actual pipeline output is per-(custkey,
orderkey) post-aggregate rows carrying revenue + cust_open_due +
mktsegment + order cols. Widen the schema and add small
intermediate row types that 2B will need (`cust_open_due_t`,
`lineitem_agg_t`).

```cpp
// frontend/tpch/q3i/views.hpp
struct q3i_pipeline_view_t {
   struct Key {
      Integer custkey;     // primary sort
      Integer orderkey;    // secondary sort
      // fold/unfold + maxFoldLength (mirror q12_pipeline_view_t)
   };
   Numeric     revenue;
   Numeric     cust_open_due;
   Varchar<10> c_mktsegment;
   Timestamp   o_orderdate;
   Integer     o_shippriority;
};

struct cust_open_due_t {
   struct Key { Integer custkey; /* fold/unfold */ };
   Numeric cust_open_due;
};

struct lineitem_agg_t {
   struct Key { Integer custkey; Integer orderkey; /* fold/unfold */ };
   Numeric revenue;
};
```

**Retroactive doc fix**: rewrite
`frontend/tpch/q3i/CLAUDE.md §"Phase 1 → Phase 2 design adjustments"
item 2`, which currently says "stay aliased to joined_ol_t". The new
text should match this widened schema and explain why
(per-(custkey, orderkey) cardinality is roughly orderkey count,
much smaller than `joined_ol_t × N` lineitems, while still carrying
everything S2's outside-pipeline filter needs).

**Commit**: "Q3I Phase 2A step 2: widen q3i_pipeline_view_t +
rewrite Phase 1 design-adjustment doc".

**Verification**: `test_query_q3i_phase1_lsm` still passes; the
widened types compile (unused yet).

### Step 3 — Add §Filter Pushdown to OPERATORS.md

Codify the principle that already governs Q12 §4 predicate hoisting
and the three Q3I plans, **before** any new query body lands so 2B
authors (and reviewers) have a canonical reference.

Add a new top-level **§Filter Pushdown** section to
`frontend/tpch/OPERATORS.md`, placed between §Per-Operator Strategy
and §Worked Examples:

> Every parameterised filter pushes as far down the operator graph
> as possible, stopping ONLY at secondary structures (so MIs / split
> indexes / pipeline views stay reusable across param sets —
> predicate hoisting). Concretely:
>
> - Single-table filters fuse with TableScan at query time.
> - Aggregate-output filters (e.g. Q3I's threshold on
>   `cust_open_due`) fuse with the SortedAggregate; never a
>   post-aggregate Filter node.
> - For merged-index physical execution (single-scanner walkers),
>   every filter applies inside the Visitor's `on_*` hook for that
>   record type — there is no post-walk Filter node.
> - Secondary structures (MIs, split indexes, pipeline views) are
>   loaded UNFILTERED on parameterised predicates.

Cross-link from `q3i/CLAUDE.md §Plan Descriptions`. The Q3I-specific
addendum (custkey-sorted family pipeline, threshold fused with
aggregate not "outside-pipeline") lands in 2C alongside the final
Q3I doc updates — this commit only adds the cross-cutting principle.

**Commit**: "OPERATORS.md: add Filter Pushdown principle".

**Verification**: docs only; existing builds unaffected.

### Step 4 — Lift Visitor to namespace scope; retrofit S3

Two tweaks to the existing `query_by_merged`:

1. **Lift the Visitor struct** from inside `query_by_merged` to
   namespace scope in `q3i/query.tpp`. Plans 2B don't reuse this
   Visitor (S1/S2/S4 use chained joins, not a Visitor) — but the
   lift clarifies the design and keeps all aggregator-related code
   adjacent.
2. **Threshold short-circuit**: today the threshold check is at
   `flush_order` (per-orderkey emit gate). Because invoices arrive
   before any order in COLI byte order, by the time `on_order`
   fires `cust_open_due` is final — we can skip the rest of the
   group entirely. Add a per-group `threshold_ok` flag and use it
   to no-op `on_order` / `on_lineitem` when the threshold has
   already failed. Honours
   `family_s3_physical.dot`'s "groups failing the threshold
   short-circuit OL accumulation entirely".
3. **Use `apply_topN`** instead of returning all qualifying rows.
   Drives top-10 ordered by revenue DESC.

**Commit**: "Q3I Phase 2A step 3: lift Visitor, threshold
short-circuit, apply_topN in query_by_merged".

**Verification**: `test_query_q3i_phase1_lsm` still reports
`[OK] row_count > 0` (now `row_count == 10` with deterministic
top-10); first-10 rows match the previous SF=1 output's top-10 by
revenue.

## Critical files (touched by this plan)

- `frontend/tpch/coli_pipeline.{hpp,tpp}` — naming rename
- `frontend/tpch/q3i/load.tpp` — naming rename callsite
- `frontend/tpch/q3i/executable_{rocksdb,leanstore}.cpp` — adapter
  member renames
- `frontend/tpch/tests/q3i/test_query_q3i_phase1_rocksdb.cpp` —
  adapter member renames
- `frontend/shared/scanner_helpers.hpp` (new)
- `frontend/tpch/operators.hpp` (new) OR
  `frontend/shared/view_templates.hpp` (appended)
- `frontend/tpch/q3i/views.hpp` — widen + new intermediate types
- `frontend/tpch/q3i/CLAUDE.md` — retroactive item-2 rewrite
- `frontend/tpch/OPERATORS.md` — new §Filter Pushdown section
- `frontend/tpch/q3i/query.tpp` — Visitor lift + retrofit

## Out of scope

- New query paths (S1, S2, S4) — see `phase_2b_baselines.md`
- Unified harness + doc updates — see `phase_2c_harness.md`
