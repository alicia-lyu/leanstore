# OPERATORS.md — TPC-H Tier 1 Monolithic Translation Reference

How Calcite operator trees translate into C++ for Q12, Q3, and Q9. Read
this alongside per-query `q{N}/CLAUDE.md §Plan Descriptions` before
filling in any `query_by_*` body.

## 1. Purpose & Structure of the Comparison

The goal is to translate Calcite plans into monolithic C++ honestly —
without operator-tree machinery, without `std::function` indirection,
and without obscuring the differences we are trying to measure.

The four storage structures fall into two groups:

- **Merged-index family (S1, S2, S3)** share one logical plan,
  `q{N}/plans/family_logical.dot`. Inside-pipeline physical execution
  varies — `BinaryMergeJoin` (S1) vs. materialized-view scan (S2) vs.
  `PremergedJoin` (S3) — and that variation is the comparison axis.
  Outside-pipeline operators are held constant across S1/2/3.
- **No-merged-index baseline (S4)** uses the default Calcite plan in
  `q{N}/plans/baseline_s4.dot` (chained `HashJoin`s). It does not
  share inside-pipeline code with the family; it represents what the
  optimizer picks when nothing is interleaved, and is the baseline the
  family is compared against.

Two further commitments shape the design:

- Within S2, the materialization point is the **pipeline output, not
  the final query result**. Otherwise S2's query time becomes a
  trivial scan and we measure nothing.
- The outside-pipeline plan uses `HashJoin` even where Calcite's
  family logical plan picks `MergeJoin` for interesting orderings.
  Justification in §7.

## 2. Operator Inventory

| Operator              | Regime  | Q12 | Q3 | Q9 | Notes |
|-----------------------|---------|-----|----|----|-------|
| TableScan             | inside  | ✓   | ✓  | ✓  | driver-owned |
| Filter (single-table) | inside  | ✓   | ✓  | ✓  | order-preserving |
| Filter (post-join)    | inside  | ✓   | —  | ✓  | order-preserving |
| Join (ORDERS×LINEITEM)| inside  | ✓   | ✓  | ✓  | comparison axis |
| Project               | inside  | ✓   | ✓  | ✓  | inline scalar exprs |
| SortedAggregate       | inside  | —   | ✓  | —  | streams on pipeline sort key |
| Downstream Join       | outside | —   | ✓  | ✓  | shared `HashJoin` |
| Sort (final)          | outside | ✓   | ✓  | ✓  | `std::sort` on bounded vector |
| Limit                 | outside | —   | ✓  | —  | `out.resize(...)` |

Q12 has no inside-pipeline aggregate (its group-by `l_shipmode` is
unrelated to the pipeline sort key; handled as a tiny inline
accumulator in the post-join callback). Q9's group-by `(n_name,
o_year)` is also unrelated to the pipeline sort key, so its aggregate
runs *outside* the pipeline regime — see §3 op 6 and §5.

## 3. Per-Operator Strategy

### Inside the pipeline

**1. TableScan.** Owned by the per-query driver in `q{N}/query.tpp`,
which instantiates `Backend::Scanner<T>`, `Backend::MergedScanner`, or
the per-query view scanner. The shared `OrdersLineitemPipeline` no
longer exposes scan drivers — it is narrowed to load-time helpers
(`populate_merged` / `get_merged_size`) only.

**2. Filter (single-table, pushed-down).** A free
`bool q{N}_predicate_<table>(const T&, const Params&)` declared in
`q{N}/views.hpp` and defined in `q{N}/query.tpp`. Applied either
inside the join driver before the post-join callback fires, or as the
first line of the callback. Order-preserving.

**3. Filter (post-join, needs joined row).** First line of the
post-join callback: `if (!pred) return;`. Used when the predicate
needs columns from multiple tables (Q9 LIKE on `p_name` post-PART-
lookup). Order-preserving.

**4. Join.** Two logical-plan families:

*Merged-index family (S1, S2, S3)* — all share `family_logical.dot`. The
inside-pipeline physical join differs:

- S1: `BinaryMergeJoin<ol_sort_key_t, joined_ol_t, orders_t,
  lineitem_t>` driven from `q{N}/query.tpp` over `orders.getScanner()`
  / `lineitem.getScanner()` → orderkey-sorted output. The
  SortedAggregate scanner-wrapper (op 6) applies cleanly.
- S2: scan `pipeline_view` (built at load time as the output of S1's
  pipeline — see §4). Query time runs the outside-pipeline plan
  against the materialized rows.
- S3: `PremergedJoin<...>` over `merged_ol.getScanner()` from
  `q{N}/query.tpp` → orderkey-sorted output. Same `JoinState` core as
  S1's `BinaryMergeJoin` — that's what makes the S1-vs-S3
  inside-pipeline comparison apples-to-apples. The SortedAggregate
  scanner-wrapper applies cleanly. PremergedJoin substitution shown in
  `family_s3_physical.dot`.

*No-merged-index baseline (S4)* — uses `baseline_s4.dot`: a chain
of `HashJoin` instances over base tables (ORDERS ⋈ LINEITEM for Q12;
CUSTOMER ⋈ ORDERS ⋈ LINEITEM for Q3; the full 6-table chain for Q9),
then filter, project, hash-aggregate, sort, limit. S4 does not share
inside-pipeline code with the family. Reusable code is limited to the
shared `HashJoin` class itself and the outside-pipeline operators
(sort, limit, project).

**Load-time vs query-time.** Query-time joins (S1/S3/S4) all go through
the typed merge-join primitives (`BinaryMergeJoin`, `PremergedJoin`,
`HashJoin`) so `JoinState` (or `HashJoin`'s build table) is shared
across structures — the cross-structure performance comparison only
stays honest if the join cores are identical. View loading (S2's
`populate_*_view`) is a one-shot offline cost off the comparison axis;
a manual two-pointer merge over base scanners is an acceptable
simplification there (see `q12/load.tpp::populate_q12_view`). Do
**not** copy the manual-merge pattern into query-time code.

**5. Project.** Inline scalar expressions in the callback body —
`revenue = li.l_extendedprice * (1 - li.l_discount)`,
`amount = ... - ps.ps_supplycost * li.l_quantity`,
`o_year = year_of(o.o_orderdate)`, CASE-WHEN priority bucketing. No
`Project` object, no expression tree.

**6. SortedAggregate (Q3 only inside the pipeline).** Implemented as a
**scanner-shaped wrapper** around the underlying merged or base
scanner, following the `MergedScannerCounter` pattern in
`frontend/geo/mixed_query.tpp`. The wrapper:

- consumes raw records from the inner scanner,
- accumulates state across records of the same group key, and
- synthesizes a single aggregated record at the group boundary that
  satisfies the scanner interface (`next()`, `seek<R>()`,
  `last_in_page()`, `go_to_last_in_page()`).

The wrapper is then passed to `PremergedJoin` (or `BinaryMergeJoin`)
as if it were a regular scanner — the join code is **unaware** that
it is seeing aggregated records. For Q3 this means a
`LineitemRevenueAggregator` wrapper that emits one synthesized
`lineitem_agg_t` record per orderkey carrying
`SUM(l_extendedprice * (1 - l_discount))`; ORDERS is then joined with
those aggregated rows. **No `unordered_map`, no re-sort, no separate
aggregate pass.**

Q12 and Q9 don't fit this pattern — their group-by keys aren't the
pipeline sort key — so their aggregates run differently:

- Q12: a tiny inline two-slot accumulator in the post-join callback
  (≤2 distinct shipmodes per query).
- Q9: a hash-aggregate *outside* the pipeline. This is legitimate
  because we are past the comparison axis once all dim joins have run.

### Outside the pipeline

**7. Downstream Join.** Uses shared
`HashJoin<JK, JR, R1, R2>` from
`frontend/shared/merge-join/hash_join.hpp`. The build phase scans the
filtered build side once at construction and materializes an
in-memory hash table (`JoinState`) held for the rest of the query;
`run()` then streams the probe side. **The algorithm is identical
regardless of pipeline structure** — pipeline output is never
re-sorted to match a downstream merge join. Per query:

- Q3: build = filtered CUSTOMER (`c_mktsegment = SEGMENT`); probe =
  the post-aggregate pipeline rows. Join key
  `c_custkey = o_custkey`.
- Q9: NATION, SUPPLIER, PART (filtered by name LIKE), and PARTSUPP
  each become a separate `HashJoin` (or its `JoinState` table) built
  once and probed inside the post-join callback. Multiple per-row
  probes chain naturally.

**8. Sort (final ORDER BY).** `std::sort(out.begin(), out.end(), cmp)`
on the bounded post-aggregate vector. Sizes: Q12 ≤7 rows (5 shipmode
combinations); Q3 ≤order-count pre-limit; Q9 ≤~175 (25 nations × 7
years).

**9. Limit (Q3 only).** `out.resize(std::min(out.size(), 10))` after
sort. A top-K heap is not worth the complexity given the small group
count.

## 4. Why Pipeline Views Differ Per Query

`OrdersLineitemPipeline::populate_merged()` is identical for all three
queries: dual-write replay of orders and lineitem into `MI_OL`. But
`populate_view(ViewAdapter&)` materializes the **output of the
structure-1 inside-pipeline operator stack** — i.e., what
`merge_join_base` plus the per-query inside-pipeline operators
(filter, project, optional SortedAggregate scanner-wrapper) produce.
S2 is exactly "scan that materialized output", so its view row type
and cardinality match S1's pipeline output.

Per query:

- **Q12.** No in-pipeline aggregate. The view stores **unfiltered**
  `joined_ol_t` rows (one per `(orderkey, linenumber)`); the Q12
  predicate is hoisted to query time so the view is reusable across
  param sets (see `q12/CLAUDE.md §Predicate hoisting`). Population uses
  a manual two-pointer merge over `orders.getScanner()` /
  `lineitem.getScanner()` for simplicity (see
  `q12/load.tpp::populate_q12_view`); a `BinaryMergeJoin` driver would
  also work and is the right choice for query-time S1 (see §3 op 4
  load-vs-query note).
- **Q3.** In-pipeline SortedAggregate on `l_orderkey`, implemented as
  the `LineitemRevenueAggregator` scanner-wrapper. The view stores
  one row per orderkey with summed revenue and the FD-attached order
  columns (`o_orderdate`, `o_shippriority`, `o_custkey`).
  `q3_pipeline_view_t` is *not* `joined_ol_t` — it has fewer rows and
  a different schema. Population drives `BinaryMergeJoin` with the
  aggregator-wrapped scanner; the join emits already-aggregated
  joined rows that get inserted directly.
- **Q9.** No in-pipeline aggregate (group-by is unrelated to
  orderkey). The view stores the post-projection joined rows,
  optionally augmented with PART/PARTSUPP columns probed during
  population. `q9_pipeline_view_t` carries the augmented columns
  needed downstream.

This is why `populate_view` in `ol_pipeline.hpp` is templated on
`ViewAdapter` — each query supplies its own row type, and the lambda
fed to the join driver differs per query (raw insert vs. streaming
aggregate vs. augmented projection).

## 5. Worked Examples

### Q12 — no inside-pipeline aggregate; tiny post-join accumulator

```cpp
struct slot { Varchar<10> mode; long high = 0, low = 0; };
slot a{params.shipmode1}, b{params.shipmode2};
auto cb = [&](const joined_ol_t& jr) {
   const auto& o  = jr.template get<orders_t>();
   const auto& li = jr.template get<lineitem_t>();
   if (!q12_predicate_lineitem(li, params)) return;
   slot* s = (li.l_shipmode == a.mode) ? &a
           : (li.l_shipmode == b.mode) ? &b : nullptr;
   if (!s) return;
   if (is_high_priority(o.o_orderpriority)) ++s->high;
   else                                     ++s->low;
};
// S3: PremergedJoin over MI[0]
PremergedJoin<MergedScanner, ol_sort_key_t, joined_ol_t,
              orders_t, lineitem_t>
    joiner(merged_ol.getScanner());
joiner.run(cb);
// S1: BinaryMergeJoin<ol_sort_key_t, joined_ol_t, orders_t, lineitem_t>
//     over orders.getScanner() / lineitem.getScanner() (same cb).
// S4: HashJoin<Integer, joined_ol_t, orders_t, lineitem_t> over
//     base scanners (same cb).
// emit a, b → out; std::sort by l_shipmode
```

### Q3 — scanner-wrapper SortedAggregate inside, shared HashJoin outside

```cpp
// LineitemRevenueAggregator wraps the underlying merged scanner.
// On each next(), it consumes raw lineitem records from the inner
// scanner, accumulates revenue across same-orderkey rows, and emits
// one synthesized lineitem_agg_t record per orderkey boundary.
// orders_t records pass through unchanged. Implements the same
// scanner interface PremergedJoin expects: next(), seek<R>(),
// last_in_page(), go_to_last_in_page(). See:
// frontend/geo/mixed_query.tpp, MergedScannerCounter — same pattern.

LineitemRevenueAggregator<...> agg_scanner(merged_ol, params);
PremergedJoin<decltype(agg_scanner),
              ol_sort_key_t, q3_pipeline_view_t,
              orders_t, lineitem_agg_t>
    joiner(agg_scanner);
// PremergedJoin is unaware aggregation happened — it just sees a
// scanner emitting (orders_t, lineitem_agg_t) records sorted by
// orderkey, and produces joined rows of pre-aggregated revenue.

// Outside: shared HashJoin against filtered CUSTOMER, then sort+limit.
HashJoin<Integer, joined_q3_t, customerh_t, q3_pipeline_view_t> hj(...);
joiner.run([&](const q3_pipeline_view_t& row) {
   /* feed row into hj's probe stream */
});
hj.run([&](const joined_q3_t& joined) { out.push_back(project(joined)); });
std::sort(out.begin(), out.end(), q3_cmp);
out.resize(std::min(out.size(), size_t{10}));
```

### Q9 — sketch only

Pre-load NATION, SUPPLIER, PART (filtered by LIKE), and PARTSUPP into
shared `HashJoin` build-phase tables once at query start. Then
`PremergedJoin` over `merged_ol.getScanner()` (instantiated in
`q9/query.tpp`) with a callback that probes each in sequence
(PART → PARTSUPP → SUPPLIER → NATION) and feeds matches into a final
`(n_name, o_year)` hash-aggregate *outside* the pipeline — legitimate
because the group-by key is unrelated to the pipeline sort key, so we
are past the comparison axis. Full sketch in `q9/CLAUDE.md`.

## 6. Filter Pushdown

Every parameterised filter pushes as far down the operator graph as possible,
stopping ONLY at secondary structures (so MIs / split indexes / pipeline views
stay reusable across param sets — predicate hoisting). Concretely:

- Single-table filters fuse with TableScan at query time.
- Aggregate-output filters (e.g. Q3I's threshold on `cust_open_due`) fuse with
  the SortedAggregate; never a post-aggregate Filter node.
- For merged-index physical execution (single-scanner walkers), every filter
  applies inside the Visitor's `on_*` hook for that record type — there is no
  post-walk Filter node.
- Secondary structures (MIs, split indexes, pipeline views) are loaded
  UNFILTERED on parameterised predicates.

### Q3I addendum: custkey-sorted family logical plan

Invoice-extended queries (Q3I, Q5I, Q10I) shift the merged-index family to a
**custkey-sorted** logical plan (see `q3i/plans/family_logical.dot`), replacing
the orderkey-sorted Q3 pattern. Key differences from the vanilla Q3 operator
strategy above:

- The inside-pipeline `SortedAggregate` is now **per-custkey** (invoice
  sub-aggregate producing `cust_open_due`), not per-orderkey. It precedes the
  O×L revenue accumulation in the scan order (`customer → invoice* → (orders →
  lineitem*)+`).
- The threshold filter on `cust_open_due` fuses with the per-customer aggregate
  and fires at the custkey group boundary — **before** any O×L rows for that
  custkey enter the pipeline. This is not an outside-pipeline Filter node.
- S1 implements the family plan as a **3-BMJ chain** over four custkey-sorted
  streams: `CustomerOpenDueAggregator` (scanner-wrapper over `split_invoice`)
  and `LineitemRevenueAggregator` (scanner-wrapper over `split_lineitem`) feed
  the three successive `BinaryMergeJoin` steps. The same accumulator structs
  (`CustomerOpenDueAccumulator`, `LineitemRevenueAccumulator`) are shared with
  S3's `COLIGroupWalkVisitor` — same logical code, different physical scan.
- S3 fuses all of the above into the `coli_group_walk` Visitor's `on_*` hooks.
  By the time a row leaves the walker it has passed every per-table predicate,
  the mktsegment gate, and the threshold gate.

Reference: `q3i/CLAUDE.md §Plan Descriptions`, `q3i/plans/family_logical.dot`,
`q3i/plans/family_s3_physical.dot`.

## 7. Comparison-Integrity Rules

1. **No hash-aggregate inside the merged-index-family pipeline.**
   Hash-aggregating inside S3 would lose the ordering signal
   `PremergedJoin` produces, and the S1-vs-S3 comparison would no
   longer measure ordered-stream-aggregation differences.
2. **No re-sort of pipeline output.** Re-sorting to feed a downstream
   merge join would penalize S4 (whose plan doesn't produce sorted
   output anyway) and add a uniform tax to S1/2/3 that has nothing
   to do with the inside-pipeline axis.
3. **S2 materializes pipeline output, not final query output.**
   Final-output materialization makes S2's query time trivial and
   renders the comparison meaningless.
4. **S4 is allowed its own logical plan.** S4 is the no-merged-index
   baseline, not a member of the family. Forcing the family logical plan
   onto S4 would require either an inside-pipeline hash-aggregate
   (rule 1 violation by analogy) or a forced re-sort (rule 2
   violation).

## 8. Why HashJoin Outside the Pipeline

Calcite's family logical plan picks `MergeJoin` for the CUSTOMER side of
Q3 because both inputs are sorted on the join key after an explicit
`Sort` operator the optimizer inserted to enable an interesting
ordering. We deviate from this and use `HashJoin` for all
outside-pipeline joins. Three reasons:

1. The choice is held constant across S1/2/3 (and S4 already uses
   `HashJoin` by default), so it does not bias the inside-pipeline
   comparison axis.
2. Outside-pipeline build-side cardinalities are small — filtered
   CUSTOMER for Q3, the four small-to-medium dim tables for Q9.
   Interesting-ordering speedups over `HashJoin` are second-order at
   these sizes.
3. Our pipeline output's sort key is `l_orderkey` (after Q3's
   inside-pipeline aggregate, the orderkey *is* the group key, so
   output stays orderkey-sorted; but it is not `o_custkey`-sorted).
   Honoring Calcite's `MergeJoin` choice would require an explicit
   `Sort(o_custkey)` after the pipeline — paid by every structure in
   the family and obscuring the inside-pipeline differences. Using
   `HashJoin` avoids the re-sort.

This is a deliberate deviation. If revisiting later, the re-sort cost
is the only thing standing between us and an honest `MergeJoin`
downstream.

## 9. Pointers

- Per-query plan rationale and SQL: `q{N}/CLAUDE.md §Plan Descriptions`,
  `§Execution Style`.
- Calcite plan files: `q{N}/plans/{family_logical,family_s3_physical,baseline_s4}.dot`.
- Join primitives: `frontend/shared/merge-join/{premerged_join,
  binary_merge_join, hash_join}.hpp`.
- Scanner-wrapper aggregation pattern: `frontend/geo/mixed_query.tpp`,
  `MergedScannerCounter`.
- Pipeline contract (`populate_merged` / `get_merged_size` only — see
  `frontend/tpch/CLAUDE.md §Pipeline Convention`). Per-query view
  loaders (`populate_*_view`) live in `q{N}/load.tpp`.
