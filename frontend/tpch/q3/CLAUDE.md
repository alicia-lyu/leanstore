# Q3: Shipping Priority

## Sibling Docs

Every non-`CLAUDE.md` Markdown in `q3/` and `q3/plans/` (the latter
has no `CLAUDE.md`; indexed here as the nearest ancestor). Read each
on the trigger described:

- [`plans/family_logical.dot`](plans/family_logical.dot) — **shared
  logical plan for S1, S2, S3** (to be refreshed in Phase 0.5; the
  current file targets the stale OL pipeline).
- [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot) —
  **S3 physical specialisation**, COL pipeline (Phase 0.5).
- [`plans/baseline_s4.dot`](plans/baseline_s4.dot) — **S4 baseline**
  (Phase 0.5).
- [`plans/phase_2{a,b,c}.md`](plans/) — historical Q3 phase plans;
  marked stale because they predate the COL-pipeline pivot.

Read [`../q3i/CLAUDE.md`](../q3i/CLAUDE.md) for the canonical
template — Q3 mirrors Q3I structure minus the invoice sibling.

## TPC-H Definition (Section 2.4.3)

```sql
SELECT  l_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority
FROM    customer, orders, lineitem
WHERE   c_mktsegment = ':1'
  AND   c_custkey    = o_custkey
  AND   l_orderkey   = o_orderkey
  AND   o_orderdate  < ':d'
  AND   l_shipdate   > ':d'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

This is the canonical TPC-H Q3, **unextended**. Q3I extends Q3 with
an invoice sibling sub-aggregate (`cust_open_due`); Q3 here is the
no-extension baseline of the COL family.

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:1` | AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY | Customer market segment filter |
| `:d` | Day in [1995-03-01, 1995-03-31] | Order-date upper bound / ship-date lower bound (same date drives both filters) |

**Validation values**: SEGMENT = BUILDING, DATE = 1995-03-15.

**Approved query variants**: none in Appendix B.

**Selectivity notes**: each segment covers ~20% of customers. At the
validation date roughly half of orders pass `o_orderdate < DATE` and
roughly half of lineitems pass `l_shipdate > DATE`.

---

## Motivation

Q3 is the **no-sibling-aggregate baseline of the COL family** — the
3-table pure-hierarchical analogue to Q3I. Where Q3I demonstrates
§3.1.2 (sibling sub-aggregate via invoice) layered on top of §3.1.3
(hierarchical join), Q3 isolates the §3.1.3 hierarchical-prefix
benefit so that Q3I's incremental gains can be attributed cleanly.

- **Why a custkey-keyed COL MI?** With ORDERS extended by `custkey`
  (FD on `orderkey`) and LINEITEM extended by `custkey` (same FD
  chain), the natural prefix hierarchy becomes
  `custkey ⊃ orderkey ⊃ linenumber`. A 3-table merged index keyed by
  this hierarchy co-locates customer, orders, and lineitem records
  per custkey. A single `PremergedJoin` pass produces the full
  3-way join — **no separate CUSTOMER merge join**, unlike the
  legacy Q3 plan that treated MI as `MI(ORDERS, LINEITEM)` and
  joined CUSTOMER at query time.

- **Comparison story**: at high SF the COL MI's per-custkey
  locality should beat both the BMJ chain (S1) and the hash-join
  baseline (S4), with the gap reflecting purely the hierarchical
  scan benefit (no sibling-aggregate confound).

---

## Cardinality structure

Unlike Q3I, **Q3 is a genuine 3-way M:N join** with no shortcut from
sibling decomposition:

- CUSTOMER × ORDERS: 1:N on `c_custkey = o_custkey`
  (~10 orders per customer at SF=1).
- ORDERS × LINEITEM: 1:N on `o_orderkey = l_orderkey`
  (~4 lineitems per order).
- Total join cardinality: ~1.5M customers × 10 × 4 ≈ 60M rows
  at SF=1, before filters.

The MI benefit is hierarchical-prefix scan locality, not branch
elimination. Per-customer, per-order, and per-lineitem filters all
fire inside the walker; nothing is reduced to a scalar before the
join (contrast Q3I's `cust_open_due`).

This is the §3.1.3 hierarchical-M:N showcase that Q3I explicitly
disclaims; the joinN counters that are blank for Q3I S3 should be
populated and meaningful for Q3 S3.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + binary merge join | Custkey-sorted secondary indexes on ORDERS (`(custkey, orderkey)`) and LINEITEM (`(custkey, orderkey, linenumber)`) | BMJ chain: customer ⋈ orders\_sec ⋈ lineitem\_sec on the custkey-extended prefix |
| 2 | Intermediate pipeline view | `q3_pipeline_view_t` (post-filter, post-aggregate; one row per (orderkey, orderdate, shippriority)) | View scan + mktsegment filter + sort/limit |
| 3 | MI[COL] only | `MergedAdapter<customer_col_t, orders_col_t, lineitem_col_t>` keyed by custkey-prefixed tagged keys | `col_group_walk[_fused_emit]` over the COL MI; CUSTOMER hierarchy co-located, no separate join |
| 4 | Traditional indexes + hash join | None | `Scan(customer)` → `Filter(mktsegment)` → `HashJoin(⋈orders)` → `Filter(orderdate)` → `HashJoin(⋈lineitem)` → `Filter(shipdate)` → SortedAggregate → TopN |

**S5 deliberately omitted.** See `§Open Questions` — Q3 has no
parameter-independent aggregate to bake (lineitem revenue is
filtered by `l_shipdate > $DATE`, which IS parameterised), so an
aCOL MI with pre-aggregated columns would either be unsound or
collapse to S3. Q3I's `pre_revenue` shares this dependence (the
existing `views_coli.hpp` definition bakes `l_shipdate >
DATE_1995_03_15` literally) and needs an audit; that work is
tracked in `§Open Questions` and is out of scope for Q3 Phase 0.

---

## Plan Descriptions

Three DOT files in [`plans/`](plans/) document the operator graphs
(refreshed in Phase 0.5 — the current files reference the stale OL
pipeline and are kept only for diff context):

- `plans/family_logical.dot` — shared logical plan for S1, S2, S3.
  All three agree on filter placement, aggregate shape, and TopN
  outside the pipeline; only the inside-pipeline physical operator
  differs (the comparison axis).
- `plans/family_s3_physical.dot` — S3 physical specialisation. A
  single `col_group_walk[_fused_emit]` over the 3-table COL
  MergedAdapter subsumes the per-table filters, the SortedAggregate
  on revenue, and the 2-way join.
- `plans/baseline_s4.dot` — S4 baseline. A HashJoin chain over base
  tables with `mktsegment` pushed down immediately after the
  CUSTOMER scan (before the hash build).

### Filter pushdown principle (applied across all four plans)

See the canonical rule in [Filter Pushdown](../OPERATORS.md#filter-pushdown).
The Q3-specific application:

Every parameterised filter is pushed as far down the operator
graph as possible, **stopping only at secondary structures** so
they remain reusable across param sets (predicate hoisting). For
Q3 this means:

- The COL MI, the COL custkey-sorted secondaries, and the
  `q3_pipeline_view_t` are all loaded **without** applying
  mktsegment, orderdate, or shipdate filters. A new param set
  triggers a new query, not a new load.
- Single-table filters (`c_mktsegment`, `o_orderdate`,
  `l_shipdate`) fuse with their TableScan at query time.
- For S4 specifically, the mktsegment predicate is applied
  **immediately after the CUSTOMER scan, before the hash build**,
  so the hash table only contains qualifying customers.
- For merged-index physical execution (S3), every filter applies
  **during** the group walk, at the Visitor's `on_*` hook for that
  record type. There is no post-walk Filter node.

### How the four approaches differ

**S3 (MI[COL] + COLGroupWalk)** is the tightest expression of the
plan. The COL tagged-key encoding co-locates customer, orders, and
lineitem records by `custkey` in byte-lex order
(`customer → (orders → lineitem*)+`), and the walker streams
through them in a single forward pass. The mktsegment gate at
`on_customer` skips the entire group; per-table date filters are
inline `if` checks at `on_order` / `on_lineitem`; revenue
accumulates per-orderkey inside the visitor. No buffering, no
hashmaps, no separate aggregate pass.

**S1 (custkey-sorted secondaries + BMJ chain)** runs the same
logical plan as S3 over three separate custkey-sorted streams
(CUSTOMER + two secondary indexes on ORDERS and LINEITEM). The
secondary keys (`(custkey, orderkey)` for orders,
`(custkey, orderkey, linenumber)` for lineitem) place both inputs
in the order required by the BMJ chain. Implemented as a 3-way
streaming merge that dispatches to the **same Visitor** as S3.
The accumulators are reused verbatim. S1 differs from S3 only in
I/O pattern: three trees instead of one, three scanner-advance
calls per group instead of one. This makes the S1-vs-S3 comparison
apples-to-apples per OPERATORS.md §6.1 — same logical plan, same
accumulator code, same filter pushdown, only the physical scan
substrate differs.

**S2 (materialised pipeline view)** caches the post-aggregate
output of the family logical plan as a `q3_pipeline_view_t` table
at load time. The view is loaded with the per-table date filters
fused (since at view-load time those filter values must be picked)
but **without** the mktsegment filter (predicate hoisting), so it
is reusable across SEGMENT param sets at fixed DATE. Query time is
then a sequential view scan with the mktsegment filter applied
per row, then sort + limit. **Open question (§9)**: should the
view be loaded fully unfiltered (date filters applied at query
time) so it is reusable across both SEGMENT and DATE? This is the
same dilemma S5 surfaces — defer until Phase 2 design.

**S4 (HashJoin chain baseline)** uses the standard plan because
nothing is custkey-sorted. Mktsegment fuses with the CUSTOMER
scan immediately, before the hash build; orderdate fuses with
ORDERS pre-build; shipdate fuses with LINEITEM pre-build. Probe
output flows into a SortedAggregate by `(orderkey, orderdate,
shippriority)`, then sort by `(revenue DESC, orderdate ASC)` +
LIMIT 10. S4 measures the no-merged-index baseline that the
family is compared against.

### Comparison axis summary

| Approach | Inside-pipeline physical | Filters resolved by |
|----------|--------------------------|---------------------|
| S1 (merge family) | 3-way custkey BMJ chain over secondaries | Visitor `on_*` hooks (same code as S3) |
| S2 (merge family) | sequential view scan | Date filters baked into view at load; mktsegment per-row at query time |
| S3 (merge family) | `col_group_walk` over MI[COL] | Visitor `on_*` hooks (same code as S1) |
| S4 (baseline) | HashJoin chain | TableScan-time filters (mktsegment, orderdate, shipdate) all pushed below the corresponding hash build/probe |

All four agree on what's outside the pipeline: `apply_top10` (sort
by `(revenue DESC, orderdate ASC)` + truncate to 10).

---

## Required Record Types

These types do not exist yet — they will be created in the
`views_col.hpp` + COL-pipeline infrastructure task that gates Q3
Phase 0.5. Names mirror the COLI analogs:

- `customer_col_t` — tagged record, sentinel id (TBD; analog to
  `customer_coli_t`'s id=21), Key `(c_custkey)`. Payload =
  `customerh_t` fields needed by Q3 (at minimum `c_custkey`,
  `c_mktsegment`).
- `orders_col_t` — tagged record, Key `(custkey, orderkey)`. FD:
  `orderkey → custkey`. Payload = `orders_t` fields needed by Q3
  (at minimum `o_custkey`, `o_orderkey`, `o_orderdate`,
  `o_shippriority`).
- `lineitem_col_t` — tagged record, Key
  `(custkey, orderkey, linenumber)`. FD: same chain. Payload =
  `lineitem_t` fields needed by Q3 (`l_orderkey`, `l_linenumber`,
  `l_extendedprice`, `l_discount`, `l_shipdate`).

For S1 (split secondaries):

- `orders_sec_t` — un-tagged secondary, Key `(custkey, orderkey)`.
- `lineitem_sec_t` — un-tagged secondary, Key
  `(custkey, orderkey, linenumber)`.

For S2 (pipeline view):

- `q3_pipeline_view_t` — Key `(orderkey, orderdate, shippriority)`,
  payload `(custkey, sum_revenue)`. One row per qualifying order
  group.

Final output:

- `q3_agg_row_t` — Key `(orderkey, orderdate, shippriority)`,
  payload `revenue` (already declared in current `q3/views.hpp`;
  preserve).

**Sentinel ordering note**: in Q3I the COLI sentinels are
`customer=1 < invoice=2 < orders=3 < lineitem=4` so invoice
finalises before O×L within a custkey group. Q3 has no invoice
sibling, so the natural order is `customer=1 < orders=2 <
lineitem=3` (keep the invoice slot reserved at id=2 if we want
COL to be a strict subset of COLI — discuss in §Open Questions).

---

## Open Questions

These resolve before Phase 0.5. Each is an explicit design
checkpoint, not an implementation detail.

### S5 viability — drop or rebuild

The lineitem revenue field is parameterised by
`l_shipdate > $DATE`. In Q3I, `views_coli.hpp` defines:

```cpp
// orders_acoli_t.pre_revenue =
//   SUM(l_extendedprice*(1-l_discount) WHERE l_shipdate>DATE_1995_03_15)
```

So Q3I S5 is **already shipdate-locked** to the validation value
— it is not reusable across DATE param sets. Q3 inherits the same
constraint. Resolutions:

1. **Drop S5 from Q3** (current plan default). aCOL collapses to
   COL because Q3 has no parameter-independent aggregate to bake.
2. **Build S5 with unaggregated lineitems**, applying the
   shipdate filter and SUM at read time over the per-customer
   lineitem set. This is what the user pushed back on — store
   lineitems as-is, do not pre-aggregate when the aggregate
   depends on a parameterised filter.

If we adopt (2) for Q3, **the same fix applies to Q3I S5**: its
current `pre_revenue` is unsound for any SEGMENT/DATE param set
other than the validation pair. Phase 0 flags this as a Q3I
audit; Q3 itself defers S5 design until that audit lands.

### S2 view granularity

Post-aggregate (one row per qualifying order, much smaller) vs
post-join (one row per qualifying lineitem, parallels Q3I S2 which
chose post-join). Q3I's choice was driven by needing per-lineitem
shipdate visibility for the `cust_open_due` threshold gate; Q3 has
no such constraint, so post-aggregate may be the simpler choice.
Decide before Phase 2.

### COL pipeline naming

Confirm `col_pipeline.{hpp,tpp}` and
`CustomerOrdersLineitemPipeline<Backend>` as the analog to
`coli_pipeline` / `COLIPipeline`. Names propagate into the walker
(`col_group_walk[_fused_emit]`) and load helpers
(`populate_merged`, `populate_split`).

### COL as a strict subset of COLI

Should `customer_col_t` reuse `customer_coli_t`'s sentinel id and
Key shape (so the COL MI is byte-compatible with a COLI MI minus
invoice)? If yes, future Q3 → Q3I migration becomes free; if no,
COL is independent and may diverge. Recommend strict-subset
naming/encoding with a TODO in the file header.

### Q3I refactor (later, out of scope)

Once COL pipeline exists, can Q3I's COLI be defined as
`COL + invoice` extension? Worth flagging but no decision in Phase 0.

---

## Implementation Phases (preview — full detail in Phase 0.5 plan)

- **Phase 0** (this doc) — design.
- **Pre-Phase-0.5 dependency**: build COL pipeline infrastructure
  (`views_col.hpp` + `col_pipeline.{hpp,tpp}`). This is a
  **separate task** that must land before Q3 Phase 0.5 can begin.
  Cleanest delivery: build COL infra and Q3 skeleton in the same
  PR family.
- **Phase 0.5** — skeleton commit. Replace OL-pipeline references
  in `q3/views.hpp`, `workload.hpp`, `load.tpp`, `query.tpp`,
  `per_structure_workload.hpp` with COL types. Refresh DOT files.
  No real `query_by_*` bodies yet — stubs only.
- **Phase 1** — minimal end-to-end S3 (`query_by_merged`) +
  `test_query_q3_lsm` digest seed.
- **Phase 2** — S1, S2, S4 baselines + cross-structure parity.
- **Phase 3** — production `q3_lsm` / `q3_btree` executables,
  CMake targets, `generate_targets.py` entries.
- **Phase 4** — defer pending S5 viability decision (§Open Q).

Mirror Q3I's [Implementation Phases](../q3i/CLAUDE.md#implementation-phases)
for full structure once Phase 0.5 starts.

---

## Stale skeleton — to be replaced in Phase 0.5

The current `q3/` skeleton predates the COL-pipeline pivot. It
will be rewritten, not merely patched, in Phase 0.5:

- `q3/views.hpp` includes `views_ol.hpp` and aliases
  `q3_pipeline_view_t = ::tpch::joined_ol_t`. Both lines go away.
- `q3/workload.hpp` composes `OrdersLineitemPipeline<Backend> ol`.
  Replace with `CustomerOrdersLineitemPipeline<Backend> col`.
- `q3/load.tpp` references `ol.populate_view` /
  `ol.get_view_size` (already noted as TODO debt in
  `frontend/tpch/CLAUDE.md`). Both go away.
- `q3/query.tpp` predicate signatures take `joined_ol_t` —
  replace with the COL join-result type or with the visitor
  hook signatures from `col_group_walk`.
- `q3/plans/*.dot` — refresh to reflect COL pipeline.

Cross-cutting refactor (operator drivers, view loading, build
wiring): see [`frontend/tpch/CLAUDE.md`](../CLAUDE.md).
