# Q3: Shipping Priority

**Reading guide**: For SQL and plan descriptions, read §TPC-H Definition and §Plan Descriptions. For implementation status and open questions, read §Implementation Phases and §Open Questions. For OutClass / wildcard / anti-pattern details, see `../CONVENTIONS.md`. Skip the rest unless reconstructing a historical decision.

## Sibling Docs

Every non-`CLAUDE.md` Markdown in `q3/` and `q3/plans/` (the latter
has no `CLAUDE.md`; indexed here as the nearest ancestor). Read each
on the trigger described:

- [`plans/family_logical.dot`](plans/family_logical.dot) — **shared
  logical plan for S1, S2, S3**, COL pipeline.
- [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot) —
  **S3 physical specialisation** over the COL MI.
- [`plans/baseline_s4.dot`](plans/baseline_s4.dot) — **S4 baseline**
  (HashJoin chain with mktsegment pushed below customer scan).

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

Q3 is a **pure hierarchical schema** — no sibling table, no sibling
aggregate. CUSTOMER, ORDERS, and LINEITEM form a strict 3-level
prefix chain along `custkey ⊃ orderkey ⊃ linenumber`:

- CUSTOMER × ORDERS: 1:N on `c_custkey = o_custkey`
  (~10 orders per customer at SF=1).
- ORDERS × LINEITEM: 1:N on `o_orderkey = l_orderkey`
  (~4 lineitems per order).
- Total join cardinality: ~1.5M customers × 10 × 4 ≈ 60M rows
  at SF=1, before filters.

Contrast with Q3I, which adds INVOICE as a *sibling* of ORDERS under
CUSTOMER and reduces it to a per-custkey scalar (`cust_open_due`)
before the main join — the §3.1.2 sibling sub-aggregate pattern. Q3
has no such sibling. The MI benefit here is purely hierarchical-prefix
scan locality (§3.1.3): per-customer, per-order, and per-lineitem
filters all fire inside the walker; the chain itself is the entire
plan.

This makes Q3 the cleanest §3.1.3 hierarchical-prefix showcase in the
Tier-1 set; the joinN counters that are blank for Q3I S3 should be
populated and meaningful for Q3 S3.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + binary merge join | Custkey-sorted secondary indexes on ORDERS (`(custkey, orderkey)`) and LINEITEM (`(custkey, orderkey, linenumber)`) | BMJ chain: customer ⋈ orders\_sec ⋈ lineitem\_sec on the custkey-extended prefix |
| 2 | Intermediate pipeline view | `q3_pipeline_view_t` (per-lineitem rows keyed by `(custkey, orderkey, linenumber)`; carries `l_extendedprice`, `l_discount`, `l_shipdate` + FD-attached order columns) | View scan + mktsegment + orderdate + shipdate filters live; per-orderkey revenue accumulator + sort/limit |
| 3 | MI[COL] only | `MergedAdapter<customer_col_t, orders_col_t, lineitem_col_t>` keyed by custkey-prefixed tagged keys | `col_group_walk[_fused_emit]` over the COL MI; CUSTOMER hierarchy co-located, no separate join |
| 4 | Traditional indexes + hash join | None | `Scan(customer)` → `Filter(mktsegment)` → `HashJoin(⋈orders)` → `Filter(orderdate)` → `HashJoin(⋈lineitem)` → `Filter(shipdate)` → SortedAggregate → TopN |

**S4 transient hash-table working set (DRAM-budget caveat).** S4 builds
three transient containers per `query_by_hash` invocation that S1/S2/S3
do not: `cust_set` (qualifying customers post-mktsegment),
`orders_map<orderkey, OrderSlot>` (qualifying orders post-orderdate ∧
custkey-membership), and `qualifying_orderkeys` (sorted driver for the
inverted-lineitem seek pass). Measured at SF=1 (BUILDING / 1995-03-15):
**9092 bytes ≈ 0.0087 MiB** (LSM backend; BTree 7900 B). Implication:
in fair-comparison perf sweeps, S4's `--dram_gib` budget should be
reduced by this amount to keep total-memory parity with S1/S2/S3
(which build no transient containers). Working-set scales ~linearly
with qualifying-customer count, so at SF=N the budget reduction grows
~N× — e.g. extrapolating from SF=1 (≈30 of 150 BUILDING customers
qualifying): SF=300 ≈ 2.7 MiB, SF=600 ≈ 5.4 MiB, SF=1500 ≈ 13.5 MiB.
The `s4_hashtable_bytes` counter in `Q3FamilyStats` (set by
`query_by_hash` in `q3/query.tpp`) surfaces this empirically per query
and is printed by the `test_query_q3_{lsm,btree}` harnesses.

**S5 deliberately omitted.** Q3 has no parameter-independent
aggregate to bake — Q3I's S5 keeps `pre_open_due` (invoice
`status='O'` is hardcoded by spec, so the aggregate is genuinely
param-independent), but Q3 has no invoice. The Q3I-S5 audit landed
2026-05-03: `pre_revenue` was dropped, lineitems are now stored
unaggregated in the aCOLI MI, revenue is recomputed at query time.
With that fix in place, an aCOL MI for Q3 would carry only the
3 base record types — i.e., it would equal S3 (the COL MI) with
no extra information. So S5 collapses into S3 for Q3 and is
omitted on those grounds, not on soundness grounds.

---

## Plan Descriptions

Three DOT files in [`plans/`](plans/) document the operator graphs:

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

**S2 (materialised pipeline view)** caches per-lineitem rows of
the post-join (pre-aggregate) family plan as a
`q3_pipeline_view_t` table at load time, keyed by
`(custkey, orderkey, linenumber)` and carrying
`l_extendedprice`, `l_discount`, `l_shipdate` plus FD-attached
order columns. **No filters are baked into the view** —
mktsegment, orderdate, and shipdate are all parameterised, so
predicate hoisting forbids fusing them at load time (the same
audit that drove the Q3I S5 rebuild). Query time is a sequential
view scan: filters apply live, a per-orderkey
`LineitemRevenueAccumulator` rolls up revenue, then sort + limit.
The view is reusable across all SEGMENT and DATE param sets.

**S4 (HashJoin chain baseline)** uses base tables only (nothing
is custkey-sorted). Filter pushdown: mktsegment fuses with the
CUSTOMER scan before the hash build; orderdate fuses with ORDERS
pre-build; shipdate fuses with LINEITEM pre-scan. **Aggregation
push-down mirrors the family logical plan**: revenue per
orderkey is computed directly above the filtered LINEITEM scan
via a `HashAggregate` (lineitem stream is unsorted in S4, so
not a SortedAggregate), shrinking the join input from ~4
lineitems/order to one partial-revenue tuple/order. The HashJoin
chain then runs over the aggregated stream — `HashJoin(o_orderkey
= l_orderkey)` attaches `o_orderdate / o_shippriority /
o_custkey`, then `HashJoin(c_custkey = o_custkey)` gates by
mktsegment. Output flows directly into the shared `apply_top10`
(sort by `(revenue DESC, orderdate ASC)` + LIMIT 10). S4 differs
from S1/S3 only in physical operators (HashAggregate +
HashJoin instead of SortedAggregate + walker/BMJ); the logical
shape — aggregate-first, join-up — is identical, preserving
OPERATORS.md §6.1 comparison-integrity. S4 measures the
no-merged-index baseline that the family is compared against.

### Comparison axis summary

All four plans share the same logical shape (aggregate-first,
join-up); they differ only in physical operators and filter
substrate.

| Approach | Inside-pipeline physical | Aggregate | Filters resolved by |
|----------|--------------------------|-----------|---------------------|
| S1 (merge family) | 3-way custkey BMJ chain over secondaries | SortedAggregate per orderkey (Visitor `flush_order`) | Visitor `on_*` hooks (same code as S3) |
| S2 (merge family) | sequential per-lineitem view scan | SortedAggregate per orderkey (view rows are `(custkey, orderkey, linenumber)`-sorted) | All filters live at query time (no filter baking — view is param-reusable) |
| S3 (merge family) | `col_group_walk` over MI[COL] | SortedAggregate per orderkey (Visitor `flush_order`, same code as S1) | Visitor `on_*` hooks (same code as S1) |
| S4 (baseline) | HashJoin chain over base tables | HashAggregate per orderkey, **above the LINEITEM scan, below both joins** | TableScan-time filters (mktsegment, orderdate, shipdate) all pushed below the corresponding hash build/probe |

All four agree on what's outside the pipeline: a single
`TopNSink<q3_agg_row_t, Cmp>(10, cmp)` that the pipeline pushes
into via `sink.offer(...)`, drained at the end via
`sink.drain_sorted(out)`.  See OutClass subsection below.

### OutClass: shared `TopNSink<q3_agg_row_t, Cmp>`

All four `query_by_*` bodies construct one
`TopNSink<q3_agg_row_t, decltype(cmp)>(10, cmp)` per query (where
`cmp` wraps `q3_family::q3_agg_row_base_t::cmp` —
`revenue DESC, o_orderdate ASC, o_orderkey ASC`).  The sink's
internal heap holds at most 10 entries; `sink.offer(row)` is
O(log 10) and evicts losers.  After the pipeline runs,
`sink.drain_sorted(out)` materialises the 10 result rows.

This is Q3's instance of the **OutClass** convention codified in
`CONVENTIONS.md §Post-pipeline OutClass`: a small-buffer sink owning the
pipeline → result boundary, push-once-per-row, drained once.
Memory is `O(K) = O(10)`, not `O(orders passing all filters)`.
Pre-`458bcaf0` Q3 buffered every qualifying order in a
`std::vector<q3_agg_row_t>& out` (~150K rows at SF=1, ~6M at
SF=40) before calling `apply_topN` — see `CONVENTIONS.md §Anti-Pattern Reference` #30.

The visitor (`COLGroupWalkVisitor<Sink>`) is template-on-Sink
because `Q3FamilyVisitor` now takes the sink type as a template
parameter rather than holding a `std::vector<AggRow>&` — the
visitor doesn't know or care about heap mechanics, just calls
`sink.offer(std::move(row))` from `flush_order`.  Q3I uses the
same shape with `q3i_agg_row_t`; `NNameRevenueAggregator` plays
the analogous role for Q5 (no LIMIT; HashAggregate IS the
OutClass).  Per OPERATORS.md §7 rule 5, the same OutClass
instance is shared across all four storage variants for
comparison-integrity at the post-pipeline boundary.

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

- `q3_pipeline_view_t` — Key
  `(custkey, orderkey, linenumber)`, one row per lineitem.
  Payload: `l_extendedprice`, `l_discount`, `l_shipdate`,
  `c_mktsegment`, `o_orderdate`, `o_shippriority`. No
  pre-aggregated `revenue` field (parameterised by shipdate;
  same audit as Q3I S2). Mirrors Q3I `q3i_pipeline_view_t`
  post-2026-05-03 redesign.

Final output:

- `q3_agg_row_t` — Key `(orderkey, orderdate, shippriority)`,
  payload `revenue` (already declared in current `q3/views.hpp`;
  preserve).

**Sentinel ordering note**: COL uses strict-subset ordering —
COLI domain tags are reused verbatim: `customer=1`, `invoice=2`
(reserved, unused), `orders=3`, `lineitem=4`. The reserved invoice
slot keeps prefix bytes aligned with COLI, enabling byte-compatible
keys and future dual-format scanner reuse. Resolved 2026-05-03; see
§Open Questions — COL domain-tag allocation.

---

## Open Questions

### COL domain-tag allocation: strict subset (resolved 2026-05-03)

COL reuses `coli_domain_tag` verbatim: `index=0`, `customer=1`,
`invoice=2` (reserved, unused), `orders=3`, `lineitem=4`. The invoice
slot stays reserved even though no invoice records exist in COL.

Why strict subset: byte-compatibility with COLI keys lets COL reuse
`customer_coli_t` and `orders_coli_t` verbatim (same Key shape,
same tagged-path encoding), and keeps prefix bytes aligned for any
future dual-format scanner. `lineitem_col_t` (idx=36) is the only
new type — its Key omits the `invoicekey` segment present in
`lineitem_coli_t`. Implemented in `views_col.hpp`; load-tested via
`test_load_col_lsm` at SF=1 (all [OK]).

### S5: omit (resolved)

The Q3I S5 audit landed 2026-05-03: `pre_revenue` was dropped,
lineitems are stored unaggregated in the aCOLI MI, revenue is
recomputed per query. With the soundness issue fixed in Q3I,
Q3's S5 question reduces to: would an aCOL MI carry any
information not already in the COL MI? It would not — Q3 has
no invoice and therefore no `pre_open_due`-style
parameter-independent aggregate. **S5 is omitted for Q3 because
it adds no information beyond S3, not because it would be
unsound.**

**Workload follow-up (raised by user)**: the realistic Q3 / Q3I
workload should vary SEGMENT and DATE across query invocations
within a single executable run, not pin them to validation
values. The current Q3I executable uses `Params::defaults()` —
fixed BUILDING / 1995-03-15. That fixed regime is precisely why
the `pre_revenue` shipdate-baking bug went undetected for as
long as it did. Tracked as a separate Q3I executable upgrade;
Q3 should follow the same convention from day one (Phase 1+
harness drives a sequence of param sets per run, not just
defaults).

### S2 view granularity: per-lineitem (resolved)

Same logic as S5: shipdate is parameterised, so any
pre-aggregated revenue forces a load-time filter bake — exactly
the bug the Q3I S2 rebuild fixed. Q3 S2 stores per-lineitem
rows keyed by `(custkey, orderkey, linenumber)` with
`l_extendedprice`, `l_discount`, `l_shipdate` and FD-attached
order columns; revenue is recomputed at query time. The view is
reusable across all SEGMENT and DATE param sets.

### COL pipeline naming

`col_pipeline.{hpp,tpp}` and
`CustomerOrdersLineitemPipeline<Backend>` are the analogs to
`coli_pipeline` / `COLIPipeline`. Names propagate into the
walker (`col_group_walk[_fused_emit]`) and load helpers
(`populate_merged`, `populate_split`). Confirmed.

### COL ↔ COLI relationship: composition, not extension

Q3 and Q3I should share record types, walkers, accumulators,
and pipeline helpers wherever the shape is identical (for DRY:
a fix in one place doesn't have to be applied twice). That is
**composition** — both queries use the same building blocks —
not **inheritance** or "Q3I extends Q3". Concretely: aim for
`customer_col_t` ≡ `customer_coli_t` minus invoice
considerations (likely a strict-subset Key shape and shared
sentinel id 1), `lineitem_col_t` ≡ `lineitem_coli_t` minus
`l_invoicekey`-aware sentinel ordering, and
`LineitemRevenueAccumulator` reused verbatim across S1/S2/S3 of
both queries. Where a piece truly differs (Q3I's
`CustomerOpenDueAccumulator`, Q3I's invoice walker hook), it
lives in Q3I-only files. Phase 0.5 will identify the exact
shareable surface.

---

## Implementation Phases

- **Phase 0** (2026-05-03) — design doc.
- **Phase 0.5** (2026-05-03; **complete**) — skeleton over COL
  pipeline. `views.hpp` defines `q3_pipeline_view_t` and aliases
  `q3_agg_row_t`. `workload.hpp` composes
  `CustomerOrdersLineitemPipeline<Backend> col`. `load.tpp` has
  real `populate_q3_view` / `populate_split` / `populate_merged`
  bodies. `query.tpp` has real `Params::defaults()`,
  `set_params_for_iter`, all four predicate implementations, and
  Phase-0.5 stub `query_by_*` returning empty vectors. All four
  storage structures wire cleanly; `test_query_q3_lsm` loads and
  reports `[OK]` parity at digest 0x0. CMake targets `q3_lsm` /
  `q3_btree` and `test_query_q3_lsm` / `test_query_q3_btree` wired
  in `frontend/CMakeLists.txt`. `generate_targets.py` updated.
- **Phase 4 §7.1** (2026-05-04; **complete**) — minimal end-to-end S3
  (`query_by_merged`) using `col_group_walk` + a trivial
  `COLGroupWalkVisitor` subclass of `q3_family::Q3FamilyVisitor`
  (Q3 uses all base no-op CRTP defaults). `apply_topN` enforces
  `revenue DESC, o_orderdate ASC, o_orderkey ASC` per shared
  `q3_family::q3_agg_row_base_t::cmp`. `test_query_q3_lsm` at SF=1
  reports S3 rows=7, deterministic digest, S1/S2/S4 stubs reported
  as `[SKIP]` until §7.2/§7.3/§7.5 land. Exit 0. As a side effect,
  `mi_records_visited` / `mi_groups_skipped` were hoisted from
  `Q3IStats` to `q3_family::Q3FamilyStats` since the shared visitor
  bumps them and Q3 had no equivalent declarations.
- **Phase 4 §7.3 + §7.5** (2026-05-04; **complete**) — S2 view scan and
  S4 hash baselines.  S2 streams `q3_pipeline_view_t` per-lineitem,
  evaluates mktsegment + orderdate once per orderkey transition, and
  applies shipdate per-lineitem via the shared
  `LineitemRevenueAccumulator` (with a `lineitem_t` proxy carrying the
  three fields it reads, mirroring Q3I).  S4 builds a qualifying-customer
  set, then an orders map gated by `(orderdate ∧ custkey ∈ cust_set)`,
  then probes lineitems and folds revenue into the OrderSlot.  Both
  paths reach `apply_topN(out, 10, q3_family::q3_agg_row_base_t::cmp)`.
- **Phase 4 §7.2** (2026-05-04; **complete**) — S1 BMJ chain over
  custkey-sorted COL split indexes (Track 1 — 2 BMJs vs Q3I's 3, no
  invoice).  `LineitemRevenueAggregator` and `lineitem_agg_t` were
  hoisted from `q3i/` to `q3_family/` (templated on `LineitemType`)
  so Q3 (`lineitem_col_t`) and Q3I (`lineitem_coli_t`) share one
  scanner-wrapper implementation.  Q3-specific `q3_cust_jk_t::Key`,
  `q3_jr1_t`, `q3_jr2_t` join-result types live in `q3/views.hpp`
  with their `SKBuilder` and `std::hash` specialisations.
  `test_query_q3_{lsm,btree}` harness flipped from `[SKIP]` tolerance
  to strict cross-structure XOR parity.  SF=1: all four paths produce
  identical digest, rows=10, exit 0.  Q3I parity preserved post-hoist
  (test_query_q3i_lsm rows=9, all five S1–S5 `[OK]`).
- **Phase 5–8** (2026-05-03/04; **complete**) — `per_structure_workload.hpp`
  alias-only file, `executable_rocksdb.cpp` / `executable_leanstore.cpp`,
  `test_query_q3_{lsm,btree}` harness (strict parity), and CMake +
  `generate_targets.py` entries (`q3_lsm` / `q3_btree`, structures 1–4)
  are all wired and verified.
- **Phase 9** (in progress) — documentation refresh propagating Phase 4
  completion across project docs; cross-link to project-wide S5
  deferral in `frontend/tpch/PLAYBOOK.md §S5`.
- **S5** — omitted by design (see §Open Questions); aligned with the
  project-wide deferral documented in `PLAYBOOK §S5`.
- **Linux perf sweep** — pending, tracked in `LINUX_PENDING.md`.
