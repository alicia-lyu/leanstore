# Q10: Returned Item Reporting

**Reading guide**: For SQL and plan descriptions, read §TPC-H Definition and §Plan Descriptions. For implementation status and scope, read §Status and §Implementation Phases. For decisions D1–D8 that shape the implementation, read §Locked-In Design Decisions. Skip the rest unless reconstructing a design choice or adding a new storage structure.

## Status

**Phase 4 + 5 complete — all four `query_by_*` paths live + **S5 aCOL
MI** (`query_by_aggregated`, D8 revised 2026-05-25); SF=1 macOS
strict 4-way XOR parity verified across two distinct param sets
(iter=0 default + iter=1 off-default param-bake guard). Linux 5L
perf sweep done both backends (2026-05-25, `RUNS.md`); **S5 aCOL is
the fastest structure on both backends** (btree 160 ms ≪ S2-preagg
17.4 s; LSM 1.37 s < S2-preagg 1.66 s). A follow-up perf
investigation added two A/B variants — see
[`PERFORMANCE.md`](PERFORMANCE.md).** The btree S2 "regression"
(174 s) was a strawman per-lineitem view; the fair **per-order
pre-aggregated view** (`q10_pipeline_view_preagg_t`,
`--q10_view_variant=preagg`) is 10×/10.4× faster (btree/LSM) and the
query-time winner on both. S3<S1 on btree is the filter-hierarchy
effect (Q10's only prune is below the COL co-location grain); the
**physical SkipOrder seek** (`--skip_order_physical=1`) cuts records
visited 4× but not btree page IO (neutral), while giving LSM a clean
+9%. Both variants are parity-gated in `test_query_q10_*`. S3 lands the bespoke
`Q10GroupWalkVisitor` (D1) on `col_group_walk` plus the canonical
`Q10QuerySink` wrapping `TopNSink<q10_agg_row_t, &q10_agg_row_t::cmp>`.
The walker also drives the Pattern B view loader via compile-time
`Q10FilterMode` dispatch (view-vs-walker drift structurally impossible).
S1 reads the custkey-sorted COL split secondaries via a nested forward
scan (functionally equivalent to a 2-BMJ chain — same access pattern,
same emit cardinality). S2 consumes the pipeline view via the D4
anomaly chain (returnflag filter → SUM-per-orderkey → orderdate filter
→ SUM-per-c_custkey). S4 builds PK-only `cust_set` + `orders_set` and
probes LINEITEM with sorted-seek + per-orderkey-transition INL recovery
of `c_custkey` and the customer record (Rules 4 / 13). All four paths
fold lineitems through `q10_admit_lineitem_from_join` /
`q10_admit_revenue_from_join` into the shared per-customer
`Q10PerCustomerAggregator`; `q10_finalize_aggregator` resolves
`n_name` via per-customer NATION INL on PK (D6, ≤25-entry cache) and
offers each `q10_agg_row_t` to the bounded TopN(20) sink.
`test_query_q10_lsm` reports `[OK]` across the splits + Pattern B view
+ per-type merged_col breakdown + sentinel ordering + 4-way parity
(S1 == S2 == S3 == S4 at matching non-zero digest, 20 rows each).
Experimental scope is the **5L cell only** (largest data, low memory
pressure — the headline cell per `paper-data/scripts/PLOTTING.md`).
Not committing to the full SF×DRAM sweep. The invoice-extended
sibling lives in [`../q10i/`](../q10i/CLAUDE.md); Q10 is the
no-extension baseline of the COL family. See §Contingency for the
design-only-future-work fallback if Phase 4 bodies / 5L results do
not land in time.

## Sibling Docs

Every non-`CLAUDE.md` Markdown in `q10/` and `q10/plans/` (the
latter has no `CLAUDE.md`; indexed here as the nearest ancestor).
Read each on the trigger described:

- [`plans/family_logical.dot`](plans/family_logical.dot) — **shared
  logical plan for S1, S2, S3**, COL pipeline with NATION attached
  as a per-customer INL on PK at emit time (no in-memory map).
- [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot) —
  **S3 physical specialisation** over the COL MI: bespoke Q10
  visitor on `col_group_walk` with incremental top-20 emit.
- [`plans/baseline_s4.dot`](plans/baseline_s4.dot) — **S4 baseline**
  (HashJoin chain; build on CUSTOMER, probe with filtered ORDERS,
  then build on C⋈O and probe with filtered LINEITEM).
- [`RUNS.md`](RUNS.md) — perf-run ledger; appended after every
  Linux sweep.
- [`AUDIT_MAY_25.md`](AUDIT_MAY_25.md) — read **when reviewing
  end-of-implementation drift**. End-of-implementation sanity audit
  conducted 2026-05-25 after the Linux 5L sweep landed and S5 aCOL
  came online; records the two MED doc-drift findings fixed in the
  same commit (workload.hpp S5-omitted comment + §Status S5
  headline) plus the clean gates (param-rotation guard, 5-way
  parity, Rule 4 / 13 sites).
- [`PERFORMANCE.md`](PERFORMANCE.md) — read **when interpreting the
  btree S2 / S3 numbers**. Records the investigation into the 5L
  anomalies (S2 174 s regression; S3 < S1): the S2 per-lineitem view
  was a strawman (the fair per-order pre-aggregated view, variant B,
  is 855× faster); the S3 < S1 btree gap is the filter-hierarchy
  effect (Q10's only prune is below the co-location grain) and a
  physical SkipOrder seek cuts CPU but not the page-bound gap. A/B
  knobs: `--q10_view_variant`, `--skip_order_physical`.

Read [`../q5/CLAUDE.md`](../q5/CLAUDE.md) as the closest cousin —
Q10 mirrors Q5's COL-pipeline structure and reuses the same
`CustomerOrdersLineitemPipeline<Backend>`, custkey-sorted
secondaries, and `lineitem_col_t` (which already carries
`l_returnflag` as a Q10 placeholder). Read
[`../q10i/CLAUDE.md`](../q10i/CLAUDE.md) for the Track-2 sibling
that adds INVOICE sub-aggregates (open / late balance).

## TPC-H Definition (§2.4.10 — "Returned Item Reporting")

```sql
SELECT  c_custkey, c_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        c_acctbal, n_name, c_address, c_phone, c_comment
FROM    customer, orders, lineitem, nation
WHERE   c_custkey   = o_custkey
  AND   l_orderkey  = o_orderkey
  AND   o_orderdate >= ':d'
  AND   o_orderdate <  ':d' + INTERVAL '3' MONTH
  AND   l_returnflag = 'R'
  AND   c_nationkey  = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC;
```

The official query is run with **`LIMIT 20`** for top-20 customers.

This is the canonical TPC-H Q10, **unextended**. Q10I extends Q10
with INVOICE sibling sub-aggregates (open / late balance); Q10
here is the no-extension baseline of the COL family.

### Substitution Parameters

| Parameter | Domain | Description | Validation |
|-----------|--------|-------------|------------|
| `:d` | First day of a month between 1993-02-01 and 1995-01-01 | Start of the 3-month order window | `1993-10-01` |

**Approved query variants**: none in TPC-H Appendix B.

**Selectivity notes**: NATION has 25 rows; Q10 applies **no**
filter on NATION (no region clause). The orderdate window is 3
months — roughly 1/24 of orders fall in any given window at
SF=1. `l_returnflag = 'R'` selects ~25% of lineitems and is the
dominant per-lineitem pruner. Customers are ~150K at SF=1; the
per-customer aggregate spans all customers (no per-customer
predicate), so the result is at most `LIMIT 20` out of ~150K
candidate aggregate rows.

### Real-world meaning

Q10 finds the customers who have returned the most goods (by
returned-lineitem revenue) over a 3-month window, along with
their contact info. The intent is operational customer service:
if a customer is returning a lot of merchandise, the account
manager should be flagged so they can reach out and understand
what's going wrong (product quality, mis-shipments, fraud,
dissatisfaction with fit, etc.). The output drives a "follow up
with these customers" action list, not a strategic decision.

---

## Motivation

Q10 is a **Track-1 §3.1.3 hierarchical-prefix showcase** of a
shape no other Q-family query covers: a **per-customer top-N**
over a narrow date window. Q3 is per-order top-N; Q5 is per-nation
aggregate (no top-N); Q12 is per-shipmode aggregate. Q10 fills the
remaining grouping shape — a top-N where the group key matches the
COL MI's leading sort key (custkey) exactly, so the walker emits
one aggregate row per custkey group and feeds them directly into
a bounded top-20 sink. **Cross-reference §Cardinality structure
framing #1: pure hierarchical along the COL chain.**

What Q10 adds beyond Q5:

- **Group key matches the MI sort key.** Q5 groups by `n_name`
  (uncorrelated with custkey); Q10 groups by `c_custkey` (the COL
  MI's leading key). One aggregate row finalises per custkey group
  during the walk — no in-walker hashmap of partial aggregates.
- **Top-N inside the walker.** Q5 has no LIMIT (drains ~5 buckets);
  Q3 has `LIMIT 10` but groups per-order (top-10 inside each
  custkey group). Q10 maintains a single bounded top-20 sink across
  the entire walk — see Decision D2.
- **Wide FD-attached output.** The GROUP BY carries 7 non-aggregate
  customer/nation columns. These ride along on the per-customer
  state in S3, on the lineitem-keyed view rows in S2, on the
  HashJoin build payloads in S1/S4, and are resolved against
  NATION via per-customer INL at emit time (Decision D6).
- **No per-customer predicate.** Q3 has the c_mktsegment gate; Q5
  has the c_nationkey ∈ nation_set gate. Q10 has neither — every
  customer enters the aggregate; the orderdate window and
  returnflag predicate do all the pruning.

The MI advantage thesis is the same as Q3 / Q5: `MI[COL]`
co-locates the chain in custkey-byte-lex order so a single
`PremergedJoin` pass produces the full 3-way join, and the
per-customer aggregate naturally aligns with the walker's group
boundary. Paper-axis framing: 5L-cell showcase that the COL MI's
per-customer locality matches what Q10's GROUP BY needs.

---

## Cardinality structure

Q10 is a **pure hierarchical schema along the COL chain** —
framing #1 from PLAYBOOK §3.5 step 4. CUSTOMER, ORDERS, LINEITEM
form a strict 3-level prefix chain along
`custkey ⊃ orderkey ⊃ linenumber`:

- CUSTOMER × ORDERS: 1:N on `c_custkey = o_custkey`
  (~10 orders per customer at SF=1).
- ORDERS × LINEITEM: 1:N on `o_orderkey = l_orderkey`
  (~4 lineitems per order).
- Total chain cardinality: ~150K customers × 10 × 4 ≈ 6M rows at
  SF=1 before filters; after `o_orderdate ∈ window` (~1/24) and
  `l_returnflag = 'R'` (~1/4), ~6M × 1/24 × 1/4 ≈ 62K rows enter
  the per-customer accumulator.

NATION attaches **outside the chain** as a dimension lookup:

- NATION (25 rows): per-customer INL on the NATION primary index
  at emit time (Decision D6). No in-memory `nation_set`, no
  `nation_name_map`, no pre-materialised name array. Q10 has no
  filter on NATION, so any in-memory sidetable would be degenerate
  (all 25 rows) and would just duplicate the NATION primary index.

NATION is a dimension lookup, not a chain participant. It does
**not** contribute M:N rows to the join output. **This is not a
§3.1.2 sibling pattern** (no per-customer scalar reduction of an
out-of-chain table) and **not a §3.1.3 genuine-tree pattern** (no
co-located sub-hierarchy inside the MI). The MI benefit is purely
hierarchical-prefix scan locality on the COL chain — anti-pattern
#27 forbids importing "no sibling shortcut" wording from
sibling-aggregate framings.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy | Params baked in |
|---|----------|--------------------|--------------|-----------------|
| 1 | Traditional indexes + binary merge join | Custkey-sorted secondaries on ORDERS (`(custkey, orderkey)`) and LINEITEM (`(custkey, orderkey, linenumber)`) — **reused verbatim from Q3 / Q5** | 2-BMJ chain: customer ⋈ orders\_sec ⋈ lineitem\_sec on the custkey-extended prefix; NATION attaches as per-customer INL at emit | none |
| 2 | Intermediate pipeline view | `q10_pipeline_view_t` (per-lineitem rows keyed by `(custkey, orderkey, linenumber)` carrying `l_extendedprice`, `l_discount`, `l_returnflag`, `o_orderdate` + FD-attached 7-column customer payload) | View scan + returnflag filter + **SUM-per-orderkey + Filter[orderdate ∈ window]** (S2-only D4 anomaly) + SUM-per-c_custkey + NATION INL at record-assembly | none |
| 3 | MI[COL] only | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_col_t>` keyed by custkey-prefixed tagged keys — **reused verbatim from Q3 / Q5** | `col_group_walk` over the COL MI with a bespoke `Q10GroupWalkVisitor` (Decision D1); per-customer accumulator with incremental top-20 emit (Decision D2); NATION INL at on_group_end (Decision D6) | none |
| 4 | Traditional indexes + hash join | None | PK-only `cust_set<custkey>`; HashJoin against orderdate-filtered ORDERS → PK-only `orders_set<orderkey>`; LINEITEM sorted-seek-on-miss with per-orderkey INL recovery of `c_custkey`; `HashAggregate` per c_custkey (group key only); FD output cols + n_name recovered at record-assembly via `customer.lookup1` + NATION INL (Decisions D5 + D6) | none |

**S2 has two view variants** (perf investigation, `PERFORMANCE.md`),
selected at query time by `--q10_view_variant`:
- `lineitem` (default, the table row above) — `q10_pipeline_view_t`, one
  row per lineitem; the D4 anomaly chain. This is the per-lineitem
  strawman responsible for the 5L btree S2 regression.
- `preagg` — `q10_pipeline_view_preagg_t` (id=72), one row per
  `(custkey, orderkey)` with `returned_revenue` pre-summed over
  `l_returnflag='R'` (a spec constant — soundly bakeable; only `:d` is
  parameterised) and `o_orderdate` kept live. The query collapses to
  scan → date filter → per-customer SUM → NATION INL → TopN (no
  per-orderkey rollup, no per-row returnflag filter). 10×/10.4× faster
  than `lineitem` at 5L (btree/LSM). Both views are populated at load
  so one image serves the A/B; `q10_admit_revenue_from_join` and the
  shared aggregator are reused. This is the **fair** S2 baseline.

**S5 — aCOL MI, implemented (D8 overturned at the per-order grain).**
D8 originally omitted S5 by reasoning only about *per-customer*
pre-aggregation: a customer's total returned revenue can't be baked
because the orderdate window is parameterised. But the **per-order**
grain *is* sound — the window filters at *order* granularity, and
`l_returnflag='R'` is a spec constant — so per-order returned revenue
is parameter-independent and bakeable (exactly the insight behind the
S2 per-order preagg view). The **aCOL MI** (`--storage_structure=5`,
`AggregatedQ10`) stores `MergedAdapter<customer_coli_t, orders_acol_t>`:
one full customer record + one `orders_acol_t` per order carrying
`{o_orderdate (live), returned_revenue (baked)}`, **no lineitems**. It
carries new information and drops the lineitem bulk — not a degenerate
copy of S3. Query path: hand-rolled `acol_group_walk`
(`q10_family/acol_walk.tpp`, `next_raw` raw-dispatch — NOT the generic
variant walk that made Q3I's aCOLI S5 lose to S3) → per-customer SUM →
NATION INL → TopN(20). This is the **fair** pre-aggregated merged index
vs the S2 preagg *view*: same pre-computation, but co-located (customer
payload stored once, not duplicated per order). At SF=150 it is the
fastest structure (9.98 ms/q vs the view's 21.5, S3's 1,017). See
[`PERFORMANCE.md §7`](PERFORMANCE.md). Row shape: `orders_acol_t`
(idx=37) in `tpch_family/views_col.hpp`.

---

## Locked-In Design Decisions

Consulted and frozen during Phase 0 — these shape the doc and
DOTs, and pre-decide Phase 1 questions that would otherwise
ambush the implementation:

- **D1. S3 visitor is bespoke, built on `col_group_walk`.** Do
  **not** subclass `q3_family::Q3FamilyVisitor`. That base is a
  Q3-shaped per-order-with-mktsegment specialisation; the real
  shared util is the COL walk itself. Q10 writes a fresh visitor
  (under `q10/`, not in `q3_family/`).
- **D2. S3 grouping: per-customer accumulator with incremental
  top-20 emit during the walk.** Each finished custkey group
  finalises its revenue, resolves `n_name` via NATION INL, then
  is offered to a bounded min-heap (capacity 20). Avoids
  materialising the full ~150K-customer intermediate vector
  before the top-N sort.
- **D3. S2 view: per-lineitem rows with customer columns
  FD-attached.** `q10_pipeline_view_t` keyed on
  `(custkey, orderkey, linenumber)`; each row carries
  `l_extendedprice`, `l_discount`, `l_returnflag`, `o_orderdate`,
  plus the 7-column customer payload (`c_custkey`, `c_name`,
  `c_address`, `c_nationkey`, `c_phone`, `c_acctbal`,
  `c_comment`). No aggregation baked (orderdate window is
  parameterised — soundness rule).
- **D4. S2 carries an anomaly per-orderkey aggregate before the
  per-customer SUM.** The shared family logical plan aggregates
  directly per `c_custkey` in a single HashAggregate (FD output
  columns recovered at record-assembly time, PK-only build
  convention). S2 alone deviates: because `o_orderdate` is hoisted
  out of the view (parameterised) and applies at *order*
  granularity, the post-view-scan operators must roll lineitems
  up to per-order revenue first, then prune orders by the
  orderdate filter, then aggregate qualifying orders per customer.
  Sequence: view scan → returnflag filter → SUM-per-orderkey →
  Filter[orderdate ∈ window] → SUM-per-c_custkey → NATION INL →
  top-20. Per-customer SUM cannot precede the orderdate filter
  without contaminating customers with out-of-window orders. S1 /
  S3 / S4 apply the orderdate filter inside the join chain itself,
  so no intermediate per-orderkey aggregate is needed; the anomaly
  is purely an S2 physical-plan artefact.
- **D5. S4 HashJoin chain — PK-only builds, INL recovery at
  record-assembly (CONVENTIONS Rules 4 / 13; mirrors Q5 S4).**
  `HashBuild(c_custkey)` carries no FD payload — produces
  `cust_set<custkey>`. ORDERS probe (orderdate filter pushed
  below) emits `orders_set<orderkey>`, again PK-only. LINEITEM
  is a sorted-seek lowering: on a miss at orderkey `K` seek to
  `{K+1, 0}`; per-orderkey transition recovers `c_custkey` via
  `orders.lookup1` (Rule 13) and caches it for the ~4 lineitems
  in that order. `HashAggregate` per `c_custkey` holds only
  `(c_custkey, revenue)`; the 6 FD output columns and `n_name`
  are recovered per surviving customer at record-assembly time
  via `customer.lookup1` + NATION INL (D6). The output row is
  **conceptual** — no dedicated intermediate schema; columns
  flow directly into the TopN sink.
- **D6. NATION is uniformly handled as an index nested-loop join,
  not as an in-memory sidetable.** Q10 has no filter on NATION,
  so a `nation_set` hashset is degenerate. Across all four shapes,
  `n_name` is resolved by a per-customer point lookup on the
  NATION base index at emit time. No `nation_set`, no
  `nation_name_map`, no pre-materialised name array.
- **D7. S1 reuses Q3/Q5's existing custkey-sorted secondaries
  verbatim:** `orders_sec_t` (Key `(custkey, orderkey)`) and
  `lineitem_sec_t` (Key `(custkey, orderkey, linenumber)`). The
  Q5 widening already put `l_returnflag` into the lineitem
  secondary payload — no new secondary needed.
- **D8 (REVISED). S5 = aCOL MI, implemented.** D8 originally omitted S5,
  but that reasoning only covered *per-customer* pre-agg. The *per-order*
  returned-revenue aggregate IS soundly bakeable (window filters at order
  grain; returnflag is constant), so S5 is now the aCOL MI. See
  §Storage Structure Options above and [`PERFORMANCE.md §7`](PERFORMANCE.md).

---

## Plan Descriptions

**Logical joins (Rule 12)** — shared across all four storage
structures:

- **#1** `CUSTOMER ⋈ ORDERS on c_custkey = o_custkey`
- **#2** `ORDERS ⋈ LINEITEM on o_orderkey = l_orderkey`
- **#3** `CUSTOMER ⋈ NATION on c_nationkey = n_nationkey` — lowered
  to per-customer INL on NATION PK at emit time (Decision D6;
  Rule 13)

S3 fuses #1 and #2 into the single `col_group_walk` visitor; S1
lowers them as a 2-BMJ chain over custkey-sorted secondaries; S2
reads the C-O-L portion pre-materialised from the view; S4
realises them as two HashJoins with PK-only-ish payloads (the
CUSTOMER build carries the 7 FD output columns because the
downstream aggregate consumes them).

Three DOT files in [`plans/`](plans/) document the operator
graphs:

- `plans/family_logical.dot` — shared logical plan for S1, S2, S3.
  All three agree on filter placement, the single per-c_custkey
  HashAggregate (with FD output cols recovered at record-assembly
  time, PK-only build convention), and the per-customer NATION INL
  attachment (D6); only the inside-pipeline physical operator
  differs. The S2-only per-orderkey anomaly aggregate (D4) is
  documented as a comment block at the bottom of the DOT, not as
  part of the shared chain.
- `plans/family_s3_physical.dot` — S3 physical specialisation. A
  single `col_group_walk` over the 3-table COL MergedAdapter
  subsumes the per-table filters and the 2-way chain join. A
  bespoke `Q10GroupWalkVisitor` (Decision D1) accumulates revenue
  per custkey group; on `on_group_end` it resolves `n_name` via
  NATION INL (D6) and offers the row to a bounded top-20 sink
  (D2). Aggregation matches the family logical's per-c_custkey
  shape; the S2-only per-orderkey anomaly aggregate is unnecessary
  because the orderdate filter fires inline at `on_order`.
- `plans/baseline_s4.dot` — S4 baseline. HashJoin chain over base
  tables only (no `col.split_*`): PK-only build (`cust_set<custkey>`)
  on CUSTOMER, probe with orderdate-filtered ORDERS → orders_set
  (PK-only on `o_orderkey`); LINEITEM probe with seek-on-miss to
  `{K+1, 0}` and per-orderkey-transition INL recovery of `c_custkey`
  via `orders.lookup1`; per-`c_custkey` HashAggregate (group key
  only — FD output cols recovered at record-assembly time via
  `customer.lookup1` + `nation.lookup1`); TopN(20). The conceptual
  output row has no dedicated intermediate schema.

### Filter pushdown principle (applied across all four plans)

See the canonical rule in
[Filter Pushdown](../OPERATORS.md#filter-pushdown). The Q10-specific
application:

Every parameterised filter is pushed as far down the operator graph
as possible, **stopping only at secondary structures** so they
remain reusable across param sets (predicate hoisting). For Q10
this means:

- The COL MI, the COL custkey-sorted secondaries, and the
  `q10_pipeline_view_t` are all loaded **without** applying the
  orderdate window or the returnflag filter. A new param set
  triggers a new query, not a new load. **No parameterised filter
  may be baked into any secondary** (PLAYBOOK §3.5 step 7).
- `o_orderdate ∈ [:d, :d+3mo)` fuses with `TableScan(ORDERS)` (or
  with the walker's `on_order` hook for S3); pushed below the
  ORDERS-side probe in S4.
- `l_returnflag = 'R'` is kept live (not baked) for view-
  reusability across a hypothetical no-returnflag Q10 variant;
  fuses with `TableScan(LINEITEM)` (or with the walker's
  `on_lineitem` hook for S3); pushed below the LINEITEM-side
  probe in S4.
- **D6**: NATION attachment is uniformly a per-customer INL on
  NATION's primary index at emit time. No filter exists on NATION,
  so the only place it can fire is the moment `n_name` is needed
  for output — at the boundary between aggregate and TopN.
- **D4 callout (S2 anomaly)**: the shared family logical plan
  aggregates directly per `c_custkey` in a single HashAggregate.
  S2 alone must insert an extra per-orderkey SUM (SUM-per-orderkey
  → Filter[orderdate ∈ window] → SUM-per-c_custkey) because its
  view is loaded unfiltered and the orderdate filter applies at
  order granularity. S1 / S3 / S4 apply the orderdate filter
  inside the join chain itself, so the per-orderkey step never
  materialises — qualifying lineitems contribute directly to the
  per-`c_custkey` cell.

### How the four approaches differ

**S3 (MI[COL] + COLGroupWalk)** is the tightest expression of the
plan. The COL tagged-key encoding co-locates customer, orders, and
lineitem records by `custkey` in byte-lex order
(`customer → (orders → lineitem*)+`), and a bespoke
`Q10GroupWalkVisitor` streams through them in a single forward
pass. `on_customer` snapshots the 7 FD output columns (no gate —
Q10 has no per-customer predicate); `on_order` applies the
orderdate window and `SkipGroup`s on miss; `on_lineitem` applies
the returnflag filter and accumulates revenue into the
per-customer cell; `on_group_end` resolves `n_name` via per-
customer NATION INL (D6) and offers the row to a bounded top-20
sink (D2). At `on_walk_end` the sink drains sorted DESC by
revenue into the result vector. No buffering of MI rows, no
per-customer hashmap of partial aggregates (the group boundary
finalises each customer in turn), no separate aggregate pass.

**S1 (custkey-sorted secondaries + BMJ chain)** runs the same
logical plan as S3 over three separate custkey-sorted streams
(CUSTOMER + two secondary indexes on ORDERS and LINEITEM). The
secondary keys (`(custkey, orderkey)` for orders,
`(custkey, orderkey, linenumber)` for lineitem) place both inputs
in the order required by the BMJ chain. Implemented as a 2-BMJ
chain that emits per-lineitem joined rows; a per-customer
`HashAggregate` rolls those up, the NATION INL fires per-customer
at emit time, and the result feeds the same top-20 sink. S1
differs from S3 only in I/O pattern: three trees instead of one.

**S2 (materialised pipeline view)** caches per-lineitem rows of
the post-join (pre-aggregate) family plan as a
`q10_pipeline_view_t` table at load time, keyed by
`(custkey, orderkey, linenumber)` and carrying
`l_extendedprice`, `l_discount`, `l_returnflag`, `o_orderdate`,
plus the 7 FD-attached customer columns. **No filters are baked
into the view** — both the orderdate window (parameterised) and
the returnflag predicate (kept live for view reusability) apply
at query time per predicate hoisting (PLAYBOOK soundness rule).
Query time follows Decision D4 (S2 anomaly): view scan →
returnflag filter → SUM-per-orderkey → Filter[orderdate ∈ window]
→ SUM-per-c_custkey → NATION INL → top-20. The intermediate
per-orderkey aggregate is the S2-only deviation from the shared
family logical's single per-c_custkey HashAggregate. The view is
reusable across all DATE param sets.

**S4 (HashJoin chain baseline)** uses base tables only (no
`col.split_*` secondaries — fairness vs S3). PK-only build
convention (CONVENTIONS Rule 4): `HashBuild(c_custkey)` produces
`cust_set<custkey>` carrying no FD payload; ORDERS probe (with
orderdate filter pushed below) emits `orders_set<orderkey>` also
PK-only. LINEITEM probe is a sorted-seek lowering: on a miss at
orderkey `K` seek to `{K+1, 0}`; per-orderkey transition recovers
`c_custkey` via `orders.lookup1` (Rule 13) and caches it for the
~4 lineitems sharing that order. `HashAggregate` per `c_custkey`
holds only `(c_custkey, revenue)`; the 6 FD output columns
(`c_name`, `c_acctbal`, `c_address`, `c_phone`, `c_comment`,
`c_nationkey`) and `n_name` are recovered per surviving customer
at record-assembly time via `customer.lookup1` + NATION INL on PK
(D6). The output row is **conceptual** — no dedicated intermediate
schema; columns are assembled directly into the TopN sink. S4
measures the no-merged-index baseline that the family is compared
against.

### Comparison axis summary

All four plans share the same logical shape (chain join + per-
customer HashAggregate + per-customer NATION INL + TopN(20));
they differ only in physical operators and filter substrate.

| Approach | Inside-pipeline physical | Aggregate | NATION attachment |
|----------|--------------------------|-----------|-------------------|
| S1 (merge family) | 2-BMJ chain over custkey-sorted split indexes | per-customer HashAggregate above the chain | per-customer INL on NATION PK at emit (D6) |
| S2 (merge family) | sequential per-lineitem view scan + returnflag filter + SUM-per-orderkey + orderdate filter (the S2-only D4 anomaly) | SUM-per-c_custkey above the per-orderkey intermediate (D4); FD output cols already in view payload | per-customer INL on NATION PK at record-assembly (D6) |
| S3 (merge family) | `col_group_walk` over MI[COL] with bespoke visitor (D1); per-customer accumulator finalised at group boundary | bounded top-20 sink fed incrementally at `on_group_end` (D2) | per-customer INL on NATION PK inside `on_group_end` (D6) |
| S4 (baseline) | HashJoin chain over base tables only; PK-only builds (`cust_set<custkey>`, `orders_set<orderkey>`); LINEITEM sorted-seek with per-orderkey-transition INL recovery of `c_custkey` (Rules 4/13) | `HashAggregate` per c_custkey (group key only — FD output cols recovered at record-assembly via `customer.lookup1`) | per-customer INL on NATION PK at record-assembly (D6) |

All four agree on what's outside the pipeline: drain the per-
customer aggregate, attach `n_name`, take the top 20 by revenue
DESC. **LIMIT 20** — `apply_topN` or a bounded min-heap.

---

## Required Record Types

Composition with sibling queries: **share record types with Q3,
Q5, and Q10I wherever the shape is identical.** Per PLAYBOOK §3.5
step 8, the relationship is composition (DRY), not inheritance.

Reused verbatim from `tpch_family/views_col.hpp`:

- `customer_coli_t` — tagged record, sentinel id reused.
  Payload already carries the full Q10 output payload:
  `c_name`, `c_address`, `c_nationkey`, `c_phone`, `c_acctbal`,
  `c_mktsegment` (Q3-only — Q10 ignores it), `c_comment`
  (`views_coli.hpp:237–243`). **No widening required.**
- `orders_coli_t` — tagged record, Key `(custkey, orderkey)`.
  Payload carries `o_orderdate` (already present for Q3 / Q5).
  **No widening required.**
- `lineitem_col_t` — tagged record, Key
  `(custkey, orderkey, linenumber)`. Payload already carries
  `l_returnflag` as an **explicit Q10 placeholder**
  (`views_col.hpp:79, 133`). Also carries `l_extendedprice`,
  `l_discount`. **No widening required.**

For S1 (split secondaries) — reused from Q3 / Q5:

- `orders_sec_t` — un-tagged secondary, Key `(custkey, orderkey)`.
- `lineitem_sec_t` — un-tagged secondary, Key
  `(custkey, orderkey, linenumber)`, already carrying
  `l_returnflag`.

New for Q10 (S2 view + final aggregate):

- `q10_pipeline_view_t` — Key `(custkey, orderkey, linenumber)`,
  one row per lineitem. Payload: `l_extendedprice`, `l_discount`,
  `l_returnflag`, `o_orderdate` + 7 FD-attached customer columns
  (`c_custkey`, `c_name`, `c_address`, `c_nationkey`, `c_phone`,
  `c_acctbal`, `c_comment`). `n_name` is **not** stored here —
  resolved per-customer at emit via NATION INL (D6). **No
  pre-aggregated revenue field** (parameterised by orderdate;
  soundness rule).
- `q10_agg_row_t` — the **conceptual** post-assembly output row;
  no dedicated intermediate schema during the pipeline. The row
  is **materialised only at TopN-sink boundary** (at most 20
  rows), carrying `c_custkey`, `revenue`, plus the 6 FD output
  columns (`c_name`, `c_acctbal`, `c_address`, `c_phone`,
  `c_comment`, `c_nationkey`) and `n_name`. In S1/S3 the FD cols
  come for free from the in-walker / in-BMJ customer record; in
  S2 they're carried in the view payload (D3); in S4 they're
  recovered via `customer.lookup1` + NATION INL at record-assembly
  (D5 + D6). C++ aliases: in-memory only (no
  `ADD_RECORD_TRAITS`); `c_custkey` is the unique tiebreaker the
  TopNSink comparator needs (CONVENTIONS §Post-pipeline OutClass).

**Composition with Q10I.** Q10I's `q10i_agg_row_t` should derive
from `q10_agg_row_t` (adds `open_balance`, `late_balance`); the
visitor base for Q10I may either reuse Q10's bespoke visitor with
extra hooks (preferred if shape matches) or fork. Defer to Q10I
Phase 0 — Q10 does not pre-commit.

### Side-table runtime structures

**None.** Decision D6 makes NATION a per-customer INL on the
primary index; there is no `nation_set`, no `nation_name_map`,
no per-query in-memory hashmap. The only runtime structures
beyond the COL adapters are:

- The bespoke S3 visitor's per-customer accumulator cell (a single
  `(c_custkey, customer_payload, revenue)` tuple resetting at
  `on_customer`).
- The bounded top-20 sink (a 20-element min-heap by revenue ASC).

Both are sub-kilobyte; their cost is negligible.

---

## Filter pushdown principle

(Stated above in §"Filter pushdown principle (applied across all
four plans)" — kept under §Plan Descriptions to mirror Q3 / Q5's
structure. Cross-referenced here so the PLAYBOOK §3.5 required-
section checklist is satisfied.)

---

## Open Questions

### Visitor placement: q10/ vs q10_family/

D1 settles that the S3 visitor is bespoke and does not subclass
`q3_family::Q3FamilyVisitor`. Open: whether the bespoke visitor
lives directly under `q10/` (one consumer) or in a new
`q10_family/` (anticipating Q10I reuse). Deferred to Q10I
Phase 0 — only mint the family namespace if Q10I genuinely shares
the per-customer top-N + NATION-INL shape. Default for Phase 1:
keep the visitor under `q10/` and hoist later if reuse
materialises.

### `q10_pipeline_view_t` wide-payload duplication

D3 settles the per-lineitem shape with FD-attached customer
columns. The 7-column customer payload duplicates per lineitem
(~6M view rows × ~50 bytes of customer payload ≈ 300 MB at SF=1
overhead vs a split shape). Decided to accept this for now —
single scanner is simpler, and 5L is the only paper-target cell
so storage cost is bounded. Phase 4b may revisit if S2 paging
becomes a bottleneck.

### Top-20 sink: `apply_topN` reuse vs custom min-heap

D2 settles the incremental-emit shape. Open: whether to reuse
the existing `apply_topN` helper used by Q3 (which sorts the
full vector and truncates) or implement a tighter bounded
min-heap. For Q3's `LIMIT 10` the difference is negligible; for
Q10's `LIMIT 20` over ~150K candidates it may matter. Phase 1
decision; not a Phase 0 design lock.

### Workload follow-up: param rotation harness

Q3I exposed a bug (the `pre_revenue` shipdate-bake) that hid for
months because the runner only used `Params::defaults()`. Q10
should follow the Q3 / Q5 convention: drive a sequence of param
sets (DATE rotation across the substitution domain) per executable
run, not just defaults. Tracked as a Phase 4 §7 checklist item;
not a Phase 0 design decision.

---

## Implementation Phases

- **Phase 0** (commit `d820148c`) — design doc + DOT plans.
- **Phase 0 fixups** (commit `dd6304ac`) — PK-only S4 builds (Rule 4
  / Rule 13 record-assembly recovery), S2-anomaly aggregation
  framing; q10_agg_row_t reframed as the conceptual post-assembly
  output row.
- **Phase 1 — complete (three sub-commits).**
  - **Commit 1** (`c7e79b69`) — 8-file compilable skeleton; all
    four `query_by_*` are stubs returning empty; `q10_lsm` links
    clean on macOS.
  - **Commit 2** (`d6514a00`) — `test_query_q10_{lsm,btree}` harness
    asserts cross-structure digest parity at 0x0; CMake targets +
    `generate_targets.py` entries (DIFF_DIRS, STRUCTURE_OPTIONS,
    exec_names); `frontend/tpch/CLAUDE.md` Tests table updated. Q10
    stays standalone (not in vanilla family loader).
  - **Commit 3** (this) — schema widening: `q10_pipeline_view_t`
    payload widened to the 7 FD customer cols (Decision D3);
    `q10_agg_row_t` widened to carry the 6 FD output cols + n_name
    (the post-assembly contract). `Q10Stats` declared at namespace
    scope with `Q10Stats* stats` member on the workload class.
    24-entry PARAM_TABLE populated (Decision E4 — every valid
    month start in [1993-02-01, 1995-01-01]); real
    `set_params_for_iter` rotates through it. Test harness
    extended with strict-equality cardinality on splits +
    merged_col, per-type breakdown via `col_group_walk`,
    sentinel-ordering check, per-customer-group distribution
    stats, `pipeline_view rows == 0 [deferred to Phase 4a (Pattern
    B)]` line.
- **Phase 4a (bundled §7.1 + Phase 4a view loader; complete —
  three commits).** Mints `frontend/tpch/q10_family/` (preemptive
  per user — Q10I drop-in target) holding `visitor.hpp` (dual-mode
  `Q10GroupWalkVisitor` on `col_group_walk`; NOT a Q3FamilyVisitor
  subclass per D1 / user-memory rule), `out_class.hpp` (`Q10QuerySink`
  wrapping canonical `TopNSink<q10_agg_row_t, &q10_agg_row_t::cmp>`),
  `view_loaders.hpp` (`Q10ViewLoadSink` + `populate_q10_view`).
  - Commit 1 — Query mode + S3 `query_by_merged`; NATION INL on PK at
    `on_group_end` (D6); harness flipped to `[DEFER]` lines for
    S1/S2/S4 plus a `Q10Stats post-S3` report block.
  - Commit 2 — ViewLoad mode + `populate_q10_view`; `load.tpp`
    wires it after `col.populate_merged()`; harness asserts
    `pipeline_view rows == |lineitem|` strict.
  - Commit 3 — harness top-3 eyeball print + this doc + RUNS.md.
- **Phase 4b (complete — three commits, 2026-05-24).** Mints
  `frontend/tpch/q10_family/admit.hpp` (per-customer aggregator +
  `q10_admit_lineitem_from_join` / `q10_admit_revenue_from_join` /
  `q10_finalize_aggregator`) shared across S1 / S2 / S4. All three
  query bodies funnel surviving lineitems through this helper; NATION
  INL fires per surviving customer at finalize (D6, cached).
  - **Commit 1 (§7.2)** — S1 `query_by_base` via nested forward scan
    over custkey-sorted COL split secondaries (functionally equivalent
    to a 2-BMJ chain). SF=1 parity verified.
  - **Commit 2 (§7.3)** — S2 `query_by_view` via the D4 anomaly chain
    (per-orderkey rollup → orderdate filter → per-customer SUM).
  - **Commit 3 (§7.5)** — S4 `query_by_hash` via D5 build/probe chain;
    `cust_set` / `orders_set` PK-only (Rule 4); LINEITEM sorted-seek
    with per-orderkey-transition INL recovery of `c_custkey` and the
    customer record (Rule 13). Strict 4-way XOR parity gate flipped on.
- **Phase 5 (complete — two commits, 2026-05-24).** Harness polish +
  docs refresh. Commit 1 splits `Q10Stats` per query path (no counter
  fan-out) and adds the off-default `set_params_for_iter(1)` re-run
  gate per PLAYBOOK §10 (param-bake regression guard). Commit 2
  propagates Q10 completion across `frontend/tpch/STATUS.md`,
  `frontend/tpch/CLAUDE.md`, `frontend/tpch/HISTORY.md`, and
  `LINUX_PENDING.md`. Production binaries (`q10_lsm`, `q10_btree`),
  CMake targets, and `generate_targets.py` entries already landed
  in Phase 1.
- **S5 (aCOL MI)** — implemented 2026-05-25 (D8 revised). Per-order
  pre-aggregated COL merged index + hand-rolled `acol_group_walk`;
  parity-verified SF=1 both backends, fastest structure at SF=150.
  See [`PERFORMANCE.md §7`](PERFORMANCE.md).
- **Linux perf sweep — 5L cell only** (Decision: paper scope). Logged
  in `LINUX_PENDING.md` once Phase 1+ lands.

---

## Contingency: "couldn't get results ready"

If Phase 1+ stalls or the 5L cell does not return clean numbers
before the paper deadline, the Phase 0 design doc lets Q10 be
cited as **design-only future work** without overclaiming. The
doc is structured for that case:

- §Status is explicit that scope is 5L-only and results are
  pending — no SF×DRAM sweep commitment.
- §Motivation makes no predictive perf claim. The COL MI thesis
  is stated as "the same as Q3 / Q5", not as a per-query promise.
- D6 (NATION = INL only) and D8 (no S5) read as **deliberate
  design constraints**, not omissions — so reviewers see a
  bounded, defensible plan rather than a half-finished sketch.
- The cross-link to Q10I makes the pair's joint status legible:
  Q10 and Q10I stand or fall together.

If results do land, this doc converts into the frozen target for
code review and the implementation tracks the locked-in decisions
above. If they don't, it reads as a self-contained future-work
appendix and nothing in the paper has overclaimed.
