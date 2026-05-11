# Q5: Local Supplier Volume

**Reading guide**: For SQL and plan descriptions, read §TPC-H Definition and §Plan Descriptions. For implementation status, read §Implementation Status. For OutClass / wildcard / latent-assumption details, see `../CONVENTIONS.md`. Skip the rest unless reconstructing a design decision or adding a new storage structure.

## Sibling Docs

Every non-`CLAUDE.md` Markdown in `q5/` and `q5/plans/` (the latter
has no `CLAUDE.md`; indexed here as the nearest ancestor). Read each
on the trigger described:

- [`plans/family_logical.dot`](plans/family_logical.dot) — **shared
  logical plan for S1, S2, S3**, COL pipeline plus side-table
  attachments (REGION, NATION, SUPPLIER).
- [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot) —
  **S3 physical specialisation** over the COL MI.
- [`plans/baseline_s4.dot`](plans/baseline_s4.dot) — **S4 baseline**
  (HashJoin chain with `r_name` pushed below REGION scan).
- [`RUNS.md`](RUNS.md) — perf-run ledger; appended after every
  Linux sweep.

Read [`../q3/CLAUDE.md`](../q3/CLAUDE.md) as the canonical Track-1
template — Q5 mirrors Q3's COL-pipeline structure and adds three
small dimension tables (REGION, NATION, SUPPLIER) that plug into
the shared family plan as reduce-side hash builds. Read
[`../q5i/CLAUDE.md`](../q5i/CLAUDE.md) for the invoice-extended
Track-2 sibling that adds an INVOICE sub-aggregate on top of this
plan.

## TPC-H Definition (§2.4.5 — "Local Supplier Volume")

```sql
SELECT  n_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM    customer, orders, lineitem, supplier, nation, region
WHERE   c_custkey   = o_custkey
  AND   l_orderkey  = o_orderkey
  AND   l_suppkey   = s_suppkey
  AND   c_nationkey = s_nationkey
  AND   s_nationkey = n_nationkey
  AND   n_regionkey = r_regionkey
  AND   r_name      = ':1'
  AND   o_orderdate >= ':d'
  AND   o_orderdate <  ':d' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY revenue DESC;
```

This is the canonical TPC-H Q5, **unextended**. Q5I extends Q5 with
an invoice sibling sub-aggregate (paid/open/late revenue split);
Q5 here is the no-extension baseline of the COL family.

### Substitution Parameters

| Parameter | Domain | Description | Validation |
|-----------|--------|-------------|------------|
| `:1` | One of REGION.r_name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST) | Region filter | `ASIA` |
| `:d` | January 1 of a year in [1993, 1997] | Start of the 1-year order window | `1994-01-01` |

**Approved query variants**: none in TPC-H Appendix B.

**Selectivity notes**: REGION has 5 rows, one per region; the
filter selects exactly 1. NATION has 25 rows; restricting to the
chosen region selects exactly 5. SUPPLIER (~10K at SF=1) is then
pruned to the in-region nations (~2K). Roughly 1/5 of orders fall
in any given 1-year window. Customers are ~150K at SF=1, of which
~30K are in-region. The `c_nationkey = s_nationkey` cross-equality
is the dominant pruner: of the ~4 lineitems per qualifying order,
typically 1–2 supplier nations match the customer's nation.

---

## Motivation

Q5 is a **Track-1 multi-dimension §3.1.3 hierarchical-prefix
showcase**: the dominant cost is the customer × orders × lineitem
chain (~60M rows at SF=1 before filters), and the chain matches
the COL MI's natural sort key `custkey ⊃ orderkey ⊃ linenumber`.
The three additional small dimension tables (REGION, NATION,
SUPPLIER) attach as reduce-side hash builds and contribute
negligibly to scan cost. **Cross-reference §Cardinality structure
framing #1: pure hierarchical along the COL chain.**

What Q5 adds beyond Q3:

- **No per-customer filter.** Q3's `c_mktsegment` gate suppresses
  ~80% of customers at the walker's `on_customer` hook. Q5 has no
  equivalent single-column predicate on CUSTOMER. The customer-side
  prune is `c_nationkey ∈ in_region_nationkeys`, which is also
  fused at `on_customer` (a 5-element set membership test) and
  prunes ~80% of customers — quantitatively similar reduction,
  qualitatively the same MI advantage.
- **Composite-key SUPPLIER semi-join.** The cross-equality
  `c_nationkey = s_nationkey` and the natural join `l_suppkey =
  s_suppkey` are fused into a single composite-key hashset probe
  `(c_nationkey, l_suppkey) ∈ supplier_nation_set`. One lookup per
  qualifying lineitem; no standalone cross-record predicate node.
- **Wider per-lineitem callback.** Each surviving lineitem probes
  `supplier_nation_set` with a composite key and accumulates revenue
  into a per-nationkey bucket — one extra constant-time hashset
  lookup per lineitem versus Q3's pure-revenue accumulator.
- **Emit-time `n_name` resolution.** `n_name` is not carried through
  any intermediate build. Inside the walker (or downstream of the
  CUSTOMER ⋈ RN inner join, for non-walker paths), NATION primary-
  index lookups materialise `n_name` lazily on first sighting of
  each new nationkey (~5 lookups, cached in the per-nationkey
  aggregator bucket).

The MI advantage thesis is the same as Q3's: `MI[COL]` co-locates
the chain in custkey-byte-lex order so a single `PremergedJoin`
pass produces the full 3-way join with no separate CUSTOMER merge
join, and the per-customer nation gate fuses into the walker. SF
sweeps should show S3 ≥ S2 > S1/S4 (the Q3I paper-line shape).

---

## Cardinality structure

Q5 is a **pure hierarchical schema along the COL chain** — framing
#1 from PLAYBOOK §3.5 step 4. CUSTOMER, ORDERS, LINEITEM form a
strict 3-level prefix chain along `custkey ⊃ orderkey ⊃ linenumber`:

- CUSTOMER × ORDERS: 1:N on `c_custkey = o_custkey`
  (~10 orders per customer at SF=1).
- ORDERS × LINEITEM: 1:N on `o_orderkey = l_orderkey`
  (~4 lineitems per order).
- Total chain cardinality: ~150K customers × 10 × 4 ≈ 6M rows
  at SF=1 before filters; after `c_nationkey ∈ region` and
  `o_orderdate ∈ window`, ~6M × 0.2 × 0.2 ≈ 240K rows enter the
  per-lineitem callback.

The three side tables attach **outside the chain** as reduce-side
operators:

- REGION (5 rows): point lookup on `r_name = :1`, returns one
  `r_regionkey`.
- NATION (25 rows): `HashJoin(REGION ⋈ NATION on n_regionkey =
  r_regionkey)` — yields `nation_set` (~5 `n_nationkey` entries).
  `n_name` is NOT stored; fetched lazily inside the chain/walker
  via NATION primary index (~5 point lookups, on first sighting of
  each new nationkey).
- SUPPLIER (~10K rows at SF=1): `HashSemiJoin(SUPPLIER ⋉ RN on
  s_nationkey = n_nationkey)` with survivors projected to
  `(s_nationkey, s_suppkey)` — yields `supplier_nation_set`
  (~2K composite-key entries; the projection IS the build, no
  separate `Build` operator). The cross-equality `c_nationkey =
  s_nationkey` and the natural join `l_suppkey = s_suppkey` are
  consumed implicitly by the downstream composite-key probe.

These are dimension lookups, not chain participants. They do **not**
contribute M:N rows to the join output. The per-lineitem probe of
`supplier_nation_set` with `(c_nationkey, l_suppkey)` is a single
constant-time hashset lookup, not a fourth join level. **This is not a §3.1.2 sibling pattern**
(no per-customer scalar reduction of an out-of-chain table) and
**not a §3.1.3 genuine-tree pattern** (no co-located sub-hierarchy
inside the MI). The MI benefit is purely hierarchical-prefix scan
locality on the COL chain — anti-pattern #27 forbids importing
"no sibling shortcut" wording from sibling-aggregate framings.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy | Params baked in |
|---|----------|--------------------|--------------|-----------------|
| 1 | Traditional indexes + binary merge join | Custkey-sorted secondaries on ORDERS (`(custkey, orderkey)`) and LINEITEM (`(custkey, orderkey, linenumber)`) | BMJ chain: customer ⋈ orders\_sec ⋈ lineitem\_sec on the custkey-extended prefix; SUPPLIER + NATION + REGION as side hash builds | none |
| 2 | Intermediate pipeline view | `q5_pipeline_view_t` (per-lineitem rows keyed by `(custkey, orderkey, linenumber)` carrying `l_extendedprice`, `l_discount`, `l_suppkey` + FD-attached `c_nationkey`, `o_orderdate`) | View scan + orderdate filter + composite-key `supplier_nation_set` probe live; per-nationkey `HashAggregate`; `n_name` fetched lazily inside the aggregator | none |
| 3 | MI[COL] only | `MergedAdapter<customer_col_t, orders_col_t, lineitem_col_t>` keyed by custkey-prefixed tagged keys | `col_group_walk[_fused_emit]` over the COL MI; CUSTOMER hierarchy co-located, no separate join; SUPPLIER + NATION + REGION as side hash builds | none |
| 4 | Traditional indexes + hash join | None | `CUSTOMER ⋈ RN` → `HashSemiJoin(⋈orders)` → orderdate filter → orderkey-only lineitem seeks → composite-key `supplier_nation_set` probe → per-nationkey `HashAggregate`; `n_name` fetched lazily inside the aggregator | none |

**S5 deliberately omitted.** Q5 has **no parameter-independent
aggregate to bake**: every per-row contribution to the answer
flows through `l_extendedprice * (1 - l_discount)` and is gated
by a parameterised `o_orderdate` window. There is no spec-hardcoded
constant analogous to Q3I's `i_status='O'` to anchor a load-time
reduction. Storing the unaggregated source rows (which is exactly
what S3 already does) is the only sound choice. **Soundness rule
from PLAYBOOK §3.5 step 5 applies; anti-pattern #10 / #24 explicitly
covers this case.**

---

## Plan Descriptions

Three DOT files in [`plans/`](plans/) document the operator graphs:

- `plans/family_logical.dot` — shared logical plan for S1, S2, S3.
  All three agree on filter placement, aggregate shape (per-nationkey
  HashAggregate with lazy in-aggregator `n_name` lookup), and the RN dimension
  subtree with its two asymmetric fan-outs (CUSTOMER ⋈ RN inner;
  SUPPLIER ⋉ RN semi-join); only the
  inside-pipeline physical operator differs.
- `plans/family_s3_physical.dot` — S3 physical specialisation. A
  single `col_group_walk[_fused_emit]` over the 3-table COL
  MergedAdapter subsumes the per-table filters, the CUSTOMER ⋈
  RN inner-join gate, and the 2-way join; the per-lineitem
  composite-key semi-join (`COL ⋉ supplier_nation_set`) and
  per-nationkey aggregator wrap the walker callback; `n_name`
  resolved lazily inside the walker per nationkey.
- `plans/baseline_s4.dot` — S4 baseline. A HashJoin chain over base
  tables only (no `col.split_*`) with `r_name` pushed below the
  REGION scan, `CUSTOMER ⋈ RN` applied before the customer
  hash build, orderkey-only lineitem seeks, and composite-key
  `supplier_nation_set` probe per lineitem.

### Filter pushdown principle (applied across all four plans)

See the canonical rule in [Filter Pushdown](../OPERATORS.md#filter-pushdown).
The Q5-specific application:

Every parameterised filter is pushed as far down the operator graph
as possible, **stopping only at secondary structures** so they
remain reusable across param sets (predicate hoisting). For Q5
this means:

- The COL MI, the COL custkey-sorted secondaries, and the
  `q5_pipeline_view_t` are all loaded **without** applying
  `r_name`, the in-region nation set, or orderdate. A new param
  set triggers a new query, not a new load. **No parameterised
  filter may be baked into any secondary** (PLAYBOOK §3.5 step 7).
- `r_name = :1` fuses with the REGION scan; the resolved
  `r_regionkey` then drives the NATION hash build's filter.
- The `nation_set` (output of `HashJoin(REGION ⋈ NATION)`)
  gates CUSTOMER via an **inner** `HashJoin(CUSTOMER ⋈ RN on
  c_nationkey = n_nationkey)` — inner because the matched
  `n_nationkey` flows downstream (consumed inside the chain/walker to
  fetch `n_name` via NATION's primary index). The join lowers
  to `c_nationkey ∈ nation_set` fused with `TableScan(CUSTOMER)`
  (or with the COL walker's `on_customer` hook for S3); the
  set-membership lowering is what the build-payload convention
  (`unordered_set<n_nationkey>`) makes possible. Surviving
  CUSTOMER rows carry `c_nationkey` downstream for the
  composite-key semi-join below.
- `o_orderdate >= :d AND o_orderdate < :d+1y` fuses with
  `TableScan(ORDERS)` (or with the walker's `on_order` hook).
- The SUPPLIER-side semi-join (`HashSemiJoin(SUPPLIER ⋉ RN on
  s_nationkey = n_nationkey)`) projects survivors to
  `(s_nationkey, s_suppkey)`. The downstream per-lineitem
  composite-key probe (`HashSemiJoin(COL ⋉ supplier_nation_set
  on (c_nationkey, l_suppkey) = (s_nationkey, s_suppkey))`) is
  a single hashset lookup that fuses the SUPPLIER semi-join,
  the cross-equality `c_nationkey = s_nationkey`, and the
  suppkey equi-join. There is no standalone cross-record
  predicate node — the cross-equality lives inside the
  composite key.
- For S4 specifically, `r_name` is pushed below the REGION scan,
  and `CUSTOMER ⋈ RN` is applied **before the hash build**,
  so the customer set only contains qualifying customers.
- For S3, every filter applies **during** the group walk, at the
  Visitor's `on_*` hook for that record type. There is no post-walk
  Filter node.

### How the four approaches differ

**S3 (MI[COL] + COLGroupWalk)** is the tightest expression of the
plan. The COL tagged-key encoding co-locates customer, orders, and
lineitem records by `custkey` in byte-lex order
(`customer → (orders → lineitem*)+`), and the walker streams
through them in a single forward pass. The CUSTOMER ⋈ RN inner-join gate
at `on_customer` (`c_nationkey ∈ nation_set`) skips the entire
group; the orderdate filter is an inline `if` check at `on_order`;
per-lineitem the walker probes `supplier_nation_set` with
`(c_nationkey, l_suppkey)` — one hashset lookup fuses the SUPPLIER
semi-join, the cross-equality, and the suppkey equi-join. Revenue
accumulates into the per-nationkey bucket carried by the Visitor.
Inside the walker, `n_name` is resolved lazily via NATION primary-
index lookup on first sighting of each new nationkey (~5 point
lookups, cached in the bucket). On_walk_end drains the buckets
to `q5_agg_row_t`. No
buffering of MI rows, no hashmaps over MI rows, no separate
aggregate pass — only the two small reduce-side structures
(`nation_set`, `supplier_nation_set`) prebuilt before the walk.

**S1 (custkey-sorted secondaries + BMJ chain)** runs the same
logical plan as S3 over three separate custkey-sorted streams
(CUSTOMER + two secondary indexes on ORDERS and LINEITEM). The
secondary keys (`(custkey, orderkey)` for orders,
`(custkey, orderkey, linenumber)` for lineitem) place both inputs
in the order required by the BMJ chain. Implemented as a 2-BMJ
chain: BMJ #1 joins `customerh_t ⋈ orders_coli_t` on custkey
(→ `q5_jr1_t`), BMJ #2 joins `q5_jr1_t ⋈ lineitem_col_t` on
`(custkey, orderkey, linenumber)` using `q5_sort_key_t`
(linenumber=`WILDCARD_KEY` on the build side; → `q5_jr2_t`).
Per-emit the same `q5_admit_lineitem` free helper is called as in
S3/S4. Unlike Q3's S1, no `LineitemRevenueAggregator` pre-aggregate
is used — Q5 needs per-lineitem `l_suppkey` for the SUPPLIER probe.
S1 differs from S3 only in I/O pattern: three trees instead of one.

**S2 (materialised pipeline view)** caches per-lineitem rows of
the post-join (pre-aggregate) family plan as a
`q5_pipeline_view_t` table at load time, keyed by
`(custkey, orderkey, linenumber)` and carrying
`l_extendedprice`, `l_discount`, `l_suppkey`, plus FD-attached
`c_nationkey` and `o_orderdate`. **No filters are baked into the
view** — `r_name`, the in-region nation set, and orderdate are
all parameterised, so predicate hoisting forbids fusing them at
load time (PLAYBOOK soundness rule). Query time is a sequential
view scan: the customer nation-set gate, orderdate filter, and
composite-key `supplier_nation_set` probe all apply live; revenue
is rolled up via a per-nationkey `HashAggregate`. Inside the
aggregator, `n_name` is resolved lazily per nationkey via NATION
primary-index lookup (~5 lookups, cached in the bucket). The view
is reusable across all REGION and DATE param sets.

**S4 (HashJoin chain baseline)** uses base tables only (no
`col.split_*` secondaries — fairness vs S3). Filter pushdown:
`r_name` fuses with the REGION scan; `CUSTOMER ⋈ RN`
gates the customer scan before the hash build; orderdate fuses
with ORDERS pre-build. Orders survivors feed an
`unordered_set<o_orderkey>` (Q3-isomorphic). Lineitem is driven
by a sorted list of qualifying orderkeys with orderkey-only seeks
into the base `lineitem` table; per orderkey, `c_nationkey` is
cached once via two primary-index lookups (ORDERS → `o_custkey`;
CUSTOMER → `c_nationkey`), amortised over ~4 lineitems per order.
Per-lineitem: probe `supplier_nation_set` with
`(cached_c_nationkey, l_suppkey)` — one hashset lookup replaces
the old separate `supplier_nation_map` probe and cross-equality
filter. Inside the aggregator, `n_name` is resolved lazily per
nationkey via NATION primary-index lookup (~5 lookups, cached
in the bucket). S4 measures the
no-merged-index baseline that the family is compared against.

### Comparison axis summary

All four plans share the same logical shape (chain join + side-
table reductions + per-nationkey HashAggregate + emit-time
`n_name` lookup); they differ only in physical operators and
filter substrate.

| Approach | Inside-pipeline physical | Aggregate | Filters / semi-joins resolved by |
|----------|--------------------------|-----------|----------------------------------|
| S1 (merge family) | 2-BMJ chain over custkey-sorted split indexes (`q5_jr1_t` → `q5_jr2_t`); `nation_set` + `supplier_nation_set` prebuilt | `NNameRevenueAggregator` keyed by nationkey; `n_name` fetched lazily inside the aggregator on first sighting of each nationkey (NATION PK lookup, ~5) | per-scan inline semi-join (nation_set gate, orderdate window); composite-key `supplier_nation_set` probe per lineitem |
| S2 (merge family) | sequential per-lineitem view scan; `nation_set` + `supplier_nation_set` prebuilt | per-nationkey HashAggregate; `n_name` fetched lazily inside the aggregator on first sighting of each nationkey (NATION PK lookup, ~5) | all filters live at query time (no filter baking — view is param-reusable); composite-key probe per lineitem |
| S3 (merge family) | `col_group_walk` over MI[COL]; `nation_set` + `supplier_nation_set` prebuilt | per-nationkey HashAggregate (Visitor map); `n_name` fetched lazily inside the walker on first sighting of each nationkey (NATION PK lookup, ~5) | Visitor `on_*` hooks; composite-key `supplier_nation_set` probe per lineitem |
| S4 (baseline) | HashJoin chain over base tables only (no `col.split_*`); orderkey-only lineitem seeks; `c_nationkey` cached per orderkey via PK lookups | per-nationkey HashAggregate; `n_name` fetched lazily inside the aggregator on first sighting of each nationkey (NATION PK lookup, ~5) | TableScan-time semi-joins pushed below each hash build; composite-key `supplier_nation_set` probe per lineitem |

All four agree on what's outside the pipeline: collect the
per-nationkey revenue map, resolve `n_name` fetched lazily inside the aggregator via
NATION primary-index lookup (~5 lookups), sort by `revenue DESC`,
output ~5 rows (one per in-region nation). **No LIMIT** — the
result cardinality is bounded by `|nation_set|` ≈ 5.

### OutClass: shared `NNameRevenueAggregator`

All four `query_by_*` bodies construct one
`NNameRevenueAggregator` per query and push qualifying lineitems
through `agg.accumulate(l, nationkey)` (via the shared
`q5_admit_lineitem` helper). The aggregator's internal map has at
most ~5 entries keyed by `n_nationkey` (one per in-region nation);
after the walk, `agg.emit(out, sides)` resolves `n_name` per
nationkey via NATION's primary-index point lookup (~5 lookups) and
materialises the final ~5 rows in one pass.

This is Q5's instance of the **OutClass** convention codified in
`CONVENTIONS.md §Post-pipeline OutClass`: a small-buffer sink owning the
pipeline → result boundary, push-once-per-row, drained once at the
end.  Memory is `O(|nation_set|)`, not `O(qualifying_lineitems)`.
Q5 has no LIMIT and therefore no `apply_topN` / `TopNSink` — the
HashAggregate IS the OutClass.

The same `NNameRevenueAggregator` instance is reused across all
four storage variants per OPERATORS.md §7 rule 5
(comparison-integrity at the post-pipeline boundary).
`TopNSink<R, Cmp>` plays the analogous role for Q3 / Q3I (LIMIT 10
queries) — see `q3/CLAUDE.md` and `q3i/CLAUDE.md`.

> **Counter convention (audit B.2)**: `lineitems_scanned` is
> bumped at four sites — S2 scan loop, S3 visitor `on_lineitem`,
> S1 `fetch_lin`, S4 `fetch_lin` — all at "raw fetch (pre-filter)"
> so the metric is comparable across paths.  See the inline
> comment block above `q5_admit_lineitem` in `q5/query.tpp` for
> the full convention; do NOT bump the counter inside the helper
> (post-SUPPLIER-probe would double-count).

---

## Required Record Types

Composition with sibling queries: **share record types with Q3 and
Q5I wherever the shape is identical.** Per PLAYBOOK §3.5 step 8,
the relationship is composition (DRY), not inheritance.

Reused verbatim from `views_col.hpp` (added for Q3 in Phase 0.5):

- `customer_col_t` — tagged record, sentinel id reused from Q3
  (matches `customer_coli_t`'s id), Key `(c_custkey)`. Payload:
  `customerh_t` fields needed by Q5 — at minimum `c_custkey` and
  `c_nationkey`. **If Q3's `customer_col_t` already carries
  `c_nationkey`, no change is needed**; otherwise extend the
  payload in place per PLAYBOOK §"Project pushdown" worth-widening
  trigger #1 (a real future query needs the column).
- `orders_col_t` — tagged record, Key `(custkey, orderkey)`. Payload:
  `o_custkey`, `o_orderkey`, `o_orderdate` (Q3 already needs the
  first three; `o_shippriority` is Q3-only and is preserved).
- `lineitem_col_t` — tagged record, Key
  `(custkey, orderkey, linenumber)`. Payload:
  `l_orderkey`, `l_linenumber`, `l_extendedprice`, `l_discount`,
  `l_suppkey` (new for Q5 — extend Q3's existing record in place
  per the same widening rule). `l_shipdate` is Q3-only and stays.

For S1 (split secondaries) — also reused from Q3:

- `orders_sec_t` — un-tagged secondary, Key `(custkey, orderkey)`.
- `lineitem_sec_t` — un-tagged secondary, Key
  `(custkey, orderkey, linenumber)`.

New for Q5 (S2 view + final aggregate):

- `q5_pipeline_view_t` — Key `(custkey, orderkey, linenumber)`,
  one row per lineitem. Payload: `l_extendedprice`, `l_discount`,
  `l_suppkey`, `c_nationkey`, `o_orderdate`. **No pre-aggregated
  revenue field** (parameterised by orderdate; PLAYBOOK soundness
  rule). Same per-lineitem granularity as Q3's view.
- `q5_agg_row_t` — Key `(n_name)`, payload `revenue` (a single
  numeric column). 5-element output (one per in-region nation),
  sorted `revenue DESC` outside the pipeline.

### Side-table runtime structures

These are not stored secondaries (no MergedAdapter / B-tree
membership), only in-process hashmaps built per query invocation:

- `nation_set` — `std::unordered_set<Integer>` of in-region
  `n_nationkey` values (~5 entries). Built as the output of
  `HashJoin(REGION ⋈ NATION on n_regionkey = r_regionkey)`,
  REGION filtered by `r_name = :1` first. Build payload is
  `n_nationkey` only (primary-key payload convention — no other
  NATION columns carried). `n_name` is NOT stored here; it is
  fetched lazily inside the aggregator (first sighting of each
  nationkey) via NATION's primary-index
  point lookup (~5 lookups per query).
- `supplier_nation_set` — `std::unordered_set<std::tuple<Integer,
  Integer>>` keyed on `(s_nationkey, s_suppkey)`, restricted to
  suppliers whose `s_nationkey ∈ nation_set` (~2K entries at SF=1).
  Encodes `SUPPLIER ⋉ nation_set` fused with the downstream
  cross-equality `c_nationkey = s_nationkey` and the natural join
  `l_suppkey = s_suppkey`: the per-lineitem probe sends
  `(c_nationkey, l_suppkey)` — one hashset lookup decides "supplier
  survived its semi-join AND its nation matches the customer's
  nation AND its suppkey equals the lineitem's suppkey". The
  cross-equality and suppkey equi-join collapse to a single
  composite-key probe; they do not exist as separate operators.

Both are built before the COL walk / chain join begins; their
build cost is sub-millisecond and is included in the per-TX
budget (not amortised across queries).

---

## Filter pushdown principle

(Stated above in §"Filter pushdown principle (applied across all
four plans)" — kept under §Plan Descriptions to mirror Q3's
structure. Cross-referenced here so the PLAYBOOK §3.5 required-
section checklist is satisfied.)

---

## Open Questions

### Sentinel id reuse vs new ids

Q3's `customer_col_t` / `orders_col_t` / `lineitem_col_t` have
specific sentinel ids in `views_col.hpp`. Q5 will share the
record types verbatim (composition), so it inherits those ids.
**Confirmed approach**: no new ids allocated; payload extensions
done in place on the Q3 types. If a payload extension breaks
Q3's load-test parity (size shift), the resolution is to widen
the Q3 record (per PLAYBOOK "Project pushdown" trigger #1 in
`frontend/tpch/CLAUDE.md`) and re-run Q3's parity test, not to
fork a Q5-specific type.

### Per-`n_name` aggregate: hash vs sorted

Inside the COL walker, lineitems for a single custkey arrive
clustered by orderkey but not by `n_name` (the per-lineitem
`s_nationkey` depends on which supplier each lineitem points
at, which is unrelated to the custkey hierarchy). So the
per-`n_name` aggregate is a `HashAggregate`, not a
`SortedAggregate`. With ~5 buckets the hashmap cost is trivial.
This differs from Q3's per-orderkey `SortedAggregate` (Q3's
aggregate key is the second component of the COL sort key, so
sorted-streaming is natural; Q5's aggregate key is unrelated to
the sort).

### Workload follow-up: param rotation harness

Q3I exposed a Q3I executable bug (the `pre_revenue` shipdate-bake)
that hid for months because the runner only used
`Params::defaults()`. Q5 should follow the convention adopted in
Q3 Phase 4: drive a sequence of param sets (REGION × DATE rotation)
per executable run, not just defaults. Tracked as a Phase 4 §7
checklist item; not a Phase 0 design decision.

### LIMIT placement

TPC-H Q5 has no LIMIT. The result is one row per in-region nation
(~5 rows). The `apply_topN` helper used by Q3 / Q3I is not needed;
a plain `std::sort` over the per-`n_name` aggregate suffices.

---

## Implementation Phases

- **Phase 0** (this commit) — design doc + DOT plans.
- **Phase 0.5** — skeleton: `views.hpp`, `workload.hpp`, `load.tpp`,
  `query.tpp` stubs, `per_structure_workload.hpp`,
  `executable_{rocksdb,leanstore}.cpp`, CMake + `generate_targets.py`
  entries. Stub `query_by_*` returns empty vectors;
  `test_query_q5_lsm` reports `[OK]` parity at digest 0x0.
  Reuses `CustomerOrdersLineitemPipeline<Backend>` from Q3
  verbatim.
- **Phase 1** (2026-05-09; **complete**) — `views.hpp` real types
  (`q5_pipeline_view_t`, `q5_agg_row_t`) were already present in the
  Phase 0.5 skeleton. This phase adds the S1 BMJ chain intermediate
  types (`q5_cust_jk_t`, `q5_jr1_t`, `q5_jr2_t`) with `std::hash`
  and `SKBuilder` specialisations. Key design decision: `q5_jr2_t`'s
  right side is `lineitem_col_t` (per-lineitem), NOT a pre-aggregate,
  because each lineitem needs `l_suppkey` for the SUPPLIER probe and
  the `c_nationkey = s_nationkey` cross-equality. No payload
  extensions were needed — `lineitem_col_t` already carries
  `l_suppkey` from Phase 0.5. All four tests pass: `test_load_col_lsm`
  all `[OK]`; `test_query_q3_lsm`, `test_query_q3i_lsm` parity
  unchanged; `test_query_q5_lsm` digest 0x0 `[OK]`.
- **Phase 1 (cont.)** (2026-05-09; **complete**) — `workload.hpp`
  Q5 PARAM_TABLE (REGION × DATE rotation, 10 entries),
  out-of-line `set_params_for_iter` that rotates through it, and
  real `q5_predicate_orders` body (orderdate window). The
  `q5_predicate_customer` body stays `return true;` per design
  (the `c_nationkey ∈ nation_set` gate is a per-query runtime
  hashmap, applied inline at the call sites in Phase 4 §7.x, not
  through the predicate signature). `load.tpp` already real in
  Phase 0.5 (no changes).
- ~~**Phase 2 / Phase 3**~~ — retired by PLAYBOOK consolidation;
  contents merged into Phase 1 (cont.) above.
- **Phase 4 §7.1** (2026-05-09; **complete**) — S3 `query_by_merged`
  via `col_group_walk` + `Q5GroupWalkVisitor` (free struct, not a
  Q3FamilyVisitor subclass — Q5's hook semantics differ). The visitor
  gates by `c_nationkey ∈ nation_set` at `on_customer`, orderdate
  window at `on_order`, and delegates per-lineitem work to the shared
  `q5_admit_lineitem` free helper. `NNameRevenueAggregator` collects
  per-`n_name` revenue; `agg.emit()` + `std::sort` produce the
  final ~5-row result. `test_query_q5_lsm` reports S3 rows >= 1,
  non-zero digest; S1/S2/S4 `[SKIP]`-tolerant.
- **Phase 4 §7.3** (2026-05-09; **complete**) — S2 `query_by_view`
  via sequential `q5_pipeline_view_t` scan. Customer gate, orderdate
  filter, SUPPLIER probe, and cross-equality all applied live;
  per-`n_name` `NNameRevenueAggregator` accumulates across the scan.
- **Phase 4 §7.5** (2026-05-09; **complete**) — S4 `query_by_hash`
  via 2-stage HashJoin chain: stage 1 joins `customerh_t ⋈
  orders_coli_t` on `custkey` (→ `q5_jr1_t`), stage 2 joins
  `q5_jr1_t ⋈ lineitem_col_t` on `(custkey, orderkey)` via
  `q5_sort_key_t` (→ `q5_jr2_t`). Per-emit callback invokes
  `q5_admit_lineitem`. `q5_sort_key_t` is a dedicated shared
  sort-key abstraction (mirrors `geo::sort_key_t`): all wildcard
  semantics live in its `match()` and `matching_keys()`, hash is
  wildcard-blind, and `SKBuilder::project<q5_jr1_t>` strips
  linenumber to `WILDCARD_KEY` so `join_state::join_and_clear`
  preserves the jr1 buffer across per-lineitem cartesian flushes.
  Replaces a 2-field `q5_co_jk_t` workaround minted in commit
  105b66a7 to side-step a confused attempt to reuse the S2 view's
  primary key as a join key.
- **Phase 4 §7.2** (2026-05-09; **complete**) — S1 `query_by_base`
  via 2-BMJ chain over custkey-sorted COL split indexes: BMJ #1
  joins `customerh_t ⋈ orders_coli_t` on `custkey` (→ `q5_jr1_t`),
  BMJ #2 joins `q5_jr1_t ⋈ lineitem_col_t` on
  `(custkey, orderkey, linenumber)` via `q5_sort_key_t`
  (linenumber=`WILDCARD_KEY` on the build side; → `q5_jr2_t`).
  No `LineitemRevenueAggregator`
  pre-aggregate — Q5 needs per-lineitem `l_suppkey` for SUPPLIER probe.
  Per-emit callback invokes `q5_admit_lineitem` (shared with S3/S4).
  Test harness flipped to strict 4-way parity: SF=1 all four paths
  `[OK]` at matching non-zero digest, rows=1. Q3 (rows=9) and Q3I
  (rows=10, S1–S5) regressions clean.
- **Phase 5–8** — `per_structure_workload.hpp` alias-only,
  executables, `test_query_q5_{lsm,btree}` strict parity, CMake —
  all landed in Phase 0.5 skeleton.
- **Phase 9** (2026-05-11; **complete**) — doc refresh: Q5 added to
  `frontend/tpch/CLAUDE.md` per-query subdirectories list, Tests
  table (`test_query_q5_{lsm,btree}` + `test_side_tables`), and
  Per-query test commands. Top-level `CLAUDE.md` repo markdown
  index updated for `LINUX_HISTORY.md`.
- **Phase 10A** (2026-05-11; **complete**) — semi-join refactor,
  docs + DOT plans only. Replace `nation_name_map` with emit-time
  NATION primary-index lookup; replace `supplier_nation_map` with
  composite-key `supplier_nation_set<(nationkey, suppkey)>`;
  reframe the two RN fan-outs asymmetrically (CUSTOMER ⋈ RN inner;
  SUPPLIER ⋉ RN semi); rewrite S4
  to use base tables with orderkey-keyed orders set and orderkey-only
  lineitem seeks (Q3-isomorphic). Updates: `q5/CLAUDE.md`,
  `plans/family_logical.dot`, `plans/baseline_s4.dot`,
  `plans/family_s3_physical.dot`. Code unchanged — see Phase 10B.
- **Phase 10B** (2026-05-11; **complete**) — code rewrite to match
  Phase 10A design. `q5_customer_rn_t` new record type (join-output of
  CUSTOMER ⋈ RN, carries c_nationkey + n_name). `supplier_nation_set`
  replaces `supplier_nation_map`: composite hashset keyed on
  (s_nationkey, s_suppkey); one per-lineitem probe fuses SUPPLIER
  semi-join + cross-equality + suppkey equi-join. `nation_name_map`
  removed; aggregator keys by n_name (resolved upstream); `emit(out)`
  needs no `sides` arg. `q5_admit_lineitem` templated on lineitem type;
  takes `cached_n_name`. `q5_resolve_n_name` lazy cache helper (NATION
  PK lookup, ~5 per query). S2 view loader FD-attaches `n_name` per
  customer row at load time. S4 rewritten on BASE adapters
  (`orders`, `lineitem`, not `col.split_*`); `orders_set` is PK-only;
  lineitem probe is sequential with seek-on-miss skip cursor; ORDERS PK
  lookup per orderkey transition resolves the joined customer record.
  `test_query_q5_lsm` strict 4-way XOR parity at SF=1:
  digest=0x2df0d67874759c18. Q3/Q3I regressions clean.
- **S5** — omitted by design (no parameter-independent aggregate
  to bake; see §Storage Structure Options).
- **Linux perf sweep** — pending; tracked in `LINUX_PENDING.md`
  (active worklist; rotated to `LINUX_HISTORY.md` once landed).

---

## Implementation Status (Phase 10B complete — 2026-05-11)

Phase 0.5 landed: all 8 per-query files exist, executable links,
`test_query_q5_lsm` runs to exit 0 with all four paths agreeing on
empty results (digest 0x0, vacuous `[OK]` parity). Q3 / Q3I
regressions clean.

Phase 1 landed: S1 BMJ chain intermediate types added to `views.hpp`
(`q5_cust_jk_t` id=58, `q5_jr1_t` id=59, `q5_jr2_t` id=60,
`q5_sort_key_t` id=61) with `std::hash` and `SKBuilder` specialisations.
`q5_pipeline_view_t::Key` is a plain primary key with default
`operator<=>` only — all join semantics live on `q5_sort_key_t`, the
dedicated shared sort-key abstraction (mirrors `geo::sort_key_t`).
Wildcard handling is uniform across all three fields of the sort key:
`match()` uses `wildcard_match` per field, `matching_keys()` enumerates
every prefix anchor, `operator%` / `std::hash` are wildcard-blind.
`SKBuilder<q5_sort_key_t>::project<q5_jr1_t>` strips linenumber to
`WILDCARD_KEY`, which is what makes BMJ's per-record-type clear in
`join_state::join_and_clear` preserve the jr1 buffer across
per-lineitem cartesian flushes. BMJ #2 right side is `lineitem_col_t`
(per-lineitem, not pre-aggregated —
needed for per-lineitem SUPPLIER probe). No payload extensions
needed. All four tests pass; Q3 / Q3I regressions clean.

Phase 1 (cont.) landed: `workload.hpp` PARAM_TABLE (REGION × DATE,
10 entries), real `set_params_for_iter`, real `q5_predicate_orders`
body (orderdate window), `Q5Stats` counter struct.

Phase 4 landed (all sub-phases in one session, 2026-05-09):

- **S3** — `Q5GroupWalkVisitor` + `col_group_walk` + `NNameRevenueAggregator`.
- **S2** — sequential `q5_pipeline_view_t` scan, all filters live.
- **S4** — 2-stage HashJoin chain; per-emit → `q5_admit_lineitem`.
- **S1** — 2-BMJ chain; per-emit → `q5_admit_lineitem` (no pre-agg).
- **Test harness** — flipped to strict 4-way parity. SF=1: all four
  `[OK]` at matching non-zero digest. Q3/Q3I regressions clean.

**Record-type ids allocated**: see `q5/views.hpp` header comment.
Skeleton uses `customer_coli_t` / `orders_coli_t` from
`views_col.hpp` directly; `lineitem_col_t` was extended in place to
carry `l_suppkey` and `l_returnflag` in commit `bec67300`.

Phase 10A landed (2026-05-11): semi-join design locked in docs and
DOT plans. Phase 10B landed (2026-05-11): C++ rewrite complete.
`nation_name_map` removed; `supplier_nation_map` replaced by
`supplier_nation_set<(nationkey, suppkey)>`; `q5_customer_rn_t` new
record type carries c_nationkey + n_name downstream. S4 rewritten on
base adapters (not `col.split_*`). All four `query_by_*` bodies use
one composite-key probe per lineitem. Strict 4-way XOR parity at SF=1:
digest=0x2df0d67874759c18. Linux perf sweep pending (`LINUX_PENDING.md`).
