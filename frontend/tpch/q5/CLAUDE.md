# Q5: Local Supplier Volume

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
- **Cross-record predicate** `c_nationkey = s_nationkey`. This is
  the only Q5 filter that cannot be pushed below the SUPPLIER
  hashmap lookup; it fires per qualifying lineitem. None of the
  other ~5 predicates are cross-record.
- **Wider per-lineitem callback.** Each surviving lineitem must
  consult the SUPPLIER hashmap and resolve a nationkey for the
  GROUP BY. This is one extra constant-time lookup per lineitem
  versus Q3's pure-revenue accumulator.

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
- NATION (25 rows): hash build on `n_regionkey`, restricted to the
  chosen region — yields ~5 `(n_nationkey → n_name)` entries.
- SUPPLIER (~10K rows at SF=1): hash build on `s_suppkey`,
  restricted to suppliers whose `s_nationkey` is in the in-region
  NATION map — yields ~2K `(s_suppkey → s_nationkey)` entries.

These are dimension lookups, not chain participants. They do **not**
contribute M:N rows to the join output. The `c_nationkey =
s_nationkey` cross-equality is a per-lineitem post-lookup filter,
not a fourth join level. **This is not a §3.1.2 sibling pattern**
(no per-customer scalar reduction of an out-of-chain table) and
**not a §3.1.3 genuine-tree pattern** (no co-located sub-hierarchy
inside the MI). The MI benefit is purely hierarchical-prefix scan
locality on the COL chain — anti-pattern #27 forbids importing
"no sibling shortcut" wording from sibling-aggregate framings.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy | Params baked in |
|---|----------|--------------------|---|---|
| 1 | Traditional indexes + binary merge join | Custkey-sorted secondaries on ORDERS (`(custkey, orderkey)`) and LINEITEM (`(custkey, orderkey, linenumber)`) | BMJ chain: customer ⋈ orders\_sec ⋈ lineitem\_sec on the custkey-extended prefix; SUPPLIER + NATION + REGION as side hash builds | none |
| 2 | Intermediate pipeline view | `q5_pipeline_view_t` (per-lineitem rows keyed by `(custkey, orderkey, linenumber)` carrying `l_extendedprice`, `l_discount`, `l_suppkey` + FD-attached `c_nationkey`, `o_orderdate`) | View scan + orderdate filter live; SUPPLIER hashmap lookup per lineitem; per-`n_name` `HashAggregate` | none |
| 3 | MI[COL] only | `MergedAdapter<customer_col_t, orders_col_t, lineitem_col_t>` keyed by custkey-prefixed tagged keys | `col_group_walk[_fused_emit]` over the COL MI; CUSTOMER hierarchy co-located, no separate join; SUPPLIER + NATION + REGION as side hash builds | none |
| 4 | Traditional indexes + hash join | None | `Scan(customer)` + nation-set gate → `HashJoin(⋈orders)` → orderdate filter → `HashJoin(⋈lineitem)` → SUPPLIER probe + cross-equality → per-`n_name` `HashAggregate` | none |

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
  All three agree on filter placement, aggregate shape (per-`n_name`
  HashAggregate above the chain), and the side-table attachments;
  only the inside-pipeline physical operator differs.
- `plans/family_s3_physical.dot` — S3 physical specialisation. A
  single `col_group_walk[_fused_emit]` over the 3-table COL
  MergedAdapter subsumes the per-table filters, the customer
  nation-set gate, and the 2-way join; the per-lineitem SUPPLIER
  probe and per-`n_name` aggregator wrap the walker callback.
- `plans/baseline_s4.dot` — S4 baseline. A HashJoin chain over base
  tables with `r_name` pushed down at the REGION scan, the
  in-region nation-set computed before the customer scan, and
  `c_nationkey ∈ nation_set` fused with the customer scan.

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
- The in-region `nation_set` (computed pre-pipeline from REGION ⋈
  NATION) gates the CUSTOMER scan: `c_nationkey ∈ nation_set`
  fuses with `TableScan(CUSTOMER)` (or with the COL walker's
  `on_customer` hook for S3).
- `o_orderdate >= :d AND o_orderdate < :d+1y` fuses with
  `TableScan(ORDERS)` (or with the walker's `on_order` hook).
- The cross-record predicate `c_nationkey = s_nationkey` fires
  per qualifying lineitem, immediately after the SUPPLIER
  hashmap probe — earliest possible position. This filter is
  **not** in the secondary (it depends on per-row CUSTOMER and
  SUPPLIER state simultaneously).
- For S4 specifically, `r_name` is pushed below the REGION scan,
  and `c_nationkey ∈ nation_set` is fused with the CUSTOMER
  scan **before the hash build**, so the customer hash table only
  contains qualifying customers.
- For S3, every filter except the cross-equality applies **during**
  the group walk, at the Visitor's `on_*` hook for that record
  type. There is no post-walk Filter node.

### How the four approaches differ

**S3 (MI[COL] + COLGroupWalk)** is the tightest expression of the
plan. The COL tagged-key encoding co-locates customer, orders, and
lineitem records by `custkey` in byte-lex order
(`customer → (orders → lineitem*)+`), and the walker streams
through them in a single forward pass. The customer nation-set
gate at `on_customer` skips the entire group; the orderdate
filter is an inline `if` check at `on_order`; per-lineitem the
walker probes SUPPLIER, applies the cross-equality, resolves
`s_nationkey → n_name` via the NATION map, and accumulates
revenue into the per-`n_name` bucket. No buffering of MI rows,
no hashmaps over MI rows, no separate aggregate pass — only the
two small reduce-side hashmaps (NATION, SUPPLIER) prebuilt before
the walk.

**S1 (custkey-sorted secondaries + BMJ chain)** runs the same
logical plan as S3 over three separate custkey-sorted streams
(CUSTOMER + two secondary indexes on ORDERS and LINEITEM). The
secondary keys (`(custkey, orderkey)` for orders,
`(custkey, orderkey, linenumber)` for lineitem) place both inputs
in the order required by the BMJ chain. Implemented as a 3-way
streaming merge that dispatches to the **same Visitor** as S3.
The accumulators are reused verbatim. S1 differs from S3 only in
I/O pattern: three trees instead of one, three scanner-advance
calls per group instead of one.

**S2 (materialised pipeline view)** caches per-lineitem rows of
the post-join (pre-aggregate) family plan as a
`q5_pipeline_view_t` table at load time, keyed by
`(custkey, orderkey, linenumber)` and carrying
`l_extendedprice`, `l_discount`, `l_suppkey`, plus FD-attached
`c_nationkey` and `o_orderdate`. **No filters are baked into the
view** — `r_name`, the in-region nation set, and orderdate are
all parameterised, so predicate hoisting forbids fusing them at
load time (PLAYBOOK soundness rule). Query time is a sequential
view scan: the customer nation-set gate, orderdate filter,
SUPPLIER probe, and cross-equality all apply live; per-`n_name`
revenue is rolled up via a `HashAggregate` keyed on `n_name`.
The view is reusable across all REGION and DATE param sets.

**S4 (HashJoin chain baseline)** uses base tables only (nothing
is custkey-sorted). Filter pushdown: `r_name` fuses with the
REGION scan; the resolved `nation_set` gates the CUSTOMER scan
before the hash build; orderdate fuses with ORDERS pre-build.
The chain is `Scan(CUSTOMER) → Filter(c_nationkey ∈ nation_set)
→ HashBuild → HashProbe(ORDERS pre-filtered) → HashProbe(LINEITEM)
→ Probe(SUPPLIER) → Filter(c_nationkey = s_nationkey) →
HashAggregate(n_name)`. S4 measures the no-merged-index baseline
that the family is compared against.

### Comparison axis summary

All four plans share the same logical shape (chain join + side-
table reductions + per-`n_name` HashAggregate); they differ only
in physical operators and filter substrate.

| Approach | Inside-pipeline physical | Aggregate | Filters resolved by |
|----------|--------------------------|-----------|---------------------|
| S1 (merge family) | 3-way custkey BMJ chain over secondaries; SUPPLIER + NATION + REGION as side hash builds | HashAggregate per `n_name` (Visitor `accumulator`, same code as S3) | Visitor `on_*` hooks (same code as S3) |
| S2 (merge family) | sequential per-lineitem view scan; SUPPLIER + NATION + REGION as side hash builds | HashAggregate per `n_name` above the view scan | All filters live at query time (no filter baking — view is param-reusable) |
| S3 (merge family) | `col_group_walk` over MI[COL]; SUPPLIER + NATION + REGION as side hash builds | HashAggregate per `n_name` (Visitor `accumulator`, same code as S1) | Visitor `on_*` hooks (same code as S1) |
| S4 (baseline) | HashJoin chain over base tables; SUPPLIER + NATION + REGION as side hash builds | HashAggregate per `n_name` above the chain | TableScan-time filters all pushed below the corresponding hash build/probe |

All four agree on what's outside the pipeline: collect the
per-`n_name` revenue map, sort by `revenue DESC`, output ~5
rows (one per in-region nation). **No LIMIT** — the result
cardinality is bounded by `|nation_set|` ≈ 5.

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
  `n_nationkey` values (~5 entries). Built from the NATION
  base-table scan filtered by `n_regionkey == :region_key`.
- `nation_name_map` — `std::unordered_map<Integer, std::string>`
  from `n_nationkey` to `n_name` (same ~5 entries).
- `supplier_nation_map` — `std::unordered_map<Integer, Integer>`
  from `s_suppkey` to `s_nationkey`, restricted to
  `s_nationkey ∈ nation_set` (~2K entries at SF=1).

All three are built before the COL walk / chain join begins;
their build cost is sub-millisecond and is included in the per-TX
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
- **Phase 2** — `workload.hpp` Params + predicate declarations.
- **Phase 3** — `load.tpp` real bodies for all 4 storage variants.
- **Phase 4 §7.1** — S3 `query_by_merged` via shared
  `col_group_walk` with a Q5-specific Visitor (CRTP subclass of
  the shared `q3_family::Q3FamilyVisitor` if extended for Q5;
  otherwise a fresh `q5_family::` namespace).
- **Phase 4 §7.2** — S1 `query_by_base` via 2-way BMJ chain over
  COL split indexes.
- **Phase 4 §7.3** — S2 `query_by_view` via `q5_pipeline_view_t`
  scan.
- **Phase 4 §7.5** — S4 `query_by_hash` via HashJoin chain.
- **Phase 5–8** — `per_structure_workload.hpp` alias-only,
  executables, `test_query_q5_{lsm,btree}` strict parity, CMake.
- **Phase 9** — doc refresh + cross-link from
  `frontend/tpch/CLAUDE.md` once skeleton acquires real code.
- **S5** — omitted by design (no parameter-independent aggregate
  to bake; see §Storage Structure Options).
- **Linux perf sweep** — pending; tracked in `LINUX_PENDING.md`.

---

## Implementation Status (Phase 1 — 2026-05-09)

Phase 0.5 landed: all 8 per-query files exist, executable links,
`test_query_q5_lsm` runs to exit 0 with all four paths agreeing on
empty results (digest 0x0, vacuous `[OK]` parity). Q3 / Q3I
regressions clean.

Phase 1 landed: S1 BMJ chain intermediate types added to `views.hpp`
(`q5_cust_jk_t` id=58, `q5_jr1_t` id=59, `q5_jr2_t` id=60) with
`std::hash<q5_cust_jk_t::Key>` specialisation and `SKBuilder`
specialisations for `q5_cust_jk_t::Key` and extended
`q5_pipeline_view_t::Key`. BMJ #2 right side is `lineitem_col_t`
(per-lineitem, not pre-aggregated). No payload extensions needed.
All four tests pass; Q3 / Q3I regressions clean.

**Real bodies**:

- `Params::defaults()` — `{"ASIA", DATE_1994_01_01}`.
- `set_params_for_iter(long)` — stubbed to `params = Params::defaults()`
  (PLAYBOOK §3.6 wrapper-uniformity requirement; non-stub body in
  Phase 5).
- `populate_q5_view` — 3-way custkey-sorted manual merge; loaded
  unfiltered, param-reusable.
- `q5_pipeline_view_t::unfoldKey`, `q5_agg_row_t::print`.
- All four storage-structure dispatch arms in `load.tpp`,
  `get_size`.
- `q5_cust_jk_t`, `q5_jr1_t`, `q5_jr2_t` — S1 BMJ chain types.
- `SKBuilder<q5_cust_jk_t::Key>` — `create` overloads for
  `customerh_t`, `orders_coli_t`, `q5_jr1_t`.
- `SKBuilder<q5_pipeline_view_t::Key>` — extended with `create`
  overloads for `q5_jr1_t` and `lineitem_col_t`.

**Stubs** (next commits, Phase 2+):

- All four `query_by_*` bodies → return 0 (`out.clear(); return 0;`).
- Predicate bodies (`q5_predicate_customer/orders/lineitem`) →
  `return true;`.
- `Q5Stats` counter struct — deferred to Phase 4.

**Record-type ids allocated**: see `q5/views.hpp` header comment.
Skeleton uses `customer_coli_t` / `orders_coli_t` from
`views_col.hpp` directly (per the design-doc correction);
`lineitem_col_t` was extended in place to carry `l_suppkey` and
`l_returnflag` in commit `bec67300`.

Next commit: Phase 2 — `workload.hpp` Params + predicate declarations.
