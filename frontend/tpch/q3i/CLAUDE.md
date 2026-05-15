# Q3I: Shipping Priority × Customer Outstanding Balance (Invoice-Extended Q3)

**Reading guide**: For SQL and plan descriptions, read §TPC-H Definition and §Plan Descriptions. For implementation status and phase history, read §Implementation Phases. For perf investigation context, read `PERFORMANCE.md`. For OutClass / wildcard / anti-pattern details, see `../CONVENTIONS.md`. Skip the rest unless reconstructing a historical decision.

## Sibling Docs

Every non-`CLAUDE.md` Markdown in `q3i/` and `q3i/plans/` (the latter
has no `CLAUDE.md`; indexed here as the nearest ancestor). Read each
on the trigger described:

- [`PERFORMANCE.md`](PERFORMANCE.md) — read **when investigating Q3I
  S3 performance** or interpreting the cross-backend (RocksDB-LSM,
  LeanStore-BTree) measurements at SF=15/40, dram=0.1 GiB. Active
  worklist post-A2c: S3 now matches S1 cache-resident; the open
  question is whether any disk-pressure operating point flips S3
  positive (A6). Historical evidence in `archive/`.
- [`plans/family_logical.dot`](plans/family_logical.dot),
  [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot),
  [`plans/baseline_s4.dot`](plans/baseline_s4.dot) — operator-graph
  diagrams; read alongside §Plan Descriptions below.
- [`plans/phase_2a_refactor.md`](plans/phase_2a_refactor.md) — read
  **when reconstructing the Phase 2A shared-substrate refactor**
  (renames, shared helpers, view-schema widening, S3 internal
  cleanup). Historical; the work is landed.
- [`plans/phase_2b_baselines.md`](plans/phase_2b_baselines.md) — read
  **when reconstructing Phase 2B** (S1 / S2 / S4 baseline `query_by_*`
  bodies introduced one commit per step). Historical; landed.
- [`plans/phase_2c_harness.md`](plans/phase_2c_harness.md) — read
  **when reconstructing Phase 2C** (cross-structure parity check, S3
  `flush_order` zero-revenue fix, S2 `populate_q3i_view` filter fix).
  Marked complete 2026-05-02.

## TPC-H Definition (Extended)

```sql
SELECT  o_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority,
        cust_open_due
FROM    customer, orders, lineitem,
        (SELECT i_custkey, SUM(i_totaldue) AS cust_open_due
           FROM invoice
          WHERE i_status = 'O'
          GROUP BY i_custkey) AS oi
WHERE   c_mktsegment = ':1'
  AND   c_custkey    = o_custkey
  AND   c_custkey    = oi.i_custkey
  AND   l_orderkey   = o_orderkey
  AND   o_orderdate  < ':d'
  AND   l_shipdate   > ':d'
  AND   oi.cust_open_due > :threshold
GROUP BY o_orderkey, o_orderdate, o_shippriority, cust_open_due
ORDER BY revenue DESC
LIMIT 10;
```

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:1` | Market segment names (e.g. BUILDING, AUTOMOBILE, MACHINERY) | Customer market segment filter |
| `:d` | March 15 of a year in \[1993, 1997\] | Order date upper bound / ship date lower bound |
| `:threshold` | Non-negative numeric | Minimum per-customer open invoice total to include |

**Validation values**: SEGMENT = BUILDING, DATE = 1995-03-15, THRESHOLD = 0.

---

## Motivation

Q3I is the **COLI MI showcase** for combining §3.1.3 hierarchical join with a
§3.1.2 sibling sub-aggregate:

- **Why not plain Q3?** Q3 joins CUSTOMER × ORDERS × LINEITEM and returns the
  top-10 unshipped orders by revenue. Adding `cust_open_due` requires grouping
  INVOICE by `i_custkey` before the main join — a correlated sub-aggregate that
  is expensive to compute separately for each custkey in S1/S4.

- **Why the COLI MI?** The 4-table merged index `MI(customer, orders, lineitem,
  invoice)` keyed by `custkey` co-locates all four record types per customer.
  One `PremergedJoin` pass computes `cust_open_due` while scanning invoices
  (§3.1.2 sibling within the custkey group) and accumulates revenue from the
  orders × lineitem sub-hierarchy (§3.1.3). The threshold filter can be applied
  early to skip custkey groups entirely.

- **Comparison story**: S3 (MI\[COLI\]) amortises the per-customer invoice scan
  across the hierarchical join, whereas S1/S4 must build a separate invoice
  hashmap before the main join. At high scale factors the COLI scan's locality
  should dominate.

---

## Cardinality structure: not a true 4-way M:N

Despite the SQL appearing to join four tables, Q3I is **not** a genuine 4-way
many-to-many join. INVOICE is reduced to a per-custkey scalar
(`cust_open_due = SUM(i_totaldue) WHERE i_status='O'`) by the inline derived
table *before* any join with ORDERS or LINEITEM. `c_custkey = oi.i_custkey`
then attaches that scalar to CUSTOMER as a sibling of the C-O-L hierarchy.

- INVOICE × {C, O, L}: 1:1 after the sub-aggregate; `i_invoicekey` and
  `l_invoicekey` are not consulted by Q3I.
- CUSTOMER × ORDERS: 1:N on `c_custkey = o_custkey`.
- ORDERS × LINEITEM: 1:N on `o_orderkey = l_orderkey`.

The COLI MI exploits this asymmetry: invoice records sit *alongside* the C-O-L
hierarchy under each custkey (sibling sub-aggregate, §3.1.2), and the walker
finalises `cust_open_due` at the invoice→orders boundary inside each custkey
group. The `lineitem_t.l_invoicekey` payload is reserved for future workloads
that *do* join lineitem with its invoice.

**Implication for benchmarking**: Q3I demonstrates §3.1.2 (sibling
sub-aggregate) cleanly, but is not a §3.1.3 hierarchical-M:N showcase. A
stronger §3.1.3 query — where all four record types contribute many-to-many
rows — is a separate follow-up.

This is also why the `joinN_output_rows` counters are blank for S3: there is
no 3-stage join chain, just one fused walk.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + merge join | None | Pre-build cust\_open\_due map from INVOICE; BinaryMergeJoin(OL) + CUSTOMER hash lookup |
| 2 | Intermediate pipeline view | `q3i_pipeline_view_t` (joined\_ol\_t rows) | Pre-build cust\_open\_due map; view scan + CUSTOMER hash filter |
| 3 | MI\[COLI\] only | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | PremergedJoin over 4-table tagged-key MI; cust\_open\_due computed in same pass |
| 4 | Traditional indexes + hash join | None | Pre-build cust\_open\_due map; HashJoin(OL) + CUSTOMER hash lookup |
| 5 | aCOLI MI (pre-aggregated) — **deferred from paper sweep** ([`PLAYBOOK §S5`](../PLAYBOOK.md)) | `MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>` (`orders_acoli_t` retired Step 4b — collapsed into `orders_coli_t`) | Scan 3-type MI; pre\_open\_due read directly; revenue recomputed from unaggregated lineitems |

---

## Plan Descriptions

Three DOT files in [`plans/`](plans/) document the operator graphs for the
four storage structures:

- [`plans/family_logical.dot`](plans/family_logical.dot) — **shared logical
  plan for S1, S2, S3.** The merged-index family agrees on this graph; only
  the inside-pipeline physical operator differs (the comparison axis).
- [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot) — **S3
  physical specialisation.** A single `coli_group_walk` over the 4-table
  COLI MergedAdapter subsumes the per-table filters, both SortedAggregates,
  the threshold filter, and the 3-way join.
- [`plans/baseline_s4.dot`](plans/baseline_s4.dot) — **S4 baseline.** A
  HashJoin chain over base tables with two pre-built hashmaps standing in
  for the missing custkey ordering.

### Filter pushdown principle (applied across all three plans)

See the canonical rule in [Filter Pushdown](../OPERATORS.md#filter-pushdown).
The Q3I-specific application:

Every parameterised filter is pushed as far down the operator graph as
possible, **stopping only at secondary structures** so they remain
reusable across param sets (predicate hoisting). For Q3I this means:

- The COLI MI, the COLI custkey-sorted secondaries, and the
  `q3i_pipeline_view_t` are all loaded **without** applying mktsegment,
  threshold, orderdate, shipdate, or status='O' filters. A new param set
  triggers a new query, not a new load.
- Single-table filters (`i_status`, `c_mktsegment`, `o_orderdate`,
  `l_shipdate`) fuse with their TableScan at query time.
- The threshold filter on `cust_open_due` fuses with the per-customer
  SortedAggregate — it fires the moment the aggregate value is finalised
  for a custkey, **before** that custkey enters the OL join. It is never
  a post-join Filter node.
- For merged-index physical execution (S3), the principle is sharper:
  every filter applies **during** the group walk, at the Visitor's
  `on_*` hook for that record type. There is no post-walk Filter node —
  by the time a row leaves `coli_group_walk`, it has passed every
  per-table predicate, the mktsegment gate, AND the threshold gate.

### How the four approaches differ

**S3 (MI[COLI] + COLIGroupWalk)** is the tightest expression of the
plan. The COLI tagged-key encoding co-locates customer, invoice, orders,
and lineitem records by `custkey` in byte-lex order
(`customer → invoice* → (orders → lineitem*)+`), and the walker
streams through them in a single forward pass. Every filter, both
sub-aggregates, and the join all fuse into the Visitor's hooks. The
mktsegment gate at `on_customer` skips the entire group; the threshold
gate at `flush_order` skips per-orderkey emission; the per-table date
filters are inline `if` checks at `on_order` / `on_lineitem`. No
buffering, no hashmaps, no separate aggregate pass — this is the
operator-level expression of the §3.1.2 sibling + §3.1.3 hierarchical
hybrid pattern.

**S1 (custkey-sorted secondaries + 4-way merge)** runs the same
logical plan as S3 over four separate custkey-sorted streams (CUSTOMER
+ three COLI secondary indexes). Implemented as a 4-way streaming
merge (`coli_secondary_group_walk`) that dispatches to the **same
Visitor** as S3. The accumulators (`CustomerOpenDueAccumulator`,
`LineitemRevenueAccumulator`) are reused verbatim. S1 differs from S3
only in I/O pattern: four trees instead of one, four scanner-advance
calls per group instead of one. This is what makes the S1-vs-S3
comparison apples-to-apples per OPERATORS.md §6.1 — same logical
plan, same accumulator code, same filter pushdown, only the physical
scan substrate differs.

**S2 (materialised pipeline view)** caches the post-aggregate output
of the family logical plan as a `q3i_pipeline_view_t` table at load
time. The view is loaded with the per-table filters fused but
**without** the mktsegment or threshold filters (predicate hoisting),
so it is reusable across param sets. Query time is then a sequential
view scan, mktsegment + threshold filters applied per row, then sort
+ limit. S2 measures "what if we paid for the inside-pipeline work
once at load time" against S1/S3's "do it every query".

**S4 (HashJoin chain baseline)** uses Calcite's default plan because
nothing is custkey-sorted. The same filter-pushdown principle applies:
per-table filters fuse with TableScans before HashJoin, the threshold
filter fuses with the invoice aggregate so only surviving custkeys
land in `open_due`, and mktsegment fuses with the customer scan so
only surviving custkeys land in `cust_seg`. Probe-time hashmap
lookups against `open_due` and `cust_seg` are then **lookups, not
filters** — the predicates were applied earlier; absence from a map
just means "this row was already excluded". The remaining
`unordered_map<orderkey, q3i_agg_row_t>` is the hashmap tax S4 pays
for unsorted HashJoin output, not a filter. S4 measures the
no-merged-index baseline that the family is compared against.

### Comparison axis summary

| Approach | Inside-pipeline physical | Filters resolved by |
|----------|--------------------------|---------------------|
| S1 (merge family) | 4-way custkey merge over secondaries | Visitor `on_*` hooks (same code as S3) |
| S2 (merge family) | sequential view scan | TableScan-time filters baked into view; mktsegment / threshold per-row at query time |
| S3 (merge family) | `coli_group_walk` over MI[COLI] | Visitor `on_*` hooks (same code as S1) |
| S4 (baseline)     | HashJoin(O ⋈ L) + 2 probe hashmaps | TableScan + per-aggregate; probe lookups encode the rest |
| S5 (aCOLI MI) — **deferred** | scan `MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>` (`orders_acoli_t` retired Step 4b) | pre\_open\_due read directly; revenue recomputed live; mktsegment / threshold / orderdate / shipdate per-row |

S5 is implemented and parity-verified at SF=1 but **deferred from
the paper sweep** across all queries — see
[`PLAYBOOK.md §S5`](../PLAYBOOK.md). All five agree on what's outside
the pipeline: a single `TopNSink<q3i_agg_row_t, Cmp>(10, cmp)` that
the pipeline pushes into via `sink.offer(...)`, drained at the end
via `sink.drain_sorted(out)`.  See OutClass subsection below.

### OutClass: shared `TopNSink<q3i_agg_row_t, Cmp>`

All five `query_by_*` bodies construct one
`TopNSink<q3i_agg_row_t, decltype(cmp)>(10, cmp)` per query and push
qualifying rows through `sink.offer(...)`; final
`sink.drain_sorted(out)` materialises the 10 result rows.  Same
shape as Q3 — the only difference is the row type carries
`cust_open_due` (`q3i_agg_row_t` derives from
`q3_family::q3_agg_row_base_t`).  `cmp` is the inherited
`q3_agg_row_base_t::cmp`
(`revenue DESC, o_orderdate ASC, o_orderkey ASC`).

This is Q3I's instance of the **OutClass** convention codified in
`CONVENTIONS.md §Post-pipeline OutClass`.  Memory is `O(K) = O(10)`, not `O(orders
passing all filters)` — important at SF=15 / SF=40 where the
pre-`458bcaf0` `std::vector<q3i_agg_row_t> + apply_topN` shape
buffered millions of rows just to discard 99.999% of them
(see `CONVENTIONS.md §Anti-Pattern Reference` #30).

The visitor (`COLIGroupWalkVisitor<Sink>`) is template-on-Sink
because `Q3FamilyVisitor` now takes the sink type as a template
parameter.  Q3I's derived class additionally needs:

```cpp
using Base = q3_family::Q3FamilyVisitor<COLIGroupWalkVisitor<Sink>,
                                         Params, lineitem_coli_t,
                                         q3i_agg_row_t, Q3IStats, Sink>;
using Base::params;
using Base::stats;
using Base::sink;
```

The `using Base::...` declarations bring the dependent-base members
into scope so the override hooks (`on_invoice`,
`per_order_admit_check`, `extra_emit_fields`, `on_group_end`) can
reference `params` / `stats` / `sink` unqualified.  Without the
`using`s, two-phase name lookup would fail because `Base` is
dependent on the `Sink` template parameter.

Per OPERATORS.md §7 rule 5, the same OutClass instance is shared
across all five storage variants for comparison-integrity at the
post-pipeline boundary.  `NNameRevenueAggregator` plays the
analogous role for Q5 (no LIMIT; HashAggregate IS the OutClass).

---

## Required Record Types

- `q3i_pipeline_view_t` — real struct keyed by `(custkey, orderkey)`; one row
  per order. Carries `revenue`, `cust_open_due`, `c_mktsegment`, `o_orderdate`,
  `o_shippriority`. Fully implemented (Phase 2).
- `q3i_agg_row_t` — final output row: `o_orderkey`, `revenue`, `o_orderdate`,
  `o_shippriority`, `cust_open_due`. Derives from `q3_family::q3_agg_row_base_t`
  (now in `q3_family/agg_row.hpp`) which owns the four base fields and the
  `cmp` comparator; Q3I adds `cust_open_due` and overrides `print()`.
- `customer_acoli_t` / `orders_coli_t` (reused) / `lineitem_acoli_t` — S5
  aCOLI MI record types; defined in `views_coli.hpp`. IDs 49 / 1 / 53.
  `orders_acoli_t` (id=50) was retired Step 4b (2026-05-03): after switching
  aCOLI types to tagged-key encoding its key became byte-identical to
  `orders_coli_t`, so the type was collapsed via
  `using orders_acoli_t = orders_coli_t`. `customer_acoli_t` carries
  `pre_open_due` (invoice sub-aggregate, baked at load time).
  `lineitem_acoli_t` carries the projected lineitem payload; revenue is
  computed at query time.

---

## File Structure

| File | Status |
|------|--------|
| `views.hpp` | Complete — `q3i_pipeline_view_t`, `cust_open_due_t`, `lineitem_agg_t`, join result types `q3i_jr{1,2,3}_t`, `q3i_agg_row_t`; `SKBuilder` specializations for both join key types |
| `workload.hpp` | Complete — `Q3IWorkload<Backend>` with all adapter members, `coli_pipeline()` accessor, `Q3IStats` (derives from `q3_family::Q3FamilyStats` — now in `q3_family/stats.hpp`), all `query_by_*` declarations |
| `per_structure_workload.hpp` | Complete — alias-only (`BaseQ3I`, `ViewQ3I`, `MergedQ3I`, `HashQ3I`) |
| `load.tpp` | Complete — ctor, `load()`, `get_size()`, `populate_q3i_view` free function (wraps `q3_family::populate_q3_view_core` — now in `q3_family/view_loaders.hpp`) |
| `query.tpp` | Complete — all four `query_by_*` bodies, accumulators, `COLIGroupWalkVisitor` (subclass of `q3_family::Q3FamilyVisitor` — now in `q3_family/coli_visitors.hpp`), predicates, `print()` |
| `executable_rocksdb.cpp` | Complete — full `main()`, dispatches all five storage structures via `TpchExecutableHelper` |
| `executable_leanstore.cpp` | Complete — same as above for LeanStore backend (`#ifndef ROCKSDB_ONLY`) |
| `CLAUDE.md` | This file |

---

## CMake Targets

Wired in `frontend/CMakeLists.txt`:

- `test_query_q3i_lsm` — cross-structure parity test (macOS + Linux).
- `q3i_lsm` — production RocksDB executable (macOS + Linux).
- `q3i_btree` — production LeanStore executable (Linux only).

`generate_targets.py` registers `q3i_lsm` / `q3i_btree` for Makefile
experiment integration; rerun it to regenerate `targets.mk` after edits.

---

## Tests

```bash
# Build (macOS)
make -C build/frontend test_query_q3i_lsm -j$(sysctl -n hw.ncpu)

# Run cross-structure parity test (SF=1)
mkdir -p build/scratch/q3i/{data,csv}
./build/frontend/test_query_q3i_lsm \
    --ssd_path=./build/scratch/q3i/data \
    --csv_path=./build/scratch/q3i/csv \
    --tpch_scale_factor=1
# Expected: 4 identical digests, 4 × row_count=10, exit 0.
# Verified: all four digests match within a single run (seed-dependent value
# varies across runs; cross-structure agreement is the invariant).
```

---

## Performance Notes

Two remediations have landed: A2c (`--coli_walker_variant=fused_emit`)
closed the per-record dispatch gap at SF=15 across both backends; A3
(Backend-trait customer-level Seek-skip) lifts LeanStore S3 +356% at
SF=15 and +116× at SF=40 disk-bound. **The paper's pitch — S3 ≥ S2 >
S1/S4 — holds at SF=15 on both backends** (S3 matches/beats the
fully-materialised view S2 without paying its storage / maintenance
cost, while comfortably beating S1 split-merge and S4 hash baselines).
RocksDB SF=40 disk-bound is the remaining open question (A6 dram
sweep). Active worklist:
[`PERFORMANCE.md`](PERFORMANCE.md). Historical evidence:
[`archive/PERFORMANCE-2026-05-03b.md`](archive/PERFORMANCE-2026-05-03b.md)
and
[`archive/PERFORMANCE-2026-05-03.md`](archive/PERFORMANCE-2026-05-03.md).

---

## Implementation Phases

> **Phase model note (2026-05-15)**: the PLAYBOOK has collapsed Phase 0.5
> (skeleton) into Phase 1. Historical references to "Phase 0.5" or "Phase 1"
> in this file describe completed work accurately under the old model.

### Phase 1 — Minimal end-to-end: merged path only (S3)

**Status (2026-05-01): complete.**

**Goal**: one runnable path that exercises the COLI MI showcase end-to-end.

**Landed:**

- `tpch_tables.hpp` — `DATE_1995_03_15 = 9204` constant.
- `q3i/query.tpp` — `Params::defaults()` (`BUILDING / 1995-03-15 / threshold=0`); all four predicates; `q3i_agg_row_t::print()`; **standalone accumulator structs** `CustomerOpenDueAccumulator` and `LineitemRevenueAccumulator` factored at namespace scope so Phase 2's S1 path reuses them unchanged (OPERATORS.md §6.1 comparison-integrity); `query_by_merged` body using `coli_group_walk` Visitor that delegates to the two accumulators.
- `q3i/load.tpp` — `load()` and `get_size()` dispatch on `FLAGS_storage_structure`. S3 calls `coli.populate_merged()`; S1 calls `coli.populate_split()`; S4 base-only; S2 TODO Phase 2.
- `q3i/executable_{rocksdb,leanstore}.cpp` — full `main()` mirroring Q12, dispatching all four storage structures via `tpch::dispatch_storage_structure`.
- `tests/q3i/test_query_q3i_phase1_rocksdb.cpp` + CMake target `test_query_q3i_phase1_lsm` — temporary Phase 1 harness that loads S3 and runs `query_by_merged` directly. Includes a `coli_group_walk` diagnostic Visitor reporting building-customer counts.
- `frontend/shared/randutils.hpp` — fixed `randomNumeric` to map `getRandU64()` correctly to `[min, max)`. Previous implementation divided by `RAND_MAX` (2^31), producing `~1e8` magnitudes for `l_discount` / `l_tax` instead of `[0, 0.1]` / `[0, 0.08]`. This is a real codebase-wide bug fix (Q12 didn't expose it because it doesn't read those fields). Q12 XOR-parity digest is unchanged: cross-structure agreement still holds within each run; absolute digest varies because the RNG sequence shifted (every previous Q12 run consumed `randomNumeric` calls during data generation, so changing its output naturally changes the seed-derived digest — but all four S1/S2/S3/S4 paths still see the same data and produce the same per-row totals).

**Resolved:** Earlier 0-row symptom was stale data from a pre-`randomNumeric`-fix load. After a clean reload, `query_by_merged` returns ~129 rows at SF=1 with correct `cust_open_due`. Defensive `memcpy` added in `variant_utils.hpp::toType` (and `LeanStoreMergedAdapter::toType`) to harden against potential alignment UB on platforms where RocksDB value buffers aren't 8-byte aligned.

**Not yet in Phase 1**: top-10 sort; baseline S1/S2/S4 paths.

**Exit criterion satisfied**: `test_query_q3i_phase1_lsm` runs end-to-end and exits with `[OK]   row_count > 0` (row_count ≈ 129 at SF=1).

---

### Phase 1 → Phase 2 design adjustments

Phase 1 surfaced four lessons that reshape the remaining phases:

1. **The two namespace-scope accumulators (`CustomerOpenDueAccumulator`,
   `LineitemRevenueAccumulator`) are the right unit of reuse.** S1 can drive
   them over the COLI custkey-sorted invoice secondary index + a parallel
   merge-join scan over the OL secondaries, with no body-level duplication
   from S3. This honours OPERATORS.md §6.1 comparison-integrity: the same
   per-record accumulation logic runs against every storage structure.
2. **`q3i_pipeline_view_t` is widened to a real struct keyed by `(custkey,
   orderkey)`.** The family logical plan's inside-pipeline `SortedAggregate`
   collapses all lineitems per orderkey into one revenue sum before the
   pipeline output is materialised. The view therefore has one row per
   `(custkey, orderkey)` — cardinality ≈ `|orders|` — far smaller than a
   `joined_ol_t` row per `(order, lineitem)` pair. Carrying `cust_open_due`,
   `c_mktsegment`, `o_orderdate`, and `o_shippriority` in the view row means
   S2's query time is a plain sequential scan with per-row mktsegment and
   threshold filters, requiring no secondary invoice lookup. This makes S2 a
   fair comparison point against S1/S3 (both of which compute `cust_open_due`
   in a single streaming pass) while keeping the view **unfiltered** on
   parametrised predicates so it stays reusable across param sets (predicate
   hoisting — see [Filter Pushdown](../OPERATORS.md#filter-pushdown)).
3. **Top-10 is cheap and belongs in Phase 2.** No reason to gate it behind
   a separate phase: `std::partial_sort_copy` over the per-orderkey result
   vec is one statement per `query_by_*` and gives spec-compliant output
   for the parity check itself.
4. **The stale-data trap (Phase 1's apparent bug) must not recur.** The
   single unified harness should always wipe its data dir before loading
   so a build that changes the RNG sequence or schema never reads old
   bytes.

---

### Phase 2 — All four paths, top-10, single harness

**Status (2026-05-02): complete.**

**Goal**: spec-compliant Q3I executable + a single `test_query_q3i_lsm`
harness that loads once, runs all four `query_by_*` paths against the same
data, and asserts XOR parity across all four. Replaces the temporary Phase 1
binary entirely.

**Landed:**

- `query.tpp` — `query_by_base` (S1: 3-BMJ chain over custkey-sorted COLI
  split indexes via `CustomerOpenDueAggregator` + `LineitemRevenueAggregator`
  scanner-wrappers), `query_by_view` (S2: sequential view scan with per-row
  mktsegment + threshold filters), `query_by_hash` (S4: 3-HJ chain over base
  tables). All four paths end with `apply_topN(..., 10, revenue DESC)`.
- `load.tpp` — `populate_q3i_view` free function (two-pointer merge over
  orders + lineitem, one row **per lineitem** keyed by
  `(custkey, orderkey, linenumber)`; no shipdate filter at load time —
  predicate hoisting; revenue computed at query time).
- `tests/q3i/test_query_q3i_rocksdb.cpp` — load-once harness: single
  `tpch.load()` + explicit `populate_q3i_view` / `populate_split` /
  `populate_merged`, all four paths against the same DB, XOR parity +
  row-count checks, top-10 printed.
- CMake: `test_query_q3i_lsm` added; `test_query_q3i_phase1_lsm` removed.

**Implementation notes:**

- `q3i_pipeline_view_t` is a real struct (not a `joined_ol_t` alias) keyed
  by `(custkey, orderkey, linenumber)`. One row **per lineitem** — cardinality
  ≈ `|lineitem|`. Carries `l_extendedprice`, `l_discount`, `l_shipdate`
  (unaggregated), plus FD-attached `cust_open_due`, `c_mktsegment`,
  `o_orderdate`, `o_shippriority`. Revenue is computed at query time with
  the shipdate filter applied live — view is reusable across all DATE param
  sets (predicate hoisting). **Revised 2026-05-03** from the original
  one-row-per-order schema which baked revenue with a hardcoded shipdate.
- `CustomerOpenDueAggregator<Backend>` and `LineitemRevenueAggregator<Backend>`
  are scanner-wrapper aggregators (OPERATORS.md §3 op 6) that drive
  `split_invoice` and `split_lineitem` respectively, emitting one aggregate
  row per group boundary. Both reuse the namespace-scope accumulator structs
  (`CustomerOpenDueAccumulator`, `LineitemRevenueAccumulator`) shared with S3.
- S1 3-BMJ chain: `customer ⋈ cust_open_due` (BMJ#1) `⋈ orders_coli_t`
  (BMJ#2) `⋈ lineitem_agg_t` (BMJ#3), all on custkey / (custkey,orderkey).
  S4 3-HJ chain is the isomorphic hash-join baseline: invoice aggregate
  hashmap + customer mktsegment map pre-built, then orders map filtered by
  date/custkey, then lineitem probe accumulating revenue per orderkey.
- The `secondary` → `split` rename (Phase 2A) disambiguates COLI split
  adapters from base-table secondary indexes.
- `apply_topN` epilogue is uniform across all four paths (OPERATORS.md §3
  op 8–9): `partial_sort_copy` into a 10-element output vector, ordered by
  `revenue DESC`.
- **S3 bug fixed (Phase 2C):** `COLIGroupWalkVisitor::flush_order` was
  emitting orders with `revenue=0` (orders whose lineitems all failed the
  shipdate filter). SQL requires a matching lineitem; zero-revenue orders
  are now suppressed.
- **S2 bug fixed (Phase 2C):** `populate_q3i_view` was not applying the
  `l_shipdate` filter; `query_by_view` intentionally does not re-apply it
  ("baked in at load time"). Filter now applied in the view loader.
  **Superseded 2026-05-03** by the S2 de-bake: the view now stores
  unaggregated per-lineitem rows without any shipdate filter; `query_by_view`
  applies the shipdate filter live at query time.

- **Top-10 tiebreaker fix (post-Phase-2C):** all four `apply_topN` calls now
  use `revenue DESC, o_orderdate ASC, o_orderkey ASC` as the comparator.
  The prior single-key `revenue DESC` comparator left `std::partial_sort`
  tie-breaking dependent on input order, which differs across paths (S2 scans
  view by `(custkey, orderkey)`, S4 iterates an `unordered_map`). On data
  states with revenue ties at the boundary, this produced three distinct
  digests. The fix is purely in the comparator — no query logic changed.
- **Defensive `--ssd_path` wipe in harness (post-Phase-2C):** the harness
  now `remove_all`s `FLAGS_ssd_path` before `rocks_db.open()`. Reusing a
  populated dir across re-runs caused `tpch.load()` to write new records on
  top of the prior DB (RocksDB does not cleanly overwrite), growing the
  lineitem count across consecutive SF=1 runs (e.g. 6051 → 7765 → 10017).
  The four query paths then read four subtly inconsistent loaded states,
  producing four distinct digests that look like query-body bugs but are
  actually a stale-fixture artefact. The wipe makes the harness re-runnable
  in place per its documented usage. The earlier "8520 lineitems on Linux"
  data-state hypothesis was retroactively invalidated — that run was on
  macOS with an accumulated DB, not a Linux RNG state.

**Exit criterion satisfied**: `test_query_q3i_lsm` at SF=1, SF=2, and SF=5
reports `[OK]` parity across S1/S2/S3/S4, 10 rows each (all four digests
identical within a run; value is seed-dependent across runs), exit 0.
Re-running the harness in place (no manual wipe between invocations) also
reports `[OK]` parity, because the harness now wipes its own ssd_path.

---

### Phase 3 — Production targets + experiment integration

**Status (2026-05-02): complete.**

**Landed:**

- `frontend/CMakeLists.txt` — `q3i_lsm` (macOS + Linux) and `q3i_btree`
  (Linux only), mirroring `q12_lsm` / `q12_btree`.
- `generate_targets.py` — `q3i_lsm` / `q3i_btree` added to `exec_names`,
  `DIFF_DIRS`, and `STRUCTURE_OPTIONS`; `targets.mk` regenerated and
  `.vscode/launch.json` refreshed.
- `q3i/executable_{rocksdb,leanstore}.cpp` already had full `main()`
  bodies from Phase 1; verified they dispatch all four storage
  structures via `TpchExecutableHelper`.
- `Q3IWorkload::load()` already dispatches on `FLAGS_storage_structure`
  (S1 → `populate_split`, S2 → `populate_q3i_view`, S3 →
  `populate_merged`, S4 → base only) so production loads exactly one
  secondary per structure.

**Skipped**: `tests/test_load_q3i_rocksdb.cpp` — `test_load_coli_lsm`
covers the relevant invariants and `test_query_q3i_lsm` cross-checks
all four structures.

**Run examples**:

```bash
# macOS smoke build
make -C build/frontend q3i_lsm -j$(sysctl -n hw.ncpu)

# Full experiment sweep (all four structures)
make q3i_lsm scale=1

# S3 in isolation for memory-pressure experiments
make q3i_lsm_3 dram=0.1
```

---

---

### Phase 4 — S5: aCOLI MI with pre-aggregated fields

**Status (2026-05-04): implementation complete and parity-verified, but
deferred from the paper sweep across all queries.** S5 (aCOLI MI) is
retained in tree as a working design reference — `q3i_lsm_5` /
`q3i_btree_5` build and parity-pass — but is not part of any reported
figure. See [`../PLAYBOOK.md §S5`](../PLAYBOOK.md) for the deferral
rationale (S3 > S5 anomaly traced to S5 lacking the hand-tuned
`coli_group_walk` — closing it is infrastructure, not per-query). The
phase notes below remain accurate as design / implementation history;
do not stand up new aCOLI MIs as part of paper work.

Earlier status: complete 2026-05-02; revised 2026-05-03 (revenue de-bake).

**Goal**: implement and verify the "MI-as-aggregate-store" research variant
(REVIEWS.md §1.1, R2-D1): a merged index that stores pre-computed aggregates
as included columns, sitting between raw co-location (S3) and full
materialisation (S2) on the pre-computation spectrum.

**Design (revised, Step 4b 2026-05-03):** `MergedAdapter<customer_acoli_t,
orders_coli_t, lineitem_acoli_t>` — three record types (invoice rows collapsed
to a scalar; lineitems stored unaggregated so revenue is computed at query time):

- `customer_acoli_t` (id=49): full `customerh_t` payload +
  `Numeric pre_open_due` = `SUM(i_totaldue WHERE i_status='O')` baked at
  load time. Parameter-independent (`i_status='O'` is hardcoded by spec).
- `orders_coli_t` (id=1): **reused** in place of the retired `orders_acoli_t`
  (id=50). After switching aCOLI types to tagged-key encoding, the two types
  became byte-identical (same domain tags, same trailing `idx_id=1`, same
  `{o_orderdate, o_shippriority}` payload post-G8). Collapsed via
  `using orders_acoli_t = orders_coli_t`. `pre_revenue` field was already
  **removed 2026-05-03** — it was parameterised by `l_shipdate`.
- `lineitem_acoli_t` (id=53): projected lineitem payload keyed by
  `(custkey, orderkey, linenumber)`. Revenue computed at query time via
  `LineitemRevenueAccumulator`, applying the shipdate filter live.

**Scan cardinality at SF=1**: 150 customers + 202 orders + 392 lineitems =
744 records vs COLI MI's ~1547 records visited — S5 skips all invoice rows.
`query_by_aggregated` uses `LineitemRevenueAccumulator` (shared with S1/S3)
for revenue; `pre_open_due` is read directly with no invoice pass.

**`populate_aggregated()` algorithm** (two passes — Pass B removed):

- Pass A: invoice scan → `unordered_map<custkey, open_due>` with
  `i_status='O'` fused.
- Pass C: customer + orders + lineitem scan → insert all three record types.
  Lineitem `custkey` resolved via orderkey→custkey map built during orders
  sub-pass. No shipdate filter at load time (predicate hoisting).

**Paper angle (REVIEWS.md §1.1, R2-D1):** S5 demonstrates that merged indexes
can store not just raw records but pre-aggregated values as included columns.
The spectrum is:

```
S1/S3 (raw co-location, full recompute each query)
  → S5 (aCOLI: invoice pre-aggregated to pre_open_due; lineitems unaggregated;
         reusable across all SEGMENT/DATE/THRESHOLD param sets)
    → S2 (fully pre-computed per-lineitem view; no accumulator at query time)
```

S5 avoids invoice rows at query time while remaining reusable across all
param sets — addressing the reviewer's request for a "convincing application
example" of multi-table merged indexes beyond raw co-location.

**Exit criterion satisfied**: all five paths produce identical digest at SF=1
(6 rows), exit 0. `[SKIP S5]` parity guard removed — S5 now participates in
the same XOR parity check as S1–S4.
`acoli_total=744 (c=150 o=202 l=392)` vs COLI MI ~1547 records visited.

---

Q5I and Q10I will follow the same 2-phase pattern (combined paths/top-10
harness, then production targets) once Q3I Phase 2 lands. The accumulator
factoring in Phase 1 is the template: each new query should land its
namespace-scope accumulators alongside the merged-path body so the
baseline-path bodies can compose them unchanged.
