# Q3I: Shipping Priority × Customer Outstanding Balance (Invoice-Extended Q3)

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

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + merge join | None | Pre-build cust\_open\_due map from INVOICE; BinaryMergeJoin(OL) + CUSTOMER hash lookup |
| 2 | Intermediate pipeline view | `q3i_pipeline_view_t` (joined\_ol\_t rows) | Pre-build cust\_open\_due map; view scan + CUSTOMER hash filter |
| 3 | MI\[COLI\] only | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | PremergedJoin over 4-table tagged-key MI; cust\_open\_due computed in same pass |
| 4 | Traditional indexes + hash join | None | Pre-build cust\_open\_due map; HashJoin(OL) + CUSTOMER hash lookup |

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

All four agree on what's outside the pipeline: `apply_top10` (sort by
revenue DESC + truncate to 10).

---

## Required Record Types

- `q3i_pipeline_view_t` — currently aliased to `joined_ol_t`; widen to include
  `cust_open_due` aggregate field once the COLI MI driver is implemented.
- `q3i_agg_row_t` — final output row: `o_orderkey`, `revenue`, `o_orderdate`,
  `o_shippriority`, `cust_open_due`.

---

## File Structure

| File | Status |
|------|--------|
| `views.hpp` | Skeleton — `q3i_pipeline_view_t` alias and `q3i_agg_row_t` declared |
| `workload.hpp` | Skeleton — `Q3IWorkload<Backend>` declared, COLI pipeline member |
| `per_structure_workload.hpp` | Skeleton — alias-only |
| `load.tpp` | Skeleton — ctor wires refs; `load()` / `get_size()` bodies are TODO stubs |
| `query.tpp` | Skeleton — all `query_by_*`, predicates, `print()` are TODO |
| `executable_rocksdb.cpp` | Skeleton — `main()` returns 0 |
| `executable_leanstore.cpp` | Skeleton — `main()` returns 0 (`#ifndef ROCKSDB_ONLY`) |
| `CLAUDE.md` | This file |

---

## CMake Targets

TODO: add to `frontend/CMakeLists.txt` when body implementations land.

```cmake
# macOS (ROCKSDB_ONLY) section:
add_executable(q3i_lsm tpch/q3i/executable_rocksdb.cpp)
# ...

# Linux section:
add_executable(q3i_btree tpch/q3i/executable_leanstore.cpp)
add_executable(q3i_lsm   tpch/q3i/executable_rocksdb.cpp)
```

---

## Implementation Phases

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

**Goal**: spec-compliant Q3I executable + a single `test_query_q3i_lsm`
harness that loads each storage structure in turn, runs `query_by_*`,
and asserts XOR parity across all four. Replaces the temporary Phase 1
binary entirely.

**Deliverables**:

1. `query.tpp` — three new bodies, each ending with a top-10 `partial_sort_copy`:
   - `query_by_base` (S1): scan `coli.split_invoice()` (custkey-sorted)
     into a `cust_open_due` hashmap via `CustomerOpenDueAccumulator` →
     `BinaryMergeJoin` over `coli.split_orders()` and
     `coli.split_lineitem()` (also custkey-sorted) → CUSTOMER hash
     lookup → `LineitemRevenueAccumulator` per orderkey → threshold
     filter on `cust_open_due`.
   - `query_by_view` (S2): same hashmap pre-pass; scan
     `q3i_pipeline_view_t` (still `joined_ol_t`) → CUSTOMER hash filter
     → revenue accumulation per orderkey.
   - `query_by_hash` (S4): same hashmap pre-pass; `HashJoin(OL)` over
     base ORDERS / LINEITEM + CUSTOMER hash lookup.
2. `query_by_merged` — refactor to also emit top-10 via the same
   `partial_sort_copy` epilogue, replacing Phase 1's "all qualifying
   rows" return.
3. `load.tpp` — extend the `load()` / `get_size()` switch:
   - S1: `coli.populate_split()` (already wired in Phase 1)
   - S2: `populate_q3i_view(pipeline_view, ...)` — two-pointer merge over
     base OL scanners, emit one `joined_ol_t` per lineitem
   - S4: base only (no extras)
4. `tests/q3i/test_query_q3i_rocksdb.cpp` — the unified harness:
   - Iterates `FLAGS_storage_structure` ∈ {1,2,3,4}
   - Wipes `--ssd_path` between iterations to defeat the stale-load trap
   - Runs `query_by_*`, computes a XOR digest over the result rows
   - Asserts equal digest across all four structures
   - Prints top-10 rows for visual inspection
5. CMake: add `test_query_q3i_lsm` target; **delete**
   `test_query_q3i_phase1_lsm` and move the Phase 1 binary's source to
   `TRASH/` per project rules.

**Exit criterion**: `test_query_q3i_lsm` at SF=1 prints top-10 rows
matching the spec column order and reports `[OK]` parity across S1/S2/S3/S4.

---

### Phase 3 — Production targets + experiment integration

**Goal**: standalone `q3i_lsm` / `q3i_btree` executables wired into the
existing experiment Makefile flow.

**Deliverables**:

- `frontend/CMakeLists.txt` — `q3i_lsm` (macOS + Linux) and `q3i_btree`
  (Linux only), mirroring `q12_lsm` / `q12_btree`.
- `generate_targets.py` — `q3i_lsm` / `q3i_btree` entries (`exec_names`,
  `STRUCTURE_OPTIONS`, `DIFF_DIRS`); regenerate `targets.mk`.
- `tests/test_load_q3i_rocksdb.cpp` (optional) — load S3 only and report
  COLI distribution stats. Skip if `test_load_coli_lsm` already covers
  the relevant invariants.
- `frontend/tpch/CLAUDE.md §Tests` and `§Layout` index entries.

**Exit criterion**: `make q3i_lsm scale=1` runs end-to-end across all four
structures and emits CSV metrics; `make q3i_lsm_3 dram=0.1` runs S3 in
isolation for memory-pressure experiments.

---

Q5I and Q10I will follow the same 2-phase pattern (combined paths/top-10
harness, then production targets) once Q3I Phase 2 lands. The accumulator
factoring in Phase 1 is the template: each new query should land its
namespace-scope accumulators alongside the merged-path body so the
baseline-path bodies can compose them unchanged.
