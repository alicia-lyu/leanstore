# Q12: Shipping Modes and Order Priority Query

## TPC-H Definition (Section 2.4.12)

```sql
SELECT l_shipmode,
       SUM(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                THEN 1 ELSE 0 END) AS high_line_count,
       SUM(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                THEN 1 ELSE 0 END) AS low_line_count
FROM orders, lineitem
WHERE o_orderkey = l_orderkey
  AND l_shipmode IN ('[SHIPMODE1]', '[SHIPMODE2]')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '[DATE]'
  AND l_receiptdate < DATE '[DATE]' + INTERVAL '1' YEAR
GROUP BY l_shipmode
ORDER BY l_shipmode;
```

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| SHIPMODE1 | REG AIR, AIR, RAIL, SHIP, TRUCK, MAIL, FOB | First ship mode filter |
| SHIPMODE2 | Same list, must differ from SHIPMODE1 | Second ship mode filter |
| DATE | January 1 of a year in [1993, 1997] | Start of 1-year receipt date window |

**Validation values**: SHIPMODE1 = MAIL, SHIPMODE2 = SHIP, DATE = 1994-01-01.

**Approved query variants**: Variant A (Appendix B, approved 1998-02-11) replaces CASE with DECODE syntax. Same semantics.

**Selectivity notes**: Two ship modes cover ~2/7 (~28.6%) of lineitems. The 1-year date window covers ~1/7 of the data. Combined with the `l_shipdate < l_commitdate < l_receiptdate` ordering constraint, expect ~4% of lineitems to pass all filters. For selectivity sweeps (Reviewer 3 W2/D5), vary DATE across years and/or widen the receipt date window.

---

## Motivation (Next Paper Scope)

> **Note**: The rest of this document describes the multi-MI pipeline cascade approach for Q12, which is **next paper scope**. For the current paper, Q12 uses a single MI(ORDERS, LINEITEM) as described in `TPCH_experiments.md` and the `plans/` directory.

## Original Motivation

Q12 is the **proof-of-concept** for the Calcite↔LeanStore integration: manually translating Calcite's optimizer-generated plans into C++ before building a general plan interpreter. It validates that:

1. Calcite's merged-index substitution rules produce executable plans
2. The pipeline/merged-index architecture maps cleanly onto LeanStore's adapter/scanner templates
3. Predicate hoisting (filters at query time, not baked into MIs) works end-to-end
4. Incremental view maintenance via delta processing is feasible

Q12 is chosen because it's the simplest interesting case: **2 tables, 1 join, 1 filter, 1 aggregate** — enough to exercise the full pipeline without the complexity of multi-way joins (Q9) or ORDER BY + LIMIT (Q3).

## Calcite Plan Architecture

Three Calcite-generated plans in `calcite-integration-info/test-plans/q12/` prescribe a two-pipeline design:

```
Pipeline 0 (leaf):
  MI[0] = MergedIndex(ORDERS, LINEITEM)    ← trivial: just interleave by orderkey

  Index creation for root MI:
    Scan MI[0] → PremergedJoin → Project → root pipeline MI

  Maintenance (for root MI): The maintenance of leaf MI is trivial.
    (ORDERS_snapshot × LINEITEM_delta) ∪ (ORDERS_delta × LINEITEM_snapshot)
    → Project → insert/delete on root MI

Pipeline 1 (root):
  Root pipeline MI = single-type index of projected join results

  Query:
    Scan root MI → Filter (hoisted) → SortedAggregate
```

This is not a typo: It is the child pipeline that defines the index creation plan and the maintenance plan of the parent merged index!

**Predicate hoisting**: After Calcite's `hoistFiltersAboveBoundaries()` pass, all Q12 filter predicates (shipmode, date comparisons) live in the root query plan only. The root pipeline MI stores **unfiltered** join+project results with date columns preserved. This makes the MI reusable across queries with different date ranges or shipmode selections.

**Key simplification vs geo benchmark**: The geo benchmark has a 5-table hierarchical join with a 5-component sort key (`sort_key_t`). Q12 has a 2-table 1:N join with a single `Integer` join key (orderkey). No intermediate join views needed — PremergedJoin directly produces the final result.

## Relationship to Existing Infrastructure

| Geo Benchmark | Q12 | Difference |
|---------------|-----|------------|
| 5 record types in merged index | 2 record types (orders_t, lineitem_t) | Simpler variant dispatch |
| `sort_key_t` with 5 components | `Integer` orderkey | Simpler SKBuilder / key matching |
| Cascading 4× BinaryMergeJoin | Single BinaryMergeJoin or PremergedJoin | No intermediate joins |
| No filter in query path | Q12 filter (5 conditions) at query time | New: filter after join/scan |
| `view_t` = full 5-way join record | `q12_result_t` = projected 6-field record | New: projection with computed fields |
| `GeoJoin` workload class | `Q12Workload` class | Same pattern, fewer adapters |
| 4 storage structures | 5 storage structures (merged index structure has 2 variants 4 & 5) | Extra merged index variant: cascading MIs |

## Storage Structure Options

| # | Strategy | Load | Query | Maintain |
|---|----------|------|-------|----------|
| 1 | Trad indexes + hash join | Base tables only | Full scan + hash join + filter + aggregate | Base tables only |
| 2 | Trad indexes + merge join | Base tables only + secondary indexes as needed | Merge join + filter + aggregate | Base tables only + loaded secondary indexes |
| 3 | Materialized view | Base tables + final unfiltered view | Direct lookup | Recompute affected view rows |
| 4 | MI[0] only | Base tables + MI[0] | PremergedJoin + filter + aggregate | MI[0] mirrors base writes |

> Structure 5 (MI[0] + root pipeline MI) is **next-paper scope**. See
> `project_current_vs_next_paper.md` in agent memory.

## Verified: Calcite Column Index Mapping

Concatenated ORDERS ∥ LINEITEM schema matches `tpch_tables.hpp` field order:

| Index | Field | Type | Source |
|-------|-------|------|--------|
| $0 | o_orderkey | Integer | orders_t key |
| $1 | o_custkey | Integer | orders_t |
| $2 | o_orderstatus | Varchar<1> | orders_t |
| $3 | o_totalprice | Numeric | orders_t |
| $4 | o_orderdate | Timestamp | orders_t |
| $5 | o_orderpriority | Varchar<15> | orders_t |
| $6 | o_clerk | Varchar<15> | orders_t |
| $7 | o_shippriority | Integer | orders_t |
| $8 | o_comment | Varchar<79> | orders_t |
| $9 | l_orderkey | Integer | lineitem_t key |
| $10 | l_linenumber | Integer | lineitem_t key |
| $11 | l_partkey | Integer | lineitem_t |
| $12 | l_suppkey | Integer | lineitem_t |
| $13 | l_quantity | Numeric | lineitem_t |
| $14 | l_extendedprice | Numeric | lineitem_t |
| $15 | l_discount | Numeric | lineitem_t |
| $16 | l_tax | Numeric | lineitem_t |
| $17 | l_returnflag | Varchar<1> | lineitem_t |
| $18 | l_linestatus | Varchar<1> | lineitem_t |
| $19 | l_shipdate | Timestamp | lineitem_t |
| $20 | l_commitdate | Timestamp | lineitem_t |
| $21 | l_receiptdate | Timestamp | lineitem_t |
| $22 | l_shipinstruct | Varchar<25> | lineitem_t |
| $23 | l_shipmode | Varchar<10> | lineitem_t |
| $24 | l_comment | Varchar<44> | lineitem_t |

---

## New Record Types

Because of the way this codebase is organized (strong typed language), the output of every operation in all plans (queries / index creation / maintenance) must have a row type defined. These include:

- `q12_result_t` (shown below)
- any secondary index required (different key, largely the same payload)
- any row type in a merged index. This can just utilize (`merged_t`)[../../shared/view_templates.hpp].
- any join result: This can simply utilize (`joined_t`)[../../shared/view_templates.hpp].

This overhead might be too significant once we move forward to queries other than q12. We might want to have these definitions automated by MACROs or some other techniques.

### `q12_result_t`

Root pipeline MI entry — the projected join output schema (next-paper scope). One record per (order, lineitem) join result, **unfiltered** (predicate hoisting).

```cpp
struct q12_result_t {
    static constexpr int id = 30; // unique, not colliding with geo IDs (max 26)
    struct Key {
        Varchar<10> l_shipmode;   // $0 — GROUP BY key, first for sorted aggregate
        Integer o_orderkey;       // unique identifier part 1
        Integer l_linenumber;     // unique identifier part 2
        ADD_KEY_TRAITS(l_shipmode, o_orderkey, l_linenumber)
    };
    // Projection fields
    Integer high_line_count;      // $1 — CASE(o_orderpriority IN ('1-URGENT','2-HIGH'), 1, 0)
    Integer low_line_count;       // $2 — CASE(o_orderpriority NOT IN ('1-URGENT','2-HIGH'), 1, 0)
    Timestamp l_shipdate;         // $3 — preserved for query-time filter
    Timestamp l_commitdate;       // $4 — preserved for query-time filter
    Timestamp l_receiptdate;      // $5 — preserved for query-time filter
    ADD_RECORD_TRAITS(high_line_count, low_line_count, l_shipdate, l_commitdate, l_receiptdate)
};
```

**Key design**: Shipmode first in key → natural sort for `SortedAggregate(GROUP BY L_SHIPMODE)`. Orderkey + linenumber ensure uniqueness.

---

## Operator Translation

> **Superseded**: The iterator/cascade operator framework originally described
> here has been replaced by the **monolithic post-join** execution style.
> See `frontend/tpch/OPERATORS.md` for the authoritative reference on how
> Calcite operators translate to C++ across all three Tier 1 queries.

## Q12-Specific Operator Configurations

These lambdas are inlined in the monolithic post-join callback. See OPERATORS.md §5 Q12 for the worked example.

### `q12_predicate`

Inlined in callback. Encodes the five Q12 filter conditions:

```cpp
// Maps to Calcite filter: $23 IN ('MAIL','SHIP') AND $19<$20 AND $20<$21
//   AND $21 >= DATE '1994-01-01' AND $21 < DATE '1995-01-01'
// Applied to q12_result_t fields (stored in root pipeline MI)
auto q12_predicate = [](const q12_result_t& r) -> bool {
    auto sm = std::string_view(r.key.l_shipmode.data, r.key.l_shipmode.length);
    return (sm == "MAIL" || sm == "SHIP")
        && r.l_shipdate < r.l_commitdate
        && r.l_commitdate < r.l_receiptdate
        && r.l_receiptdate >= DATE_1994_01_01
        && r.l_receiptdate < DATE_1995_01_01;
};
```

For options 1-2 (traditional indexes), the equivalent predicate operates on raw `lineitem_t` fields before joining:

```cpp
auto q12_predicate_lineitem = [](const lineitem_t& l) -> bool { /* same conditions */ };
```

### `q12_projection`

Inlined in callback. Implements the projection (only needed for structure 5, next-paper scope):

```cpp
// joined_ol_t carries (orders_t::Key, orders_t, lineitem_t::Key, lineitem_t)
auto q12_projection = [](const joined_ol_t& j) -> q12_result_t {
    auto prio = std::string_view(j.order.o_orderpriority.data, ...);
    return q12_result_t {
        .key = { j.lineitem_key.l_shipmode,      // $23
                 j.order_key.o_orderkey,          // $0
                 j.lineitem_key.l_linenumber },   // $10
        .high_line_count = (prio == "1-URGENT" || prio == "2-HIGH") ? 1 : 0,
        .low_line_count  = (prio != "1-URGENT" && prio != "2-HIGH") ? 1 : 0,
        .l_shipdate      = j.lineitem.l_shipdate,          // $19
        .l_commitdate    = j.lineitem.l_commitdate,        // $20
        .l_receiptdate   = j.lineitem.l_receiptdate        // $21
    };
};
```

### `q12_accumulator`

Inlined in callback. Groups by shipmode, sums `high_line_count` and `low_line_count`:

```cpp
// q12_agg_row_t: { Varchar<10> l_shipmode, Integer high_line_count, Integer low_line_count }
auto shipmode_key = [](const q12_result_t& r) { return r.key.l_shipmode; };

auto count_accum = Accumulator<q12_result_t, q12_agg_row_t> {
    .init     = [](const q12_result_t& r) { return q12_agg_row_t{r.key.l_shipmode, 0, 0}; },
    .fold     = [](q12_agg_row_t& acc, const q12_result_t& r) {
                    acc.high_line_count += r.high_line_count;
                    acc.low_line_count  += r.low_line_count;
                },
    .finalize = [](q12_agg_row_t& acc) { /* no-op */ }
};
```

---

## Q12 Plans as Operator Trees

> **Superseded**: The iterator-tree plan compositions below have been replaced
> by monolithic post-join execution. See `OPERATORS.md §5 Q12` for the
> current worked example.

## Stages × Options

Loading is dispatched by `Q12Workload::load()` in `load.tpp`. See
`OPERATORS.md §4` for why `populate_view` differs per query.

### Stage 1: Loading

| Structure | What gets populated | How |
|-----------|--------------------|----|
| 1, 4 | Base tables only | `TPCHWorkload::load()` — standard per-table insert |
| 2 | Base tables + pipeline view | `ol.populate_view(pipeline_view)` — merge-join base tables, insert `joined_ol_t` rows |
| 3 | Base tables + MI[0] | `ol.populate_merged()` — dual-write replay of orders and lineitem |

orders_t key = `{o_orderkey}` (4 bytes folded), lineitem_t key = `{l_orderkey, l_linenumber}` (8 bytes folded). Natural interleaving: for each orderkey, the order record sorts before its lineitems (shorter key prefix). `toType()` discriminates by fold-length (4 vs 8).

### Stage 2: Queries

See `OPERATORS.md §5 Q12` for the monolithic post-join execution sketch.
All four structures (S1–S4) share the same post-join logic: filter by 5
conditions, accumulate into 2-slot shipmode counters, sort by shipmode.

### Stage 3: Maintenance

> Deferred: maintenance paths (RF1/RF2) are out of scope for the current
> paper. See §Motivation for next-paper context.

---

## Data Generation Enhancements

### Date Encoding

Current `lineitem_t::generateRandomRecord` uses `urand(1, 10000)`. For Q12 filter to be meaningful:

```cpp
// Days since 1970-01-01
constexpr int DATE_1992_01_01 = 8035;
constexpr int DATE_1998_12_31 = 10591;
constexpr int DATE_1994_01_01 = 8766;
constexpr int DATE_1995_01_01 = 9131;

// Generate realistic dates:
l_shipdate    = urand(DATE_1992_01_01, DATE_1998_12_31);
l_commitdate  = l_shipdate + urand(1, 90);    // commit after ship
l_receiptdate = l_commitdate + urand(1, 30);   // receipt after commit
```

This ensures `l_shipdate < l_commitdate < l_receiptdate` always holds, and ~14% of lineitems have `l_receiptdate` in the [1994-01-01, 1995-01-01) range.

### Order Priority

Current `orders_t::generateRandomRecord` uses random strings. Need:

```cpp
static constexpr std::array<const char*, 5> PRIORITIES = {
    "1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"
};
o_orderpriority = PRIORITIES[urand(0, 4)];
```

### Shipmode

Current `lineitem_t::generateRandomRecord` uses random strings. Need:

```cpp
static constexpr std::array<const char*, 7> SHIPMODES = {
    "REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"
};
l_shipmode = SHIPMODES[urand(0, 6)];
```

~28% of lineitems will have MAIL or SHIP. Combined with date filter (~14%), expect ~4% of lineitems to pass Q12 filter.

---

## File Structure

Shared infrastructure (already exists):

- `frontend/shared/merge-join/` — PremergedJoin, BinaryMergeJoin, HashJoin
- `frontend/shared/adapter-scanner/` — LeanStore and RocksDB adapters/scanners

Q12-specific (current layout):

- `frontend/tpch/q12/views.hpp` — row-shape types only: `q12_pipeline_view_t`
  alias and `q12_agg_row_t` with key and payload. `Params` and predicates have
  moved to `workload.hpp`.
- `frontend/tpch/q12/workload.hpp` — `Q12Workload` template class; `Params`
  struct with `defaults()`, `q12_predicate_lineitem`, `q12_predicate_joined`
  declarations; private `orders`/`lineitem` reference members.
- `frontend/tpch/q12/per_structure_workload.hpp` — alias-only:
  `using BaseQ12 = ::tpch::BaseStructure<Q12Workload<Backend>, q12_agg_row_t>`
  and siblings. Forwarder bodies live in `frontend/tpch/per_structure_workload.hpp`.
- `frontend/tpch/q12/load.tpp` — fully implemented: ctor, `load()`, `get_size()`,
  and free function `populate_q12_view` (two-pointer manual merge, not
  `BinaryMergeJoin`).
- `frontend/tpch/q12/query.tpp` — `Params::defaults()` and
  `q12_agg_row_t::print()` implemented; `query_by_*` bodies are TODO.
- `frontend/tpch/q12/executable_rocksdb.cpp` / `executable_leanstore.cpp`
- `frontend/tpch/q12/test_load_view_stats.hpp` — `ViewStats<Backend>`,
  `dump_view_stats<Backend>`, `compare_mi_and_view` (cross-check with
  [OK]/[FAIL] tags).
- `frontend/tpch/q12/test_load_q12_rocksdb.cpp` /
  `frontend/tpch/q12/test_load_q12_leanstore.cpp` — standalone load-test
  executables that build MI[0] + pipeline view and cross-check cardinality
  and orderkey ranges.

## CMake Targets

Already added to `frontend/CMakeLists.txt`:

- `test_load_q12_lsm` — macOS + Linux (RocksDB). Loads MI[0] and pipeline
  view, cross-checks. All [OK] at SF=1.
- `test_load_q12_btree` — Linux only (LeanStore). Same checks.

Still to add:

```cmake
# macOS (ROCKSDB_ONLY) section:
add_executable(q12_lsm tpch/q12/executable_rocksdb.cpp)
target_link_libraries(q12_lsm RocksDB RocksDBLogger ...)
target_compile_definitions(q12_lsm PRIVATE ROCKSDB_ONLY)

# Linux section:
add_executable(q12_btree tpch/q12/executable_leanstore.cpp)
target_link_libraries(q12_btree leanstore LeanStoreLogger ...)

add_executable(q12_lsm tpch/q12/executable_rocksdb.cpp)
target_link_libraries(q12_lsm RocksDB RocksDBLogger ...)
```

Follow exact pattern from `geo_lsm` / `geo_btree` targets.

## Executable Structure

The executable instantiates adapters and dispatches on `FLAGS_storage_structure`.
See `executable_rocksdb.cpp` / `executable_leanstore.cpp` for the actual code
(structures 1–4 only; structure 5 is next-paper scope).

---

## Dependency on Tagged Row Format

The current plan uses fold-length discrimination (4 vs 8 bytes) for MI[0], which works for Q12. Tagged row format migration is tracked separately and is NOT a blocker for initial Q12 implementation. Can be retrofitted later.

---

## Execution Style: Monolithic vs Cascade

The operator framework above (§Operator Framework, §Q12 Plans as Operator Trees) follows cascade/iterator style. An alternative **monolithic** style fuses post-join operators into single functions.

### Record Types Required (Either Style)

The merge-join infrastructure (`frontend/shared/merge-join/`) requires typed join results. `JoinState`, `BinaryMergeJoin`, `HashJoin`, and `PremergedJoin` are all templated on a `JR` type that must provide `JR::Key(Rs::Key...)` and `JR(Rs...)` constructors. The generic `joined_t<TID, JK, fold_pks, Ts...>` from `frontend/shared/view_templates.hpp` satisfies this.

**Required types for Q12:**

- **Base tables**: `orders_t`, `lineitem_t` — in `tpch_tables.hpp`
- **Join result**: `joined_t<TID, Integer, false, orders_t, lineitem_t>` — required by all join operators (PremergedJoin, BinaryMergeJoin, HashJoin)
- **MI records**: Same `orders_t`, `lineitem_t` in `MI_ORDERS_LINEITEM` (structure 3)
- **Intermediate pipeline view** (structure 2): Stores ORDERS ⋈ LINEITEM join output — can reuse `joined_t` or define a view-specific type with a key suited to downstream access

### What "Monolithic" Means in Practice

The join result type (`joined_t`) is unavoidable. But everything *after* the join — filter, project, aggregate — can be fused into a single function rather than composed as separate iterator stages:

```
// Structure 3 sketch (MI PremergedJoin, monolithic post-join)
void q12_query_structure3(MergedAdapter& mi) {
    Integer high_mail = 0, low_mail = 0, high_ship = 0, low_ship = 0;

    premerged_join(mi, [&](const joined_t& row) {
        auto& o = std::get<orders_t>(row.payloads);
        auto& l = std::get<lineitem_t>(row.payloads);
        if (l.l_shipdate >= l.l_commitdate) return;
        if (l.l_commitdate >= l.l_receiptdate) return;
        if (l.l_receiptdate < DATE_1994 || l.l_receiptdate >= DATE_1995) return;
        auto sm = to_sv(l.l_shipmode);
        if (sm != "MAIL" && sm != "SHIP") return;
        auto prio = to_sv(o.o_orderpriority);
        bool is_high = (prio == "1-URGENT" || prio == "2-HIGH");
        if (sm == "MAIL") { (is_high ? high_mail : low_mail)++; }
        else              { (is_high ? high_ship : low_ship)++; }
    });
    // Output: MAIL high_mail low_mail, SHIP high_ship low_ship
}
```

### Feasibility Assessment

**Q12 is the ideal case for monolithic post-join execution** because:

1. Only 2 tables, 1 join — the callback body is ~15 lines
2. Aggregation by shipmode has tiny cardinality (2-7 groups) — local counters, no sort
3. All 5 filter predicates can be checked inline with zero overhead

**Trade-offs vs cascade (§Operator Framework)**:

- **Pro**: No per-query types beyond the generic `joined_t` instantiation — no `q12_agg_row_t`
- **Pro**: ~30 lines of code vs ~100+ lines of operator tree composition
- **Pro**: No virtual dispatch overhead from iterator `next()` calls
- **Con**: Maintenance plans (§Stage 3) are harder to express monolithically when they involve delta joins — but for the current paper scope (no maintenance experiments), this is not a blocker

### Recommendation

For the current paper (single MI per query, no maintenance), use monolithic style for all query-time post-join execution. The join itself must use the typed infrastructure (`PremergedJoin`, `BinaryMergeJoin`, `HashJoin` with `joined_t`). Reserve full cascade/iterator composition for future work where operator reuse across plans becomes necessary.

---

## Open Questions

1. ~~RF2 delete strategy~~ — deferred (no maintenance experiments this paper).
2. ~~Plans for options 1-2~~ — resolved: S4 uses baseline_s4.dot, S1/2/3 share family_logical.dot. See OPERATORS.md §1.
3. ~~Spectrum between options 4 and 5~~ — resolved: current paper uses 1 MI per query (structures 1-4 only). Structure 5 is next-paper scope.
4. ~~Operator framework scope~~ — resolved: monolithic post-join, no shared operator framework. See OPERATORS.md §3.

---

## Tests

Tests owned by this directory. The tpch-level index is in
[`../CLAUDE.md §Tests`](../CLAUDE.md#tests).

| Test | Backend | Source |
|------|---------|--------|
| `test_load_q12_lsm` | RocksDB (mac+Linux) | `../tests/q12/test_load_q12_rocksdb.cpp` |
| `test_load_q12_btree` | LeanStore (Linux only) | `../tests/q12/test_load_q12_leanstore.cpp` |
| `test_query_q12_lsm` | RocksDB (mac+Linux) | `../tests/q12/test_query_q12_rocksdb.cpp` |
| `test_query_q12_btree` | LeanStore (Linux only) | `../tests/q12/test_query_q12_leanstore.cpp` |

Both binaries build MI[0] + the pipeline view, then cross-check that
both structures agree on row count and orderkey range.

### Commands

```bash
# Build (macOS)
make -C build/frontend test_load_q12_lsm test_query_q12_lsm \
    -j$(sysctl -n hw.ncpu)

# Build (Linux adds the leanstore variants)
make -C build/frontend test_load_q12_btree test_query_q12_btree -j$(nproc)

# Run load test
mkdir -p test_data2 test_csv2
./build/frontend/test_load_q12_lsm \
    --ssd_path=./test_data2 \
    --csv_path=./test_csv2 \
    --tpch_scale_factor=1

# Run query test (cross-structure parity over all four query_by_*)
mkdir -p test_data3 test_csv3
./build/frontend/test_query_q12_lsm \
    --ssd_path=./test_data3 \
    --csv_path=./test_csv3 \
    --tpch_scale_factor=1
# Expected: per-structure shape checks + 4-way XOR parity = [OK].
# All shape and parity checks [OK] at SF=1 once the load-order bug
# was fixed (orders now precede lineitems so `o_orderdate` is threaded
# into lineitem date generation — see tpch_workload.hpp::load).
# Expected:
#   - MI[0] distribution stats block (all [OK])
#   - Pipeline view stats block (rows, orderkey range, size MiB)
#   - Cross-check block: [OK] view rows == MI lineitem rows,
#     [OK] orderkey ranges agree,
#     [OK] view size in plausible range vs MI (0.3x-5x).
# Any [FAIL] indicates a loading bug.
```

**Note**: use distinct directories from the tpch-level test
(`test_data2` / `test_csv2` instead of `test_data` / `test_csv`) to
avoid clobbering the MI-only load test's data. See `../CLAUDE.md §Tests`
for the rationale on `--ssd_path` vs `--csv_path` separation.

---

## Implementation Status

**Shared infrastructure completed** (2026-04-29):

- `views_ol.hpp`: fully implemented — `ol_sort_key_t` (2-component sort key:
  orderkey + linenumber), `joined_ol_t` (derived struct with explicit
  `unfoldKey` override), `SKBuilder<ol_sort_key_t>` (create, project, to_key).
  Unit-tested (12 tests in `test_views_ol.cpp`).
- Shared merge-join infra: `PremergedJoin` decoupled from record types via
  `jk_from_variants` and `SKBuilder::to_key<R>`.
- `OrdersLineitemPipeline` trimmed (2026-04-29): only `populate_merged` and
  `get_merged_size` remain.

**Q12 load fully implemented** (2026-04-30):

- `Q12Workload<Backend>::Q12Workload(...)` — wires gflags into `params`. Done.
- `Q12Workload<Backend>::load()` — dispatches to `populate_q12_view` /
  `ol.populate_merged`. Done. No TODO debt.
- `Q12Workload<Backend>::get_size() const` — dispatches to view size /
  `ol.get_merged_size`. Done.
- `populate_q12_view` — two-pointer manual merge over base scanners (not
  `BinaryMergeJoin`); emits one `joined_ol_t` row per lineitem. Done.
- `Params::defaults()` — validation parameters (MAIL/SHIP, DATE_1994). Done.
- `q12_agg_row_t::print()` — formatted output. Done.
- `test_load_q12_lsm` / `test_load_q12_btree` load-test binaries pass all
  [OK] cross-checks at SF=1, including the view-size sanity bound
  (0.3×–5× MI[0] size). View reports 0.28 MiB at SF=1.
- **View size reporting fixed** (2026-04-30): `RocksDB.hpp::get_size<Record>()`
  now uses `SizeApproximationOptions` with `include_memtables=true` to fix
  spurious 0.00 MiB readings for sub-CF prefix ranges.

**Q12 query bodies completed** (2026-04-30):

- `query_by_base` — `BinaryMergeJoin` over base scanners with
  `q12_predicate_lineitem` pushed into the lineitem fetch lambda.
- `query_by_view` — scan `pipeline_view` and apply `q12_predicate_joined`
  post-scan (predicate hoisting per `OPERATORS.md §4` Q12 bullet).
- `query_by_merged` — `PremergedJoin` over MI[0] with post-join filter via
  `q12_predicate_joined`. Required a fix to `PremergedJoin` itself: the
  tentative-skip path now uses `jk_from_variants<JK>` instead of calling
  `SKBuilder<JK>::create` directly on variant operands.
- `query_by_hash` — `HashJoin` with the same filter-pushdown lineitem
  fetcher as S1.
- `q12_predicate_lineitem`, `q12_predicate_joined` — implemented;
  `q12_predicate_joined` delegates to the lineitem variant via
  `j.line()` to keep S1–S4 semantically identical (defends
  `OPERATORS.md §6.1`).

**Cross-structure parity test added** (2026-04-30):

- `test_query_q12_lsm` / `test_query_q12_btree` build all four query paths
  in one process, run them against a single load (MI[0] + pipeline view
  populated together), and check both:
  - **shape**: 2 rows per structure, shipmodes equal `params.shipmode1` /
    `shipmode2` after sort, `high + low > 0` for both rows.
  - **XOR parity**: per-row digest of `(l_shipmode bytes, high_line_count,
    low_line_count)` mixed with `rotl(acc, 13) ^ field`, then XOR-folded
    across rows. All four structures must produce the same digest.
- Sources: `tests/q12/test_query_q12_rocksdb.cpp`,
  `tests/q12/test_query_q12_leanstore.cpp`,
  `tests/q12/test_query_q12_checks.hpp`.
- Status at SF=1: all checks `[OK]` across all four structures —
  shape (row count, expected shipmodes, `high+low > 0`) and parity
  (identical XOR digest, e.g. `0x9000007000e03e` on a sample run).
  ~25–30 lineitems satisfy the Q12 predicate at SF=1 after the
  data-generation load-order fix described below.

### XOR parity scope (limitation)

The XOR digest is computed over the **aggregate result rows**, not over
join-time data. Concretely, the digest mixes only:

- `l_shipmode` (length-prefixed bytes)
- `high_line_count` (4-byte little-endian)
- `low_line_count` (4-byte little-endian)

It is **not** computed over orderkeys, lineitem keys, or any pre-aggregate
field. Two consequences:

1. The check verifies the four structures agree on the final per-shipmode
   counts. It does *not* verify they joined the same set of orderkeys —
   any bug that preserves the count totals (e.g., off-by-one missing one
   high and one low at the same shipmode) would still pass.
2. If the data ever degenerates to zero matches, the digest collapses
   to the fixed value `xor((MAIL,0,0), (SHIP,0,0)) = 0x9000007000003c`
   — a passing parity check carries no information in that case. SF=1
   currently produces ~25–30 matches so the digest is non-degenerate,
   but the test would still need a non-zero shape check to catch this
   regression.

For now this is acceptable: the predicate is identical across S1–S4 by
construction (`q12_predicate_joined` delegates to the lineitem version),
so a count-only digest is sufficient to detect divergence in the join /
filter paths. A finer-grained digest (folding orderkeys before
aggregation) is a future hardening step.

### Resolved: data-generation load order (2026-04-30)

The original "zero matches" symptom turned out not to be a selectivity
problem — the date generator is spec-compliant. The bug was in
`TPCHWorkload::load()`: `loadPartsuppLineitem()` ran before
`loadOrders()`, so the `order_dates` map was empty when lineitems
were generated, and `o_orderdate` defaulted to 0. Lineitem dates were
in `[2, 151]` instead of `[orderdate+2, orderdate+151]`, so no
receiptdate ever landed in `[DATE_1994_01_01, DATE_1995_01_01)`.

Fix: reordered `load()` to `loadCustomer → loadOrders →
loadPartsuppLineitem` (customer must precede orders because
`generate_custkey_for_orders` reads `last_customer_id`; orders must
precede lineitems for `order_dates`). Phase 0 diagnostic counts before
and after the reorder confirmed receiptdate range jumped from
`[2, 151]` to `[8063, 10569]` and the predicate-conjunction match
count jumped from 0 to ~25–30. Per-shipmode counts are now realistic
(e.g. `MAIL 2/8`, `SHIP 5/10`).

The earlier proposal to "chain dates by construction" was rejected
because it would diverge from TPC-H §4.2.3 and contaminate Q3/Q9
selectivity stats too. Generator field-fidelity (placeholder
`o_totalprice`, `l_extendedprice`) remains a separate concern,
tracked in `.claude/plans/data-generator-evolution.md`.

`BaseQ12` / `ViewQ12` / `MergedQ12` / `HashQ12` forwarder bodies live in the
shared `frontend/tpch/per_structure_workload.hpp`; the per-query file is
alias-only.

Cross-cutting status: see `frontend/tpch/CLAUDE.md §What's Needed`.
