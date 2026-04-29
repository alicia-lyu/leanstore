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

- `frontend/tpch/q12/views.hpp` — Params, q12_pipeline_view_t, q12_agg_row_t, predicates
- `frontend/tpch/q12/workload.hpp` — Q12Workload template class
- `frontend/tpch/q12/per_structure_workload.hpp` — BaseQ12/ViewQ12/MergedQ12/HashQ12
- `frontend/tpch/q12/load.tpp` / `query.tpp` — method bodies (TODO stubs)
- `frontend/tpch/q12/executable_rocksdb.cpp` / `executable_leanstore.cpp`

## CMake Targets

Add to `frontend/CMakeLists.txt`:

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

## Implementation Status

**Shared infrastructure completed** (2026-04-29):

- `views_ol.hpp`: fully implemented — `ol_sort_key_t` (2-component sort key:
  orderkey + linenumber), `joined_ol_t`, `SKBuilder<ol_sort_key_t>` (create,
  project, to_key). Unit-tested (12 tests in `test_views_ol.cpp`).
- Shared merge-join infra: `PremergedJoin` decoupled from record types via
  `jk_from_variants` and `SKBuilder::to_key<R>`.

**Q12-specific method bodies** remain TODO stubs (`load.tpp` + `query.tpp`).
Loading lives in `OrdersLineitemPipeline` per the Pipeline Convention
(`frontend/tpch/CLAUDE.md §Pipeline Convention`). `load()` and `get_size()`
are one-line dispatchers.

Stubbed methods:

- `Q12Workload<Backend>::Q12Workload(...)` — wire gflags into `params`.
- `Q12Workload<Backend>::load()` — dispatches to `ol.populate_view` /
  `ol.populate_merged`; base-table size case is a TODO pending
  `TPCHWorkload::get_size()`.
- `Q12Workload<Backend>::get_size() const` — dispatches to
  `ol.get_view_size` / `ol.get_merged_size`.
- `Q12Workload<Backend>::query_by_base(out)` — see §Stages × Options, Option 2.
- `Q12Workload<Backend>::query_by_view(out)` — see §Stages × Options, Option 3.
- `Q12Workload<Backend>::query_by_merged(out)` — see §Stages × Options, Option 4
  / §Execution Style: monolithic post-join.
- `Q12Workload<Backend>::query_by_hash(out)` — see §Stages × Options, Option 1.
- `BaseQ12<Backend>::query / get_size` and the View/Merged/Hash siblings —
  forwarders to the right `Q12Workload` method.
- `q12_predicate_lineitem`, `q12_predicate_joined` — see §Q12-Specific
  Operator Configurations, `q12_predicate`.

Cross-cutting TODOs (`OrdersLineitemPipeline` method bodies, build wiring):
see `frontend/tpch/CLAUDE.md`.
