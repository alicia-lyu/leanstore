# Q12: First Calcite-Planned Query

## Motivation

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
| 5 | MI[0] + root MI | Base tables + MI[0] + root MI | Scan root MI + filter + aggregate | MI[0] + delta propagation to root MI |

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

Root pipeline MI entry. One record per (order, lineitem) join result — **unfiltered** (predicate hoisting).

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
    Integer high_line_count;      // $1 — CASE(o_orderpriority = '1-URGENT', 1, 0)
    Integer low_line_count;       // $2 — CASE(o_orderpriority <> '1-URGENT', 1, 0)
    Timestamp l_shipdate;         // $3 — preserved for query-time filter
    Timestamp l_commitdate;       // $4 — preserved for query-time filter
    Timestamp l_receiptdate;      // $5 — preserved for query-time filter
    ADD_RECORD_TRAITS(high_line_count, low_line_count, l_shipdate, l_commitdate, l_receiptdate)
};
```

**Key design**: Shipmode first in key → natural sort for `SortedAggregate(GROUP BY L_SHIPMODE)`. Orderkey + linenumber ensure uniqueness.

---

## Q12 Filter (query-time, hoisted from index creation)

```cpp
bool q12_filter(const q12_result_t& r) {
    // All conditions on root pipeline MI's projected schema ($0-$5)
    auto shipmode = std::string_view(r.key.l_shipmode.data, r.key.l_shipmode.length);
    return (shipmode == "MAIL" || shipmode == "SHIP")
        && r.l_shipdate < r.l_commitdate        // $3 < $4
        && r.l_commitdate < r.l_receiptdate      // $4 < $5
        && r.l_receiptdate >= DATE_1994_01_01     // $5 >= date
        && r.l_receiptdate < DATE_1995_01_01;     // $5 < date
}
```

For options 1-2 (traditional indexes), the filter applies directly on `lineitem_t` fields:

```cpp
bool q12_filter_lineitem(const lineitem_t& l) {
    auto shipmode = std::string_view(l.l_shipmode.data, l.l_shipmode.length);
    return (shipmode == "MAIL" || shipmode == "SHIP")
        && l.l_shipdate < l.l_commitdate
        && l.l_commitdate < l.l_receiptdate
        && l.l_receiptdate >= DATE_1994_01_01
        && l.l_receiptdate < DATE_1995_01_01;
}
```

## Q12 Projection

```cpp
q12_result_t q12_project(const orders_t::Key& ok, const orders_t& o,
                          const lineitem_t::Key& lk, const lineitem_t& l) {
    q12_result_t result;
    result.key.l_shipmode = l.l_shipmode;
    result.key.o_orderkey = ok.o_orderkey;
    result.key.l_linenumber = lk.l_linenumber;
    auto prio = std::string_view(o.o_orderpriority.data, o.o_orderpriority.length);
    result.high_line_count = (prio == "1-URGENT") ? 1 : 0;
    result.low_line_count = (prio != "1-URGENT") ? 1 : 0;
    result.l_shipdate = l.l_shipdate;
    result.l_commitdate = l.l_commitdate;
    result.l_receiptdate = l.l_receiptdate;
    return result;
}
```

---

## Stages × Options

### Stage 1: Loading

#### Options 1+2 (Traditional Indexes)

Load ORDERS and LINEITEM into **separate** `RocksDBAdapter<orders_t>` and `RocksDBAdapter<lineitem_t>`.

```
TPCHWorkload::load()  // loads all 8 tables into individual adapters
```

No merged indexes or views needed. Reuse `TPCHWorkload` as-is with default insert functions.

#### Option 3 (Fully Materialized View)

Load base tables, then compute the full Q12 result and store in `RocksDBAdapter<q12_view_t>` where `q12_view_t` stores pre-aggregated results:

```cpp
struct q12_view_t {
    static constexpr int id = 31;
    struct Key {
        Varchar<10> l_shipmode;   // only 2 values: MAIL, SHIP (after filtering)
        ADD_KEY_TRAITS(l_shipmode)
    };
    Integer high_line_count;      // SUM of highs
    Integer low_line_count;       // SUM of lows
    ADD_RECORD_TRAITS(high_line_count, low_line_count)
};
```

After loading base tables:
1. Scan LINEITEM, filter by Q12 conditions
2. For each qualifying lineitem, lookup its order from ORDERS adapter
3. Project HIGH/LOW counts
4. Aggregate by shipmode, insert 2 rows into view

#### Option 4 (MI[0] Only)

Load ORDERS and LINEITEM into `RocksDBMergedAdapter<orders_t, lineitem_t>`.

```
TPCHWorkload::loadOrders([&](const orders_t::Key& ok, const orders_t& o) {
    orders_adapter.insert(ok, o);         // base table
    merged_adapter.insert(ok, o);         // MI[0]
});
TPCHWorkload::loadLineitem([&](const lineitem_t::Key& lk, const lineitem_t& l) {
    lineitem_adapter.insert(lk, l);       // base table
    merged_adapter.insert(lk, l);         // MI[0]
});
```

orders_t key = `{o_orderkey}` (4 bytes folded), lineitem_t key = `{l_orderkey, l_linenumber}` (8 bytes folded). Natural interleaving: for each orderkey, order record sorts before its lineitems (shorter key prefix). `toType()` distinguishes by fold-length (4 vs 8).

#### Option 5 (MI[0] + Root Pipeline MI)

Same as Option 4 for MI[0], plus run the **index creation plan** over MI[0]:

```
// After MI[0] is populated:
// Scan MI[0] with PremergedJoin → project → insert into root pipeline MI
PremergedJoin<MergedScanner, Integer, q12_result_t, orders_t, lineitem_t> join(merged_scanner);
while (auto result = join.next()) {
    auto [key, record] = *result;
    q12_result_t projected = q12_project(/* extract order and lineitem from join */);
    root_mi_adapter.insert(projected.key, projected);
}
```

The root pipeline MI is a **regular (non-merged) adapter**: `RocksDBAdapter<q12_result_t>`. It stores one record type, keyed by `{l_shipmode, o_orderkey, l_linenumber}`.

---

### Stage 2: Queries

#### Option 1 (Traditional + Hash Join)

```
1. Scan lineitem_adapter (full scan)
2. For each lineitem, apply q12_filter_lineitem()
3. Hash qualifying lineitems by l_orderkey into hash table
4. Scan orders_adapter
5. Probe hash table for each order
6. For matches: q12_project(), accumulate aggregates by shipmode
```

Pattern: similar to `HashJoiner` in geo's `join_search_count.tpp`, but simpler (2 tables, not 5).

#### Option 2 (Traditional + Merge Join)

```
1. Scan lineitem_adapter sorted by l_orderkey (natural key order)
2. Scan orders_adapter sorted by o_orderkey (natural key order)
3. BinaryMergeJoin on orderkey
4. For each joined pair: apply q12_filter_lineitem(), q12_project()
5. Accumulate aggregates by shipmode
```

Pattern: similar to `BaseJoiner` in geo's `join_search_count.tpp`, but with only one `BinaryMergeJoin` stage instead of cascading 4.

#### Option 3 (Materialized View)

```
1. Scan q12_view_adapter (only 2 records: MAIL and SHIP)
2. Return results directly
```

Trivial — the view stores pre-aggregated results. This is the fastest query but most expensive to maintain.

#### Option 4 (MI[0] PremergedJoin)

```
1. Create MergedScanner over MI[0]
2. PremergedJoin<Scanner, Integer, q12_result_t, orders_t, lineitem_t>
3. For each joined (order, lineitem) pair:
   a. q12_project() → q12_result_t
   b. q12_filter() on result (query-time filter)
   c. If passes: accumulate aggregates by shipmode
```

Pattern: similar to `MergedJoiner` in geo, but with filter applied after join (matching Calcite's hoisted filter architecture).

#### Option 5 (Root Pipeline MI Scan)

```
1. Scan root_mi_adapter (RocksDBAdapter<q12_result_t>)
2. For each record: apply q12_filter() (query-time filter)
3. Accumulate aggregates by shipmode (sorted aggregate — records naturally sorted by shipmode)
```

This is the **Calcite query plan**: `MergedIndexScan → Filter → SortedAggregate`. No join at query time. The sorted aggregate can be done in a single pass since records are keyed by shipmode.

---

### Stage 3: Maintenance (RF1 Insert / RF2 Delete)

#### Options 1+2 (Traditional Indexes)

**RF1** (insert new order + lineitems):
```
orders_adapter.insert(new_order_key, new_order);
for each new_lineitem:
    lineitem_adapter.insert(new_lineitem_key, new_lineitem);
```

**RF2** (delete order + lineitems):
```
// Scan lineitem by orderkey prefix, collect linenumbers
lineitem_adapter.scan(lineitem_t::Key{orderkey, 0}, [&](auto& lk, auto& l) {
    lineitem_adapter.erase(lk);
});
orders_adapter.erase(orders_t::Key{orderkey});
```

No view/MI maintenance needed.

#### Option 3 (Materialized View)

**RF1**:
```
// Insert into base tables
orders_adapter.insert(new_order_key, new_order);
for each new_lineitem:
    lineitem_adapter.insert(new_lineitem_key, new_lineitem);
    // Immediately check if this lineitem qualifies for the view
    if (q12_filter_lineitem(new_lineitem)) {
        auto projected = q12_project(new_order_key, new_order, lk, new_lineitem);
        // Update aggregated view: lookup shipmode, increment counts
        q12_view_adapter.update1(
            q12_view_t::Key{projected.key.l_shipmode},
            [&](q12_view_t& v) {
                v.high_line_count += projected.high_line_count;
                v.low_line_count += projected.low_line_count;
            });
    }
```

**RF2**: Reverse — lookup order and its lineitems, for each qualifying lineitem decrement view counts, then delete base records.

#### Option 4 (MI[0] Only)

**RF1**:
```
// Insert into base tables + MI[0]
orders_adapter.insert(ok, order);
merged_adapter.insert(ok, order);          // MI[0]
for each lineitem:
    lineitem_adapter.insert(lk, lineitem);
    merged_adapter.insert(lk, lineitem);   // MI[0]
```

**RF2**: Delete from both base tables and MI[0].

No further maintenance — query-time join handles everything.

#### Option 5 (MI[0] + Root Pipeline MI)

**RF1** — Calcite maintenance plan (two branches):
```
// 1. Insert into base tables + MI[0]
orders_adapter.insert(ok, order);
merged_adapter.insert(ok, order);

for each lineitem:
    lineitem_adapter.insert(lk, lineitem);
    merged_adapter.insert(lk, lineitem);

    // Branch 1: LINEITEM delta × ORDERS snapshot
    // Lookup the order for this lineitem's orderkey in MI[0]
    auto order_opt = merged_adapter.lookup1(orders_t::Key{lk.l_orderkey});
    if (order_opt) {
        auto projected = q12_project(orders_key, *order_opt, lk, lineitem);
        root_mi_adapter.insert(projected.key, projected);
    }
```

Note: Branch 2 (ORDERS delta × LINEITEM snapshot) fires when the order is inserted. At that point, no lineitems exist yet for this order in MI[0], so Branch 2 produces nothing. This is correct for RF1 where order is inserted before its lineitems.

**RF2**:
```
// Delete from root pipeline MI first
// Scan MI[0] for lineitems of this orderkey
merged_adapter.scan(lineitem_t::Key{orderkey, 0}, [&](auto& lk, auto& l) {
    if (std::holds_alternative<lineitem_t>(l)) {
        auto& li = std::get<lineitem_t>(l);
        auto& lik = std::get<lineitem_t::Key>(lk);
        // Lookup the order
        auto order_opt = merged_adapter.lookup1(orders_t::Key{orderkey});
        if (order_opt) {
            auto projected = q12_project(orders_t::Key{orderkey}, *order_opt, lik, li);
            root_mi_adapter.erase(projected.key);
        }
    }
});
// Then delete from MI[0] and base tables
// (reverse order: lineitems first, then order)
```

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

```
frontend/tpch/q12/
    Q12.md                     -- This plan document
    q12_views.hpp              -- q12_result_t, q12_view_t, q12_filter, q12_project
    q12_workload.hpp           -- Q12Workload class template
    q12_query.tpp              -- Query implementations per storage structure
    q12_maintain.tpp           -- RF1/RF2 maintenance per storage structure
    executable_rocksdb.cpp     -- RocksDB entry point
    executable_leanstore.cpp   -- LeanStore entry point (Linux only)
```

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

```cpp
// executable_rocksdb.cpp (sketch)
int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

    // Base table adapters
    RocksDBAdapter<orders_t> orders(rocks_db);
    RocksDBAdapter<lineitem_t> lineitem(rocks_db);
    // ... other TPC-H tables for TPCHWorkload

    // MI[0] — merged orders + lineitem
    RocksDBMergedAdapter<orders_t, lineitem_t> merged_ol(rocks_db);

    // Root pipeline MI — q12 result
    RocksDBAdapter<q12_result_t> root_mi(rocks_db);

    // Fully materialized view (pre-aggregated)
    RocksDBAdapter<q12_view_t> q12_view(rocks_db);

    rocks_db.open();

    TPCHWorkload<RocksDBAdapter> tpch(orders, lineitem, ...);
    Q12Workload<RocksDBAdapter, RocksDBMergedAdapter, RocksDBScanner, RocksDBMergedScanner>
        q12(tpch, merged_ol, root_mi, q12_view, orders, lineitem);

    if (!FLAGS_recover) {
        q12.load();  // dispatches to appropriate loading based on storage_structure
    } else {
        tpch.recover_last_ids();
    }

    switch (FLAGS_storage_structure) {
        case 1: /* hash join */ break;
        case 2: /* merge join */ break;
        case 3: /* materialized view */ break;
        case 4: /* MI[0] only */ break;
        case 5: /* MI[0] + root MI */ break;
    }
}
```

---

## Dependency on Tagged Row Format

The current plan uses fold-length discrimination (4 vs 8 bytes) for MI[0], which works for Q12. Tagged row format migration is tracked separately and is NOT a blocker for initial Q12 implementation. Can be retrofitted later.

---

## Open Questions (from SESSION_PROGRESS.md)

1. **RF2 delete strategy**: TPC-H RF2 deletes orders by orderkey. But how does this interact with the root pipeline MI keyed by `(shipmode, orderkey, linenumber)`? Need to scan/probe by orderkey within the MI. See SESSION_PROGRESS §5, §6.

2. **Plans for options 1-2**: Calcite's "before" plans are optimized for interesting orderings (merge-join friendly). Should traditional-index options use unoptimized plans? This is a side project — not blocking implementation.

3. **Spectrum between options 4 and 5**: In more complex queries, there may be intermediate pipelines between the leaf MI and the root MI. Q12 has only 2 pipelines, so the spectrum collapses to just "MI[0] only" vs "MI[0] + root MI."
