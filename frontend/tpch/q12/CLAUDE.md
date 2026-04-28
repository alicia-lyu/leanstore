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

Root pipeline MI entry — the output schema of the `ProjectIterator` in the index creation plan. One record per (order, lineitem) join result, **unfiltered** (predicate hoisting).

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

## Operator Framework

Every query plan — index creation, query, and maintenance — is expressed as a tree of composable pull-based operators. This mirrors the Cascade/Volcano iterator model and maps directly onto Calcite's relational algebra nodes, making plan interpretation mechanical once the framework exists.

### Base Interface

```cpp
template <typename Output>
class Iterator {
public:
    virtual ~Iterator() = default;
    virtual void open() = 0;               // initialize / reset state
    virtual std::optional<Output> next() = 0;  // pull next row; nullopt when done
    virtual void close() = 0;             // release resources
};
```

Each concrete operator takes its child(ren) as constructor arguments, forming a tree. The root is driven by a consumer (e.g., `InsertIterator` as a sink, or a bare `while (auto row = root->next())` loop for aggregation).

### Concrete Operators

**`ScanIterator<Adapter, Row>`** — wraps a single-type adapter's full scan.

```cpp
// next() returns the next row from adapter, nullopt at end of scan
ScanIterator<RocksDBAdapter<orders_t>, orders_t>(orders_adapter)
```

**`MergedScanIterator<MergedAdapter, Records...>`** — wraps a merged scanner over a merged index.

```cpp
// next() returns std::variant<Records...> discriminated by tagged key
MergedScanIterator<RocksDBMergedAdapter<orders_t, lineitem_t>, orders_t, lineitem_t>(mi0_adapter)
```

An optional seek key narrows the scan to a key prefix (used in maintenance delta joins).

**`FilterIterator<Row>`** — skips rows that do not satisfy a predicate.

```cpp
// Constructor: child iterator + predicate
FilterIterator<q12_result_t>(child, std::function<bool(const q12_result_t&)>)
// next() pulls from child, loops until predicate passes or child exhausted
```

**`ProjectIterator<Input, Output>`** — transforms each row from Input to Output.

```cpp
// Constructor: child iterator + transform function
ProjectIterator<joined_ol_t, q12_result_t>(child, std::function<q12_result_t(const joined_ol_t&)>)
// next() pulls one row, applies transform, returns result
```

**`SortedAggregateIterator<Input, Output>`** — groups consecutive rows sharing the same key, emitting one aggregated Output per group. Requires input sorted by group key (enforced by key design).

```cpp
// Constructor: child, key extractor, accumulator (fold function + finalize)
SortedAggregateIterator<q12_result_t, q12_agg_row_t>(child, shipmode_key, count_accum)
// next() consumes a run of same-key rows, returns one aggregated group
```

**`PremergedJoinIterator<JK, Output, Records...>`** — wraps existing `PremergedJoin` logic as an iterator. Scans the merged index and assembles complete join results (one record of each type per join key group).

```cpp
// Constructor: merged scan child
PremergedJoinIterator<Integer, joined_ol_t, orders_t, lineitem_t>(merged_scan_child)
// next() returns the next fully-assembled join result, nullopt when scan exhausted
```

**`HashJoinIterator<Left, Right, Output>`** — classic build-probe hash join. Build phase consumes the entire left child on `open()`; `next()` probes with each right-child row.

```cpp
HashJoinIterator<lineitem_t, orders_t, joined_ol_t>(left_child, right_child, join_key_fn, combine_fn)
```

**`BinaryMergeJoinIterator<Left, Right, Output>`** — merge-join two sort-ordered child iterators on a shared key.

```cpp
BinaryMergeJoinIterator<lineitem_t, orders_t, joined_ol_t>(left_child, right_child, key_fn, combine_fn)
```

**`InsertIterator<Row>`** — sink operator. Pulls from child, inserts each row into an adapter, and returns the inserted row (enabling counting or chaining).

```cpp
// Constructor: child + target adapter
InsertIterator<q12_result_t>(child, root_mi_adapter)
// Drive by exhausting: while (sink->next()) {}
```

The key property: every plan is just a different tree composition of these operators, parameterized by the specific predicate/projection/accumulator functions described below.

---

## Q12-Specific Operator Configurations

These named functions plug into the generic operators. They live in `q12_operators.hpp`.

### `q12_predicate`

Plugs into `FilterIterator<q12_result_t>`. Encodes the five Q12 conditions hoisted above the root MI boundary:

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

Plugs into `ProjectIterator<joined_ol_t, q12_result_t>`. Implements Calcite's projection `$23, $0, $10, CASE($5 IN ('1-URGENT','2-HIGH'),1,0), CASE($5 NOT IN ('1-URGENT','2-HIGH'),1,0), $19, $20, $21`:

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

Plugs into `SortedAggregateIterator<q12_result_t, q12_agg_row_t>`. Groups by shipmode (the first key component), sums `high_line_count` and `low_line_count`:

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

The three Calcite plans from `calcite-integration-info/test-plans/q12/` each become a tree of the operators above. The same operators compose differently; only the plugged-in functions differ.

### Index Creation Plan (populates root pipeline MI from MI[0])

```
InsertIterator<q12_result_t>(root_mi_adapter,
  ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
    PremergedJoinIterator<Integer, joined_ol_t, orders_t, lineitem_t>(
      MergedScanIterator<orders_t, lineitem_t>(mi0_adapter))))
```

Drive by exhausting the sink: `while (index_creation_plan->next()) {}`.

### Query Plan (option 5 — root MI scan)

```
SortedAggregateIterator<q12_result_t, q12_agg_row_t>(shipmode_key, count_accum,
  FilterIterator<q12_result_t>(q12_predicate,
    ScanIterator<q12_result_t>(root_mi_adapter)))
```

### Maintenance Plan — RF1 Insert, Branch 1 (LINEITEM delta × ORDERS snapshot)

For each new lineitem, probe MI[0] with an orderkey prefix to retrieve its order, then propagate to the root MI:

```
InsertIterator<q12_result_t>(root_mi_adapter,
  ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
    PremergedJoinIterator<Integer, joined_ol_t, orders_t, lineitem_t>(
      MergedScanIterator<orders_t, lineitem_t>(mi0_adapter, orderkey_prefix))))
```

The `MergedScanIterator` with an `orderkey_prefix` seek restricts the scan to one order group. This effectively performs the snapshot × delta join: the scan returns the existing orders_t snapshot record plus the newly inserted lineitem_t delta. Branch 2 (ORDERS delta × LINEITEM snapshot) is a symmetric tree; for RF1 it fires when the order is inserted, but at that point no lineitems yet exist for this orderkey, so it produces nothing — which is correct.

---

## Stages × Options

The key insight is that every (Load, Query, Maintain) × option combination is just a different operator tree composition. Loading always writes to adapters; the operator tree describes the data transformation.

### Stage 1: Loading

| Option | What gets populated | How |
|--------|--------------------|----|
| 1, 2 | Base tables only | `TPCHWorkload::load()` — standard per-table insert |
| 3 | Base tables + `q12_view_t` adapter | Standard load, then drive the Option 3 index creation tree (see below) |
| 4 | Base tables + MI[0] | Dual-write: each `orders_t`/`lineitem_t` insert also goes to `merged_ol_adapter` |
| 5 | Base tables + MI[0] + root MI | Option 4 load, then drive the Index Creation Plan tree above |

orders_t key = `{o_orderkey}` (4 bytes folded), lineitem_t key = `{l_orderkey, l_linenumber}` (8 bytes folded). Natural interleaving: for each orderkey, the order record sorts before its lineitems (shorter key prefix). `toType()` discriminates by fold-length (4 vs 8).

**Option 3 index creation tree** (builds pre-aggregated `q12_view_t`):

```
InsertIterator<q12_view_t>(q12_view_adapter,
  SortedAggregateIterator<q12_result_t, q12_view_t>(shipmode_key, count_accum,
    FilterIterator<q12_result_t>(q12_predicate,
      ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
        HashJoinIterator<lineitem_t, orders_t, joined_ol_t>(orderkey,
          ScanIterator<lineitem_t>(lineitem_adapter),
          ScanIterator<orders_t>(orders_adapter))))))
```

### Stage 2: Queries

**Option 1 (Hash Join):**

```
SortedAggregateIterator<joined_ol_t, q12_agg_row_t>(shipmode_key, count_accum,
  ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
    HashJoinIterator<lineitem_t, orders_t, joined_ol_t>(orderkey,
      FilterIterator<lineitem_t>(q12_predicate_lineitem,
        ScanIterator<lineitem_t>(lineitem_adapter)),
      ScanIterator<orders_t>(orders_adapter))))
```

Note: `SortedAggregateIterator` here requires a sort step after the hash join because hash join output is unordered. In practice this means either materializing into a sorted buffer or using a hash aggregate. For the design document we annotate this as requiring an intermediate sort.

**Option 2 (Merge Join):**

```
SortedAggregateIterator<q12_result_t, q12_agg_row_t>(shipmode_key, count_accum,
  ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
    FilterIterator<joined_ol_t>(q12_predicate_joined,
      BinaryMergeJoinIterator<lineitem_t, orders_t, joined_ol_t>(orderkey,
        ScanIterator<lineitem_t>(lineitem_adapter),
        ScanIterator<orders_t>(orders_adapter)))))
```

Both scans produce rows in natural orderkey order. Merge join output sorted by orderkey, not by shipmode — so `SortedAggregateIterator` again requires an intermediate sort on shipmode.

**Option 3 (Materialized View):**

```
ScanIterator<q12_view_t>(q12_view_adapter)
```

Trivial — the pre-aggregated view holds at most 2 rows (MAIL and SHIP). No join, no filter, no aggregation at query time.

**Option 4 (MI[0] PremergedJoin):**

```
SortedAggregateIterator<q12_result_t, q12_agg_row_t>(shipmode_key, count_accum,
  FilterIterator<q12_result_t>(q12_predicate,
    ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
      PremergedJoinIterator<Integer, joined_ol_t, orders_t, lineitem_t>(
        MergedScanIterator<orders_t, lineitem_t>(mi0_adapter)))))
```

Output of `ProjectIterator` is not yet sorted by shipmode, so an intermediate sort is needed before `SortedAggregateIterator`. Alternatively: use a hash aggregate.

**Option 5 (Root MI Scan — the Calcite query plan):**

```
SortedAggregateIterator<q12_result_t, q12_agg_row_t>(shipmode_key, count_accum,
  FilterIterator<q12_result_t>(q12_predicate,
    ScanIterator<q12_result_t>(root_mi_adapter)))
```

This is the cleanest plan: root MI records are keyed by shipmode, so the scan is already sorted and `SortedAggregateIterator` needs no intermediate sort. No join at query time.

### Stage 3: Maintenance (RF1 Insert / RF2 Delete)

**Options 1+2 — RF1:** Insert `new_order` into `orders_adapter`, insert each `new_lineitem` into `lineitem_adapter`. No further work.

**Options 1+2 — RF2:** Scan `lineitem_adapter` by orderkey prefix; erase each found lineitem. Erase order from `orders_adapter`. No view/MI maintenance.

**Option 3 — RF1:**

```
// For each new lineitem: drive this tree (produces at most 1 row to update view)
InsertOrUpdateIterator<q12_view_t>(q12_view_adapter, merge_fn,
  ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
    FilterIterator<joined_ol_t>(q12_predicate_joined,
      SingleRowJoinIterator<lineitem_t, orders_t>(new_lineitem, orders_adapter))))
```

**Option 3 — RF2:** Symmetric reverse: for each deleted lineitem that passes the predicate, decrement view counts. Delete base records.

**Option 4 — RF1:** Dual-write inserts to `orders_adapter` + `merged_ol_adapter` (order), and to `lineitem_adapter` + `merged_ol_adapter` (each lineitem). No further maintenance — query-time join handles everything.

**Option 4 — RF2:** Delete from both base tables and MI[0].

**Option 5 — RF1 (Calcite maintenance plan, Branch 1 per lineitem):**

```
InsertIterator<q12_result_t>(root_mi_adapter,
  ProjectIterator<joined_ol_t, q12_result_t>(q12_projection,
    PremergedJoinIterator<Integer, joined_ol_t, orders_t, lineitem_t>(
      MergedScanIterator<orders_t, lineitem_t>(mi0_adapter, orderkey_prefix))))
```

Branch 2 (ORDERS delta × LINEITEM snapshot) fires at order insertion time — at that point MI[0] has no lineitems for this orderkey yet, so it produces nothing. Correct for RF1 ordering.

**Option 5 — RF2:** Scan MI[0] with orderkey prefix to enumerate existing lineitems; for each, reconstruct the `q12_result_t` key and erase from root MI. Then delete from MI[0] and base tables (lineitems first, then order).

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

Operator framework (reusable across all future queries):

```
frontend/shared/operators/
    iterator.hpp          -- Iterator<Output> base class template
    scan.hpp              -- ScanIterator, MergedScanIterator
    filter.hpp            -- FilterIterator
    project.hpp           -- ProjectIterator
    aggregate.hpp         -- SortedAggregateIterator
    join.hpp              -- HashJoinIterator, BinaryMergeJoinIterator, PremergedJoinIterator
    insert.hpp            -- InsertIterator (sink)
```

Q12-specific:

```
frontend/tpch/q12/
    CLAUDE.md                  -- This plan document
    q12_types.hpp              -- q12_result_t, q12_view_t, q12_agg_row_t
    q12_operators.hpp          -- q12_predicate, q12_projection, q12_accumulator
    q12_plans.hpp              -- Factory functions composing operator trees per plan
    q12_workload.hpp           -- Q12Workload class (load, run dispatch)
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

The executable instantiates adapters and selects the operator tree for the active storage structure. Each `case` branch calls a factory function from `q12_plans.hpp` that returns a composed operator tree.

```cpp
// executable_rocksdb.cpp (sketch)
int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

    RocksDBAdapter<orders_t>   orders_adapter(rocks_db);
    RocksDBAdapter<lineitem_t> lineitem_adapter(rocks_db);
    // ... other TPC-H tables for TPCHWorkload

    RocksDBMergedAdapter<orders_t, lineitem_t> mi0_adapter(rocks_db);
    RocksDBAdapter<q12_result_t>               root_mi_adapter(rocks_db);
    RocksDBAdapter<q12_view_t>                 q12_view_adapter(rocks_db);

    rocks_db.open();

    TPCHWorkload<RocksDBAdapter> tpch(orders_adapter, lineitem_adapter, ...);
    Q12Workload q12(tpch, mi0_adapter, root_mi_adapter, q12_view_adapter,
                    orders_adapter, lineitem_adapter);

    if (!FLAGS_recover) {
        q12.load();   // dispatches based on FLAGS_storage_structure
    } else {
        tpch.recover_last_ids();
    }

    // Each case builds and drives the appropriate operator tree from q12_plans.hpp
    switch (FLAGS_storage_structure) {
        case 1: q12.run_hash_join();   break;
        case 2: q12.run_merge_join();  break;
        case 3: q12.run_mat_view();    break;
        case 4: q12.run_mi0_join();    break;
        case 5: q12.run_root_mi();     break;
    }
}
```

---

## Dependency on Tagged Row Format

The current plan uses fold-length discrimination (4 vs 8 bytes) for MI[0], which works for Q12. Tagged row format migration is tracked separately and is NOT a blocker for initial Q12 implementation. Can be retrofitted later.

---

## Execution Style: Monolithic vs Cascade

The operator framework above (§Operator Framework, §Q12 Plans as Operator Trees) follows cascade/iterator style. An alternative **monolithic** style avoids intermediate record types by using single functions with local variables.

### Record Types Strictly Required (Either Style)

- **Base tables**: `orders_t`, `lineitem_t` — in `tpch_tables.hpp`
- **MI records**: Same `orders_t`, `lineitem_t` in `MI_ORDERS_LINEITEM` (structure 3/4)
- **Root MI / view output** (structure 2/5): `q12_result_t` or `q12_view_t` — must be typed because they are stored in adapters

### What Cascade Style Requires Additionally

- `joined_ol_t` (or `joined_t<orders_t, lineitem_t>`) — output of hash/merge/premerged join iterators
- `q12_agg_row_t` — output of SortedAggregateIterator
- Every iterator template is parameterized on these types

### What Monolithic Execution Looks Like

```
// Structure 4 sketch (MI PremergedJoin, no cascade)
void q12_query_structure4(MergedAdapter& mi) {
    Integer high_mail = 0, low_mail = 0, high_ship = 0, low_ship = 0;

    premerged_join(mi, [&](orders_t& o, lineitem_t& l) {
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

Key observations:

- **No intermediate record types at all**: Filter, project, and aggregate are fused into the callback lambda
- **No `joined_ol_t`**: The `premerged_join` callback receives references to the original `orders_t` and `lineitem_t` — field access is direct (`o.o_orderpriority`, `l.l_shipmode`)
- **Aggregation is trivial**: Q12 groups by shipmode (2 values for validation params), so 4 counters suffice — no sorted aggregate needed

### Feasibility Assessment

**Q12 is the ideal case for monolithic execution** because:

1. Only 2 tables, 1 join — the callback body is ~15 lines
2. Aggregation by shipmode has tiny cardinality (2-7 groups) — local counters, no sort
3. All 5 filter predicates can be checked inline with zero overhead
4. The existing `premerged_join` utility already provides exactly the callback interface needed

**Trade-offs vs cascade (§Operator Framework)**:

- **Pro**: Eliminates `joined_ol_t` and `q12_agg_row_t` definitions entirely
- **Pro**: ~30 lines of code vs ~100+ lines of operator tree composition
- **Pro**: No virtual dispatch overhead from iterator `next()` calls
- **Con**: Structure 5 (root MI scan) still needs `q12_result_t` as a stored record type — monolithic style doesn't help there
- **Con**: Maintenance plans (§Stage 3) are harder to express monolithically when they involve delta joins — but for the current paper scope (no maintenance experiments), this is not a blocker

### Recommendation

For the current paper (single MI per query, no maintenance), use monolithic style for all query-time execution. Reserve cascade/iterator style for future work where operator composition and reuse across plans becomes necessary.

---

## Open Questions (from SESSION_PROGRESS.md)

1. **RF2 delete strategy**: TPC-H RF2 deletes orders by orderkey. But how does this interact with the root pipeline MI keyed by `(shipmode, orderkey, linenumber)`? Need to scan/probe by orderkey within the MI. See SESSION_PROGRESS §5, §6.

2. **Plans for options 1-2**: Calcite's "before" plans are optimized for interesting orderings (merge-join friendly). Should traditional-index options use unoptimized plans? This is a side project — not blocking implementation.

3. **Spectrum between options 4 and 5**: In more complex queries, there may be intermediate pipelines between the leaf MI and the root MI. Q12 has only 2 pipelines, so the spectrum collapses to just "MI[0] only" vs "MI[0] + root MI."

4. **Operator framework scope**: Should `frontend/shared/operators/` be built from the start (reusable from Q12 onward), or should operator classes start in `frontend/tpch/q12/` and be promoted to shared once a second query is implemented? Starting in shared costs little and avoids a refactor.
