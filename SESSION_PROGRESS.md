# Session Progress: TPC-H Q12 from Calcite-Generated Plans

## Status: GRAND PLAN COMPLETE → Ready for CONCRETE PLAN

## Goal

Implement TPC-H Q12 as the first Calcite-planned query, translating optimizer-generated plans from `calcite-integration-info/test-plans/q12/` into manually coded C++. This validates the Calcite↔LeanStore integration architecture before building a general-purpose plan interpreter.

---

## Calcite Q12 Plans (from `calcite-integration-info/test-plans/q12/`)

### What the plans prescribe

**2 merged indexes:**

- **MI[0]** (Pipeline 0): Contains ORDERS (sourceIndex=0) and LINEITEM (sourceIndex=1) interleaved by orderkey. Created trivially — just insert both record types. No index creation plan needed.
- **Root pipeline MI** (Pipeline 1): An indexed view storing the **unfiltered** join→project result from Pipeline 0. After predicate hoisting, filters live only in the root query plan — the MI stores all joined rows with date columns preserved for query-time filtering. Keyed by L_SHIPMODE. The "index creation plan" describes how to populate this MI by scanning MI[0].

**1 index creation plan** (`pipeline-0-index-creation-plan.dot`):
Scan MI[0] → MergeJoin(ORDERS, LINEITEM) on O_ORDERKEY=L_ORDERKEY → Project → populate root pipeline MI.

After predicate hoisting, **no filter** is applied during index creation. The MI stores all joined rows.

Projection (widened to preserve columns needed for query-time filtering):

- `$0` = `L_SHIPMODE` (from lineitem)
- `$1` = `HIGH_LINE_COUNT = CASE(O_ORDERPRIORITY = '1-URGENT', 1, 0)`
- `$2` = `LOW_LINE_COUNT = CASE(O_ORDERPRIORITY <> '1-URGENT', 1, 0)`
- `$3` = `L_SHIPDATE` (preserved for query-time filter)
- `$4` = `L_COMMITDATE` (preserved for query-time filter)
- `$5` = `L_RECEIPTDATE` (preserved for query-time filter)

**1 query plan** (`root-pipeline-query-plan.dot`):
Scan root pipeline MI → **Filter** → SortedAggregate(GROUP BY L_SHIPMODE, SUM(HIGH_LINE_COUNT), SUM(LOW_LINE_COUNT)).

Filter conditions (applied at query time, hoisted from index creation):

- `L_SHIPMODE IN ('MAIL', 'SHIP')` — `$0`
- `L_SHIPDATE < L_COMMITDATE` — `$3 < $4`
- `L_COMMITDATE < L_RECEIPTDATE` — `$4 < $5`
- `L_RECEIPTDATE >= '1994-01-01'` — `$5 >= date`
- `L_RECEIPTDATE < '1995-01-01'` — `$5 < date`

**1 maintenance plan** (`physical-maintenance.dot`):
Propagates base table deltas to the root pipeline MI. MI[0] maintenance is trivial (mirror base table inserts/deletes). Per predicate hoisting, the maintenance plan should be **filter-free** — inserting unfiltered join+project results into the root pipeline MI. Two-branch delta processing:

- Branch 1: MergedIndexScan(ORDERS snapshot) × MergedIndexDeltaScan(LINEITEM delta) → join → project → insert/delete on root pipeline MI
- Branch 2: MergedIndexDeltaScan(ORDERS delta) × MergedIndexScan(LINEITEM snapshot) → join → project → insert/delete on root pipeline MI
- UNION both branches

*Note: The current `physical-maintenance.dot` still shows Filter+Project in the maintenance plan. This may not yet reflect the hoisting change — the design intent is filter-free maintenance.*

### Calcite column index mapping (concatenated ORDERS ∥ LINEITEM schema)

- `$0`: O_ORDERKEY
- `$5`: O_ORDERPRIORITY
- `$9`: L_ORDERKEY (= $0, join condition)
- `$19`: L_SHIPDATE
- `$20`: L_COMMITDATE
- `$21`: L_RECEIPTDATE
- `$23`: L_SHIPMODE

*(Must verify against actual struct field order in `tpch_tables.hpp` during concrete planning.)*

---

## Existing Infrastructure (reusable)

### Record Types (`frontend/geo/tpch_tables.hpp`)

- `orders_t` (id=4): Key=`{o_orderkey}`, has `o_orderpriority` (Varchar<15>)
- `lineitem_t` (id=5): Key=`{l_orderkey, l_linenumber}`, has `l_shipmode` (Varchar<10>), `l_shipdate`, `l_commitdate`, `l_receiptdate` (all Integer/Timestamp)
- 6 other TPC-H tables fully defined

### Data Loading (`frontend/geo/tpch_workload.hpp`)

- `TPCHWorkload<AdapterType>` loads all 8 tables with scale-factor-based counts
- ORDERS_SCALE=1500/SF, LINEITEM_SCALE=6000/SF
- Supports custom insert functions for flexible data routing

### Merged Adapters (`frontend/shared/adapter-scanner/`)

- `RocksDBMergedAdapter<Records...>` / `LeanStoreMergedAdapter<Records...>`: variadic template storing heterogeneous records in one index
- Key interleaving: records sorted by shared key prefix, distinguished by fold-length + value-length (current) → to be replaced by tagged row format
- `RocksDBMergedScanner` / `LeanStoreMergedScanner`: return `std::variant<Records::Key..., Records...>` pairs

### Join Infrastructure (`frontend/shared/merge-join/`)

- `PremergedJoin`: single-pass join over merged scanner with adaptive seek strategy and join state machine
- `BinaryMergeJoin`: two-way merge join from independent sources
- `HashJoin`: hash-based join
- `JoinState`: caching + Cartesian product + output buffering. This class adds quite some overhead to hash join, which I previously implemented without JoinState. Investigation pending.

### View Templates (`frontend/shared/view_templates.hpp`)

- `joined_t<TID, JK, fold_pks, Ts...>`: joined record type with composite key -> to be modified so that the input records are flattened with proper projection, instead of being stored in one joined record as a tuple of several input records.
- `merged_t<TID, T, JK, extra_id>`: single record in merged index -> to be modified to tagged row format
- `SKBuilder<JK>`: uniform key extraction across all types -> Naming convention: JK vs SK to be made consistent (use SK instead of JK, our work is focused on sorts)

### Executable Framework (`frontend/geo/executable_helper.hpp`)

- `ExecutableHelper` + `DBTraits` (LeanStoreTraits / RocksDBTraits)
- `schedule_bg_txs()`: background insert/erase/lookup thread -> bugs in Leanstore when doing background inserts, likely because some records shift to different pages. It should work in RocksDB.
- `tput_tx()`: benchmark query throughput measurement
- `jumpmuTry/jumpmuCatch` for transaction exception handling

### Geo Benchmark Pattern (`frontend/geo/`)

- `GeoJoin` workload class: holds base tables, merged adapter, view adapters
- `join_search_count.tpp`: BaseJoiner/MergedJoiner/HashJoiner query patterns
- `views.hpp`: sort_key_t, SKBuilder specializations, intermediate join types
- `executable_rocksdb.cpp`: adapter instantiation, storage_structure dispatch, load/run lifecycle

---

## Key Design Decisions

### 1. Directory Structure: `frontend/tpch/q12/`

New directory, sibling to `frontend/geo/`. Organized under `tpch/` for future TPC-H queries. Reuses shared infrastructure heavily.

### 2. Two Merged Indexes (Calcite terminology — not "view")

- **MI[0]**: `MergedAdapter<orders_t, lineitem_t>` — raw data, trivially populated during load. Maintenance mirrors base table writes.
- **Root pipeline MI**: Single-record-type adapter storing `q12_result_t` keyed by `{l_shipmode, o_orderkey, l_linenumber}`. Stores **unfiltered** join+project results (including date columns for query-time filtering). Populated by index creation plan (pipeline 0 processing over MI[0]). Shipmode-first key enables sorted aggregate without re-sorting.

In the future, a merged index can either store multiple types of records (from base tables or from views), or one type of records. Each is the source(s) of a pipeline as clear in the before plans.

### 3. Data Loading: Full 8-Table TPC-H

Reuse `TPCHWorkload::load()`. Enhance date generation to produce realistic dates (days since epoch, 1992-1998 range) and order priorities from TPC-H domain (`1-URGENT`, `2-HIGH`, `3-MEDIUM`, `4-NOT SPECIFIED`, `5-LOW`).

### 4. Storage Structure Variants (--storage_structure 1-5)

Pending question: the before plans generated in calcite are optimized for interesting orderings (for merged index-based plans, or for trad index + merged joins). Should option 1 and 3 use a different plan.

1. **Traditional indexes + hash join**: Separate ORDERS and LINEITEM adapters. Query: scan+filter LINEITEM, hash-join ORDERS, project, aggregate.
2. **Traditional indexes + merge join**: Separate ORDERS and LINEITEM adapters. Query: scan+filter LINEITEM, merge-join ORDERS, project, aggregate.
3. **Fully materialized view** of the final query result.
4. **MI[0] only (merged index)**: PremergedJoin over `MergedAdapter<orders_t, lineitem_t>`, filter+project+aggregate at query time. No root pipeline MI.
5. **MI[0] + root pipeline MI (full Calcite plan)**: MI[0] as maintenance source, root pipeline MI for fast query reads.

Option 4 and 5 may become a full spectrum in more complex queries.

### 5. Update Stream: TPC-H RF1/RF2

- **RF1** (insert): New order + 1-7 lineitems. Insert into base tables + MI[0] (trivial mirror). Propagate to root pipeline MI via Branch 1 maintenance.
- **RF2** (delete): Pending question: is it more realistic to delete by orderkey or by shipmode? What does [TPCH](../tpch-kit/doc/tpc-h_v2.17.3.pdf) do?

### 6. Root Pipeline MI Maintenance On Delete

<!-- Keyed by `(shipmode, orderkey, linenumber)`. Delete by orderkey requires only 2 prefix scans (MAIL + SHIP). MI[0] deletion is trivial — delete by key directly. -->

---

## Critical Infrastructure Change: Tagged Row Format

### Problem

Current merged adapter/scanner uses **fold-length + value-length** to distinguish record types (`toType()`). This works when all record types have distinct key lengths but **will not generalize** to future queries where sources may share key lengths.

### Solution: Calcite's `TaggedRowSchema` format

Reference: `TaggedRowSchema.java` in the Calcite repo (`org.apache.calcite.materialize`).

```text
[domainTag_k0][keyVal_k0][domainTag_k1][keyVal_k1]...[indexTag(0x00)][sourceId]...[payload]
```

- **Domain tags** (1 byte each): identify column type. Domains 1..K for key fields, domain 0 for index identifier.
- **Index identifier** (2 bytes): domain tag 0 + 1-byte source ID (0..sourceCount-1). Discriminates record types.
- **Payload**: remaining non-key columns, variable per source.

### Q12 Example (K=1, shared key = orderkey)

- ORDERS (src=0): `[0x00][fold(orderkey)][0x02][0x00][[...order payload]]`
- LINEITEM (src=1): `[0x00][fold(orderkey)][0x01][fold(linenumber)][0x01][[...lineitem payload]]`

Correct interleaving: ORDERS before LINEITEM for same orderkey. Unique keys: lineitems distinguished by linenumber suffix. Record type ID: read sourceId byte at fixed offset.

USER: this doesn't look correct. What does K=1 mean? Key field count doesn't make sense in a merged index, all are byte formatted. I also changed the byte format above.`[0x00]` is the domain tag for orderkey (which also gives us info about the field length), `[0x00]` the domain tag for linenumber, `[0x02]` the domain tag for index identifier. After the domain tag for the index identifier comes the index identifier per se (`[0x00]` for ORDERS, `[0x01]` for LINEITEM)

### Scope of Change

Modify `foldKey()`/`unfoldKey()` in merged adapter context, replace `toType()` with sourceId-based dispatch in merged scanners:

- `RocksDBMergedAdapter.hpp` / `LeanStoreMergedAdapter.hpp`
- `RocksDBMergedScanner.hpp` / `LeanStoreMergedScanner.hpp`
- `variant_utils.hpp` (toType function)

### Migration Strategy Options

- **(a)** Make tagged format the new default and update geo benchmark simultaneously
- **(b)** Parameterize format choice per merged index (template parameter or runtime flag)
- **(c)** Implement tagged format only for Q12's merged adapters as new classes

---

## New File Structure

```text
frontend/tpch/q12/
    q12_views.hpp              -- q12_result_t record (root pipeline MI entry), Q12 filter & projection functions
    q12_workload.hpp           -- Q12Workload class (load, query, maintain methods)
    q12_join_query.tpp         -- Query implementations: by_view, by_merged, by_base, by_hash
    q12_maintain.tpp           -- RF1/RF2 + view maintenance per Calcite maintenance plan
    executable_rocksdb.cpp     -- RocksDB entry point (macOS + Linux)
    executable_leanstore.cpp   -- LeanStore entry point (Linux only)
```

---

## Concrete Plan Context (files to read in detail)

| File | Why |
|------|-----|
| `frontend/geo/tpch_tables.hpp` | orders_t/lineitem_t field layouts, date types, generateRandomRecord — verify column indices for Calcite schema mapping |
| `frontend/geo/tpch_workload.hpp` | Loading patterns, scale factors, custom insert function hooks |
| `frontend/geo/views.hpp` | sort_key_t, SKBuilder, joined_t usage patterns — template for q12_views.hpp |
| `frontend/shared/view_templates.hpp` | joined_t and merged_t templates to reuse or simplify |
| `frontend/geo/workload.hpp` | GeoJoin class structure — template for Q12Workload |
| `frontend/geo/join_search_count.tpp` | BaseJoiner/MergedJoiner/HashJoiner patterns — template for q12_join_query.tpp |
| `frontend/geo/executable_rocksdb.cpp` | Adapter instantiation, storage_structure dispatch, load/run lifecycle |
| `frontend/geo/executable_helper.hpp` | ExecutableHelper, DBTraits, schedule_bg_txs, tput_tx patterns |
| `frontend/shared/merge-join/premerged_join.hpp` | PremergedJoin template params, JoinState, 1:N join handling |
| `frontend/shared/adapter-scanner/RocksDBMergedAdapter.hpp` | Merged adapter instantiation for 2 record types |
| `frontend/shared/adapter-scanner/RocksDBMergedScanner.hpp` | Variant return type, seekTyped, toType for 2-record case |
| `frontend/shared/adapter-scanner/LeanStoreMergedAdapter.hpp` | Merged adapter foldKey — must be updated for tagged row format |
| `frontend/shared/adapter-scanner/LeanStoreMergedScanner.hpp` | LeanStore merged scanner toType — must be updated for sourceId dispatch |
| `frontend/shared/variant_utils.hpp` | Current toType() using fold-length heuristic — must be replaced |
| Calcite `TaggedRowSchema.java` | Reference implementation for tagged row format |
| `frontend/CMakeLists.txt` | Build target patterns for q12_lsm / q12_btree |

---

## Potential Challenges

1. **Date encoding**: Current `lineitem_t::generateRandomRecord` uses `urand(1,10000)` for dates — not epoch-based. Need to calibrate so ~5-10% of lineitems pass the Q12 filter (dates in 1994 range, MAIL/SHIP shipmodes).

2. **Column index mapping**: Calcite plan references `$5` (o_orderpriority), `$19-$21` (lineitem dates), `$23` (l_shipmode) — offsets in concatenated (ORDERS ∥ LINEITEM) schema. Must verify against actual struct field order.

3. **PremergedJoin for 2 tables**: Existing usage is 5 tables (geo benchmark). Need to verify it works cleanly with just 2 record types (should be simpler — fewer variant alternatives, simpler join state).

4. **Concurrent RF1 atomicity**: Order + lineitems should be in one transaction so Branch 1 maintenance sees the order when processing lineitem deltas. Insert order first, then lineitems.

5. **Tagged row format migration**: Cross-cutting change affecting geo benchmark. Geo currently relies on fold-length discrimination. Must choose: (a) new default + update geo, (b) parameterize per index, or (c) Q12-only new classes.

---

## Completed Steps

- [x] Created CLAUDE.md with project overview and build instructions
- [x] macOS ARM (M1) build support for RocksDB-only frontend (commits `15218459`, `8aa57bc1`)
- [x] Renamed `rocksdb.cpp` → `rocksdb_typeinfo.cpp` to avoid macOS case collision
- [x] Created symlink `calcite-integration-info/test-plans` → Calcite test output
- [x] GRAND PLAN complete for TPC-H Q12 implementation
- [x] Updated CLAUDE.md with Q12 integration context (commit `66eee393`)

## Next Steps

### Immediate term (bug fix & small decisions)

None for now.

### Short-term (next session)

- [ ] Create `Q12.md` for a grand plan for Q12 specifically, which is detailed in the table below.
- [ ] Decide RF2 delete strategy: delete by orderkey or shipmode? Check TPC-H spec (§5, §6)

### Mid-term

Q12 implementation:

| Stage | Option 1+2 | Option 3 | Option 4+5 |
|-------|------------|----------|------------|
|Loading|required traditional indexes|final materialized view|index creation plans for merged indexes|
|Queries|Full queries|Retrieve from the final view|root pipeline plan which only involves the root merged index|
|Maintenance|required traditional indexes|final materialized view|maintenance plans for merged indexes|

- [ ] CMake integration: add q12_lsm and q12_btree targets
- [ ] Verification: build, smoke test, cross-validate query results across storage structures

Not specific to Q12:

- [ ] Tagged row format for merged adapters/scanners: decide migration strategy (a/b/c), modify `merged_t` and `toType()` (§Tagged Row Format)

### Side projects

- [ ] Decide storage structure variants: should options 1-2 (trad indexes) use non-MI-optimized plans? (§4)
- [ ] Flatten `joined_t` projection: store flattened fields instead of tuple of input records (§View Templates)
- [ ] Naming convention: rename JK → SK consistently (sort key, not join key) (§View Templates)
- [ ] Investigate `JoinState` overhead in hash join: previously implemented without it (§Join Infrastructure)
- [ ] LeanStore background insert bugs: records shifting pages causes issues; RocksDB unaffected (§Executable Framework)

### Long-term (no implementation planned but important to keep in mind)

None for now.