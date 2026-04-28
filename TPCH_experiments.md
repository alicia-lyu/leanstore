# TPC-H Experiment Plan (Current Paper)

## Scope

This document tracks experiments for the **current VLDB revision** — complementing the existing geo benchmark with standard TPC-H queries.

**Key constraint**: Like `frontend/geo/`, each query uses at most **one merged index** that interleaves the source tables of a single pipeline. This is NOT the multi-MI pipeline cascade approach (that is next paper scope; see `calcite-integration-info/test-plans/` and `frontend/tpch/q12/CLAUDE.md` for that future work).

The pattern follows geo exactly:

- **Storage structure 1**: Traditional indexes (separate per-table B-trees) + merge join at query time
- **Storage structure 2**: Materialized view storing the pipeline's join output (intermediate view — the same pipeline whose sources the MI interleaves)
- **Storage structure 3**: Single merged index interleaving the pipeline's source tables + join via PremergedJoin at query time
- **Storage structure 4**: Clustered indexes + hash join at query time

## Overall Goal

Complement the handcrafted geo-benchmark queries with standard TPC-H queries to demonstrate the generality of merged indexes. This directly addresses VLDB reviewer critiques (Reviewers 1, 2, 3) demanding recognized benchmarks. For each query, we compare traditional indexes, materialized views, and a single merged index — showing where MI helps, where it matches materialized views, and where it loses.

The secondary goal is honest evaluation. Reviewer 2 (D2) specifically wants to see cases where merged indexes are NOT clearly beneficial, so our query selection must include at least one such case.

## Query Plan Sources

Each storage structure has a corresponding query plan in DOT (Graphviz) format, stored in `frontend/tpch/<query>/plans/`. These plans guide the C++ implementation — they specify the operator tree, join order, and access patterns for each structure.

### Plan ↔ Storage Structure Mapping

| Structure                   | Plan Source                                       | File                       |
| --------------------------- | ------------------------------------------------- | -------------------------- |
| 1 (traditional + merge join) | Interesting-ordering optimized plan from Calcite  | `structure_1.dot`          |
| 2 (materialized view)       | Default Calcite plan (hash joins, no int. order.) | `structure_2_4_default.dot` |
| 3 (single merged index)     | Adapted from structure 1 with MI substitution     | `structure_3_draft.dot`    |
| 4 (clustered + hash join)   | Same default plan as structure 2                  | `structure_2_4_default.dot` |

### Plan Sources in Detail

**Structure 1** plans come from `calcite-integration-info/test-plans/<query>/before-pipeline.dot`. Despite the name "before-pipeline," these ARE interesting-ordering optimized — they use `EnumerableMergeJoin` with explicit `EnumerableSort` nodes to maintain sort order through the join chain. The Calcite optimizer produces these when configured with merge-join rules and interesting-ordering heuristics. Available for Q12, Q3, Q9.

**Structure 2 & 4** plans are default Calcite plans using `EnumerableHashJoin` — no sort operators before joins, no interesting-ordering optimization. These represent what a standard SQL optimizer would produce. Structure 2 materializes the plan's output as a view; structure 4 executes it at query time with hash joins over clustered indexes.

**Structure 3** plans adapt the structure 1 plan by replacing the innermost sort+merge-join pipeline (ORDERS ⋈ LINEITEM on orderkey) with an `EnumerablePremergedJoin` over a merged index. All other operators remain unchanged. These are drafts pending user review — the user will finalize which pipeline to target and confirm the MI composition.

### Per-Query Plan Status

| Query   | Structure 1 | Structure 2 & 4 | Structure 3        |
| ------- | ----------- | ---------------- | ------------------ |
| **Q12** | Done        | Done             | Draft, needs review |
| **Q3**  | Done        | Done             | Draft, needs review |
| **Q9**  | Done        | Done             | Draft, needs review |
| **Q5**  | Not yet     | Not yet          | Not yet            |
| **Q7**  | Not yet     | Not yet          | Not yet            |

### Directory Layout

```text
frontend/tpch/
  tpch_tables.hpp              -- TPC-H record types + spec-accurate generators
  tpch_workload.hpp            -- TPCHWorkload<AdapterType> loader
  q12/plans/
    structure_1.dot            -- interesting-ordering (merge join)
    structure_2_4_default.dot  -- default (hash join)
    structure_3_draft.dot      -- MI-adapted (pending review)
  q3/plans/
    ...
  q9/plans/
    ...
```

### Shared TPC-H Infrastructure

`frontend/tpch/tpch_tables.hpp` and `frontend/tpch/tpch_workload.hpp` are the canonical homes of the TPC-H record types and workload loader used by every Tier 1 query. The old `frontend/geo/tpch_tables.hpp` and `frontend/geo/tpch_workload.hpp` are now thin `#include` wrappers preserved so the geo benchmark continues to build; loggers (`frontend/shared/logger/*`) and `generate_targets.py` reference the new canonical paths.

**This is a substantive rewrite of data generation, not a directory move.** Running geo and TPC-H experiments at the same scale factor will now produce different distributions because the new code follows TPC-H spec §4.2.3 domains, while the old geo loader used free-form random strings and a derived lineitems-per-order ratio. Struct field layouts, types, key structs, table `id` values (0–7), and `print()` are unchanged, so consumer code keeps compiling.

`tpch_tables.hpp` improvements over the old geo version (with rationale tying back to the Tier 1 queries):

- **Spec §4.2.3 domain arrays**: `TPCH_COLORS[92]`, `TPCH_TYPES_S{1,2,3}`, `TPCH_CONTAINERS_S{1,2}`, `TPCH_SEGMENTS[5]`, `TPCH_PRIORITIES[5]`, `TPCH_MODES[7]`, `TPCH_INSTRUCTIONS[4]`. Required so Q12's `l_shipmode IN ('MAIL','SHIP')`, Q9's `p_name LIKE '%green%'`, and Q3's `c_mktsegment = 'BUILDING'` actually match a meaningful fraction of the rows. With the old random strings these filters returned ~0 rows.
- **`p_name` built from 5 distinct color words** — direct Q9 dependency.
- **`lineitem_t::generateRandomRecord(partkey_fn, suppkey_fn, o_orderdate)`** — ship/commit/receipt dates and `l_returnflag`/`l_linestatus` are derived from the order's date so Q12's date-window filter and Q9's `extract(year from o_orderdate)` aggregation produce realistic selectivities.
- **`part_t::generateRandomRecord(Integer partkey)`** takes the partkey so `p_retailprice` follows the spec formula; a backward-compat overload is kept for the geo benchmark.
- **TPC-H date constants** — `TPCH_STARTDATE` = 1992-01-01, `TPCH_CURRENTDATE` = 1995-06-17, `TPCH_ENDDATE` = 1998-12-31, `TPCH_ORDERS_ENDDATE` = 1998-08-02 (as days since epoch), replacing the old `urand(1, 10000)` date generator.
- **Hardcoded `NATIONS[25]` / `REGIONS[5]`** with `fromData()` factories — Q9 groups by nation name, so we need exactly the 25 spec-defined names rather than random strings.

`tpch_workload.hpp` improvements over the old geo loader:

- **Sparse `o_orderkey`** (first 8 of every 32 keys populated) via `orderkey_from_index()` — spec §4.2.3, prevents key-density artifacts in MI scans.
- **`urand(1, 7)` lineitems per order** replacing the `LINEITEM_SCALE / ORDERS_SCALE` derived ratio — produces the realistic 1:N MI pattern PremergedJoin is designed for.
- **`o_custkey % 3 != 0` constraint** — leaves 1/3 of customers without orders, needed for honest Q3 selectivity.
- **`order_dates` (`unordered_map<Integer, Timestamp>`)** propagates each order's date to its lineitems' date generation.
- **Exactly 4 PARTSUPP rows per part** (was random) — keeps Q9's `partsupp` join cardinality on-spec.
- **0-indexed `getNationID` / `getRegionID`**; `loadNation()` / `loadRegion()` rewritten to populate from the hardcoded arrays.

### Geo Benchmark Reference

The geo benchmark's single MI interleaves 5 tables ordered by a geographic key hierarchy:

```text
MI = MergedIndex(nation, states, county, city, customer)
Key hierarchy: (nationkey, statekey, countykey, citykey, custkey)
```

- Each table's key is a prefix of the next table's key
- PremergedJoin scans the MI once, assembling join results in a single pass
- The materialized view (structure 2) stores the full 5-way join output in a separate index

For TPC-H, each query's MI follows the same pattern — just with different tables and key hierarchies. The likely MI for Q12, Q3, Q9 is `MergedIndex(ORDERS, LINEITEM)` by `orderkey`.

## Materialized View: Intermediate Pipeline View, Not Final Query Result

Per the paper's spectrum of pre-computation (§\ref{subsec:spec-precomp}), the most comparable alternative to a merged index is the **intermediate pipeline view** — the materialized output of the same pipeline whose sources the MI interleaves. For all Tier 1 queries, this pipeline is ORDERS ⋈ LINEITEM, so structure 2 stores their join output (all columns, unfiltered, unaggregated).

This is deliberate: the MI and the intermediate view represent the **same point in the query plan** but differ in pre-computation strategy. The MI stores the sources co-located and pre-sorted; the view stores the fully computed join output. At query time, the MI must assemble join results (PremergedJoin scan); the view skips that work but pays more at update time (1-to-N maintenance).

**Why not the final query result view?** A final view (e.g., Q12 aggregated by shipmode with filters applied) would trivially beat any query-time strategy — there's nothing left to compute. But its update cost is high and its reusability is zero (different filter parameters require a different view). Comparing MI against a final view requires a **hybrid workload** of interleaved queries and updates, which is not native to TPC-H. We may design such a workload separately, but it is not the primary comparison.

**What structure 2 stores per query:**

| Query | Pipeline | View Contents |
|-------|----------|---------------|
| Q12   | ORDERS ⋈ LINEITEM | All ORDERS + LINEITEM columns per joined row, keyed for the query's downstream access pattern |
| Q3    | ORDERS ⋈ LINEITEM | Same join output; CUSTOMER join + filter + aggregate + ORDER BY at query time |
| Q9    | ORDERS ⋈ LINEITEM | Same join output; PART, PARTSUPP, SUPPLIER, NATION joins + filter + aggregate at query time |

## Query Selection and Priority

### Tier 1: Must Implement

| Query   | Tables Involved                                        | Why Selected                                                |
| ------- | ------------------------------------------------------ | ----------------------------------------------------------- |
| **Q12** | ORDERS, LINEITEM (2)                                   | Simplest case; 1 join, 1 filter, 1 aggregate                |
| **Q3**  | CUSTOMER, ORDERS, LINEITEM (3)                         | Reviewer 3 W3 mentions Q3; ORDER BY + LIMIT                 |
| **Q9**  | PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION (6) | Most complex TPC-H join; tests MI on a sub-join of 6 tables |

### Tier 2: Should Implement (one or two)

| Query   | Tables Involved                                          | Why Consider                                                           |
| ------- | -------------------------------------------------------- | ---------------------------------------------------------------------- |
| **Q5**  | CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION (6) | Reviewer 3 W3 mentions Q5; star-join topology                          |
| **Q10** | CUSTOMER, ORDERS, LINEITEM, NATION (4)                   | Medium complexity; customer-centric aggregation                        |
| **Q7**  | SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION x2 (5+)     | Self-join on NATION; potential "MI not helpful" case for Reviewer 2 D2 |

### Selection Rationale

The Tier 1 queries form a complexity gradient:

- **Q12** (2 tables) — validates the basic single-MI architecture on TPC-H
- **Q3** (3 tables) — the MI covers 2 of 3 tables; 1 remaining join at query time
- **Q9** (6 tables) — the MI covers 2-3 tables; 3-4 remaining joins at query time

One Tier 2 query should be chosen where MI provides marginal or no benefit. Q7 is the best candidate because the self-join on NATION creates a non-hierarchical join graph.

## Per-Query Sketches

These are preliminary — the actual MI composition depends on Calcite's `int-ord-plans/`.

### Q12: ORDERS x LINEITEM

**Likely MI**: `MergedIndex(ORDERS, LINEITEM)` by `orderkey`

- ORDERS key: `(orderkey)`, LINEITEM key: `(orderkey, linenumber)`
- Natural prefix hierarchy — LINEITEM extends ORDERS key
- PremergedJoin produces joined rows; filter on shipmode + dates; aggregate by shipmode

**Storage structures**:

| # | Strategy            | Description                                                   |
| - | ------------------- | ------------------------------------------------------------- |
| 1 | Traditional + merge | Separate ORDERS + LINEITEM indexes; merge join on orderkey    |
| 2 | Materialized view   | ORDERS ⋈ LINEITEM join output stored (intermediate pipeline view) |
| 3 | Single merged index | MI(ORDERS, LINEITEM) by orderkey; PremergedJoin at query time     |
| 4 | Clustered + hash    | Clustered indexes; hash join at query time                        |

### Q3: CUSTOMER x ORDERS x LINEITEM

**Likely MI**: `MergedIndex(ORDERS, LINEITEM)` by `orderkey`

- Same MI as Q12 — the ORDERS-LINEITEM join is the most scan-intensive pipeline
- CUSTOMER joined at query time (small table, indexed lookup by custkey)
- Final: ORDER BY revenue DESC, o_orderdate + LIMIT 10

| # | Strategy            | Description                                                             |
| - | ------------------- | ----------------------------------------------------------------------- |
| 1 | Traditional + merge | Merge join chain on orderkey; then merge join with CUSTOMER             |
| 2 | Materialized view   | ORDERS ⋈ LINEITEM join output stored; CUSTOMER join + rest at query time |
| 3 | Single merged index | MI(ORDERS, LINEITEM) by orderkey; join CUSTOMER at query time           |
| 4 | Clustered + hash    | Clustered indexes; hash join chain at query time                        |

### Q9: 6-Table Profit Query

**Likely MI**: `MergedIndex(ORDERS, LINEITEM)` by `orderkey` (largest join)

- The ORDERS-LINEITEM join dominates I/O (LINEITEM is the largest TPC-H table)
- Remaining joins (PART, SUPPLIER, PARTSUPP, NATION) done at query time via hash joins
- Final: GROUP BY nation, year; SUM profit

| # | Strategy            | Description                                                                       |
| - | ------------------- | --------------------------------------------------------------------------------- |
| 1 | Traditional + merge | Merge join chain over separate indexes sorted on join keys                        |
| 2 | Materialized view   | ORDERS ⋈ LINEITEM join output stored; 4 remaining joins + rest at query time       |
| 3 | Single merged index | MI(ORDERS, LINEITEM) by orderkey; 4 remaining merge/hash joins at query time      |
| 4 | Clustered + hash    | Clustered indexes; 5 hash joins at query time                                     |

### Q5 or Q7 (Tier 2 — TBD)

Requires generating plans (structure 1 from Calcite interesting-ordering, structures 2/4 from default Calcite, structure 3 adapted).

## Storage Structure Convention

Matching the geo benchmark exactly:

| Value | Meaning                                |
| ----- | -------------------------------------- |
| 0     | Force data reload (no recovery)        |
| 1     | Traditional indexes + merge join       |
| 2     | Materialized view                      |
| 3     | Single merged index + PremergedJoin    |
| 4     | Clustered indexes + hash join          |

This is simpler than the multi-MI scheme in `frontend/tpch/q12/CLAUDE.md` (which uses 1-5). That scheme is for the next paper.

## Experiment Infrastructure

### Makefile and Targets

Experiments are driven by `generate_targets.py`, which emits Makefile rules into `targets.mk`. To add TPC-H queries:

1. Add `q12_lsm`, `q12_btree` (etc.) to `exec_names` in `generate_targets.py`
2. Define `STRUCTURE_OPTIONS` for each (e.g., `"q12_lsm": [1, 2, 3, 4]`)
3. Add `DIFF_DIRS` mappings (e.g., `"q12_lsm": "tpch/q12"`)
4. Run `python3 generate_targets.py > targets.mk` to regenerate

### CSV Output

Experiments output CSV files to `build/<exec>/<scale>-in-<dram>/`. Metrics collected (from geo benchmark pattern):

- Query latency (per-query and aggregate)
- Throughput (queries/sec)
- Maintenance cost (updates/sec)
- Storage size (bytes per structure)
- Scan count / IO statistics

### Scale Factors

- Development/debugging: scale=1 (tiny, seconds to load)
- Unit testing: scale=15 (default, minutes)
- Paper experiments: scale=40+ (LSM), scale=15+ (B-tree, limited by DRAM)

### Running Experiments

```bash
# Build and run all Q12 variants on LSM
make q12_lsm scale=15

# Run a single storage structure
make q12_lsm_3 dram=0.1

# Debug with LLDB
make q12_btree_lldb_3
```

## Reviewer Critique Mapping

### Reviewer 1: More TPC-H

| Concern                              | Experiment                                        |
| ------------------------------------ | ------------------------------------------------- |
| "Evaluate on standard benchmarks"    | Q12, Q3, Q9 with all 4 storage structure variants |
| "Show generality beyond geo queries" | Complexity gradient: 2-table, 3-table, 6-table    |

### Reviewer 2: Honest Trade-offs

| Concern                   | Experiment                                                        |
| ------------------------- | ----------------------------------------------------------------- |
| D2: "Show where MI loses" | Q7 (non-hierarchical join graph) or large-table MI with high skew |
| "Maintenance overhead"    | Insert/delete latency: MI vs. materialized view vs. traditional   |
| "Space overhead"          | Storage size comparison across all 4 structures                   |

### Reviewer 3: Specific Experiments

| Concern                                 | Experiment                                                                                        |
| --------------------------------------- | ------------------------------------------------------------------------------------------------- |
| W2/D5: Larger scan ranges               | Vary Q12 date filter: 1-month, 1-year, 3-year windows                                             |
| W3/D6: Single-table scan overhead in MI | Measure LINEITEM-only scan in standalone index vs. MI with ORDERS interleaved                     |
| W3/D6: Join order comparison            | For Q3/Q5: compare plans with vs. without MI (requires int-ord-plans)                             |
| D3-D4: MI vs. materialized views        | Every query has MI (3), view (2), and traditional (1,4); compare latency, maintenance, space      |
| "B-tree vs LSM deeper analysis"         | Run all queries on both `_btree` and `_lsm` executables                                           |

### Ad-Hoc Experiments (Reviewer 3)

These do not require full query implementations:

1. **Scan selectivity sweep**: For Q12, vary the date filter window from 1 month to 5 years. Plot query latency vs. selectivity for each storage structure.

2. **Single-table scan overhead**: Load LINEITEM into (a) standalone index, (b) MI with ORDERS interleaved. Measure full-scan time for LINEITEM only. The overhead comes from skipping ORDERS records during scan.

3. **MI density analysis**: For each query's MI, measure what fraction of records belong to each table type. High skew means single-table scans pay high overhead.

## Implementation Order

1. **Q12 all structures** — plans ready for all 4 structures; implement C++ execution code
2. **Q3 all structures** — plans ready; implement following Q12 patterns
3. **Ad-hoc experiments** — selectivity sweep, single-table scan overhead, using Q12 MI
4. **Q9 all structures** — plans ready; most complex (6 tables, 4 remaining joins in structure 3)
5. **Tier 2 query** (Q7 or Q5) — for honest evaluation; requires generating new plans

## Relationship to Next Paper

The existing files in `calcite-integration-info/test-plans/` (other than `before-pipeline.dot`) and `frontend/tpch/q12/CLAUDE.md` describe a **multi-MI pipeline cascade** approach where each query can have multiple merged indexes (e.g., Q12 has 2 MIs, Q9 has 5 MIs). That is **next paper scope**.

For the current paper, we use only the single-MI approach (like geo/). Plans for each storage structure live in `frontend/tpch/<query>/plans/`.
