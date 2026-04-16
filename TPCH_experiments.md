# TPC-H Experiment Plan (Current Paper)

## Scope

This document tracks experiments for the **current VLDB revision** — complementing the existing geo benchmark with standard TPC-H queries.

**Key constraint**: Like `frontend/geo/`, each query uses at most **one merged index** that interleaves the source tables of a single pipeline. This is NOT the multi-MI pipeline cascade approach (that is next paper scope; see `calcite-integration-info/test-plans/` and `frontend/tpch/q12/CLAUDE.md` for that future work).

The pattern follows geo exactly:

- **Storage structure 1**: Traditional indexes (separate per-table B-trees) + join at query time
- **Storage structure 2**: Materialized view storing the pipeline's output
- **Storage structure 3**: Single merged index interleaving the pipeline's source tables + join via PremergedJoin at query time

## Overall Goal

Replace the handcrafted geo-benchmark queries with standard TPC-H queries to demonstrate the generality of merged indexes. This directly addresses VLDB reviewer critiques (Reviewers 1, 2, 3) demanding recognized benchmarks. For each query, we compare traditional indexes, materialized views, and a single merged index — showing where MI helps, where it matches materialized views, and where it loses.

The secondary goal is honest evaluation. Reviewer 2 (D2) specifically wants to see cases where merged indexes are NOT clearly beneficial, so our query selection must include at least one such case.

## What Calcite Must Provide: `int-ord-plans/`

For each TPC-H query, Calcite must identify **which one pipeline** is the best candidate for a single merged index. This is delivered via `calcite-integration-info/int-ord-plans/` (does not exist yet).

### What a Single-Pipeline MI Plan Looks Like

For each query, Calcite picks the one sub-join (pipeline) where interleaving source tables in a merged index is most beneficial. The plan must specify:

1. **Which tables** go into the merged index (the pipeline's source tables)
2. **What join key** orders the interleaving (the shared prefix key)
3. **The query plan** that uses this MI — a PremergedJoin scan over the MI, followed by any remaining joins with tables outside the MI, then filters and aggregation
4. **The maintenance plan** — how to propagate inserts/deletes to the MI when base tables change

### Concrete Example: Geo Benchmark (Reference)

The geo benchmark's single MI interleaves 5 tables ordered by a geographic key hierarchy:

```text
MI = MergedIndex(nation, states, county, city, customer)
Key hierarchy: (nationkey, statekey, countykey, citykey, custkey)
```

- Each table's key is a prefix of the next table's key
- PremergedJoin scans the MI once, assembling join results in a single pass
- The materialized view (structure 2) stores the full 5-way join output in a separate index

For TPC-H, each query's MI will follow the same pattern — just with different tables and key hierarchies.

### Expected Directory Structure

```text
calcite-integration-info/int-ord-plans/
  q12/
    plan.dot           -- The single-MI query plan
    maintenance.dot    -- Maintenance plan for the MI
    README.md          -- Which pipeline was selected, why, what tables/keys
  q3/
    ...
  q5/
    ...
  q9/
    ...
```

### Per-Query Status

| Query   | `int-ord-plans/` | Status                                                     |
| ------- | ----------------- | ---------------------------------------------------------- |
| **Q12** | ❌ Not yet        | Need: which pipeline, which tables in the single MI        |
| **Q3**  | ❌ Not yet        | Need: likely ORDERS+LINEITEM by orderkey, but Calcite must confirm |
| **Q5**  | ❌ Not yet        | Need: pipeline selection for 6-table star join             |
| **Q9**  | ❌ Not yet        | Need: pipeline selection for 6-table chain                 |
| **Q7**  | ❌ Not yet        | May show MI is not helpful (honest evaluation)             |

### What LeanStore Can Do Without `int-ord-plans/`

Even before Calcite delivers plans, we can implement:

- **Traditional index baselines** (storage_structure=1) for all queries — these don't need a Calcite plan since they use standard hash/merge joins over separate per-table indexes
- **Operator framework** in `frontend/shared/` — reusable join, scan, filter, aggregate operators
- **Ad-hoc experiments** — selectivity sweeps, single-table scan overhead (reuse existing geo MI infrastructure)

### What Is Blocked

- **MI variants** (storage_structure=3) for all queries — cannot pick tables/keys without Calcite's pipeline selection
- **Materialized view variants** (storage_structure=2) — the view must store the output of the same pipeline Calcite selects for MI, so it depends on the same plan

## Query Selection and Priority

### Tier 1: Must Implement

| Query    | Tables Involved                                    | Why Selected                                              |
| -------- | -------------------------------------------------- | --------------------------------------------------------- |
| **Q12**  | ORDERS, LINEITEM (2)                               | Simplest case; 1 join, 1 filter, 1 aggregate              |
| **Q3**   | CUSTOMER, ORDERS, LINEITEM (3)                     | Reviewer 3 W3 mentions Q3; ORDER BY + LIMIT               |
| **Q9**   | PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION (6) | Most complex TPC-H join; tests MI on a sub-join of 6 tables |

### Tier 2: Should Implement (one or two)

| Query    | Tables Involved                                          | Why Consider                                                                  |
| -------- | -------------------------------------------------------- | ----------------------------------------------------------------------------- |
| **Q5**   | CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION (6) | Reviewer 3 W3 mentions Q5; star-join topology                                |
| **Q10**  | CUSTOMER, ORDERS, LINEITEM, NATION (4)                   | Medium complexity; customer-centric aggregation                               |
| **Q7**   | SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION x2 (5+)    | Self-join on NATION; potential "MI not helpful" case for Reviewer 2 D2        |

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

| # | Strategy                    | Description                                               |
| - | --------------------------- | --------------------------------------------------------- |
| 1 | Traditional indexes         | Separate ORDERS + LINEITEM indexes; hash or merge join    |
| 2 | Materialized view           | Pre-joined + pre-filtered result stored in one index      |
| 3 | Single merged index         | MI(ORDERS, LINEITEM) by orderkey; PremergedJoin at query time |

### Q3: CUSTOMER x ORDERS x LINEITEM

**Likely MI**: `MergedIndex(ORDERS, LINEITEM)` by `orderkey`

- Same MI as Q12 — the ORDERS-LINEITEM join is the most scan-intensive pipeline
- CUSTOMER joined at query time (small table, indexed lookup by custkey)
- Final: ORDER BY revenue DESC, o_orderdate + LIMIT 10

| # | Strategy                    | Description                                               |
| - | --------------------------- | --------------------------------------------------------- |
| 1 | Traditional indexes         | Hash join LINEITEM x ORDERS, then hash join with CUSTOMER |
| 2 | Materialized view           | Pre-joined 3-way result stored and pre-sorted             |
| 3 | Single merged index         | MI(ORDERS, LINEITEM) by orderkey; join CUSTOMER at query time |

### Q9: 6-Table Profit Query

**Likely MI**: `MergedIndex(ORDERS, LINEITEM)` by `orderkey` (largest join)

- The ORDERS-LINEITEM join dominates I/O (LINEITEM is the largest TPC-H table)
- Remaining joins (PART, SUPPLIER, PARTSUPP, NATION) done at query time via hash joins
- Final: GROUP BY nation, year; SUM profit

| # | Strategy                    | Description                                               |
| - | --------------------------- | --------------------------------------------------------- |
| 1 | Traditional indexes         | 5 hash joins over separate indexes                        |
| 2 | Materialized view           | Full 6-way join materialized                              |
| 3 | Single merged index         | MI(ORDERS, LINEITEM) by orderkey; 4 remaining hash joins  |

### Q5 or Q7 (Tier 2 — TBD)

Blocked on Calcite plan generation and query selection decision.

## Storage Structure Convention

Matching the geo benchmark exactly:

| Value | Meaning                         |
| ----- | ------------------------------- |
| 0     | Force data reload (no recovery) |
| 1     | Traditional indexes + join      |
| 2     | Materialized view               |
| 3     | Single merged index             |

This is simpler than the multi-MI scheme in `frontend/tpch/q12/CLAUDE.md` (which uses 1-5). That scheme is for the next paper.

## Experiment Infrastructure

### Makefile and Targets

Experiments are driven by `generate_targets.py`, which emits Makefile rules into `targets.mk`. To add TPC-H queries:

1. Add `q12_lsm`, `q12_btree` (etc.) to `exec_names` in `generate_targets.py`
2. Define `STRUCTURE_OPTIONS` for each (e.g., `"q12_lsm": [1, 2, 3]`)
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

| Concern                              | Experiment                                                      |
| ------------------------------------ | --------------------------------------------------------------- |
| "Evaluate on standard benchmarks"    | Q12, Q3, Q9 with all 3 storage structure variants               |
| "Show generality beyond geo queries" | Complexity gradient: 2-table, 3-table, 6-table                  |

### Reviewer 2: Honest Trade-offs

| Concern                   | Experiment                                                             |
| ------------------------- | ---------------------------------------------------------------------- |
| D2: "Show where MI loses" | Q7 (non-hierarchical join graph) or large-table MI with high skew     |
| "Maintenance overhead"    | Insert/delete latency: MI vs. materialized view vs. traditional        |
| "Space overhead"          | Storage size comparison across all 3 structures                        |

### Reviewer 3: Specific Experiments

| Concern                                  | Experiment                                                                                                             |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| W2/D5: Larger scan ranges                | Vary Q12 date filter: 1-month, 1-year, 3-year windows                                                                 |
| W3/D6: Single-table scan overhead in MI  | Measure LINEITEM-only scan in standalone index vs. MI with ORDERS interleaved                                          |
| W3/D6: Join order comparison             | For Q3/Q5: compare plans with vs. without MI (requires int-ord-plans)                                                  |
| D3-D4: MI vs. materialized views         | Every query has both MI (structure 3) and view (structure 2); compare latency, maintenance, space                      |
| "B-tree vs LSM deeper analysis"          | Run all queries on both `_btree` and `_lsm` executables                                                               |

### Ad-Hoc Experiments (Reviewer 3)

These do not require full query implementations:

1. **Scan selectivity sweep**: For Q12, vary the date filter window from 1 month to 5 years. Plot query latency vs. selectivity for each storage structure.

2. **Single-table scan overhead**: Load LINEITEM into (a) standalone index, (b) MI with ORDERS interleaved. Measure full-scan time for LINEITEM only. The overhead comes from skipping ORDERS records during scan.

3. **MI density analysis**: For each query's MI, measure what fraction of records belong to each table type. High skew means single-table scans pay high overhead.

## Implementation Order

1. **Traditional baselines** for Q12, Q3, Q9 — storage_structure=1, no Calcite dependency
2. **Q12 MI + view** — storage_structure=2,3 (pending int-ord-plans, or use obvious ORDERS+LINEITEM MI)
3. **Ad-hoc experiments** — selectivity sweep, single-table scan overhead, using Q12 MI
4. **Q3 MI + view** — storage_structure=2,3 (pending int-ord-plans)
5. **Q9 MI + view** — storage_structure=2,3 (pending int-ord-plans)
6. **Tier 2 query** (Q7 or Q5) — for honest evaluation

## Relationship to Next Paper

The existing files in `calcite-integration-info/test-plans/` and `frontend/tpch/q12/CLAUDE.md` describe a **multi-MI pipeline cascade** approach where each query can have multiple merged indexes (e.g., Q12 has 2 MIs, Q9 has 5 MIs). That is **next paper scope**.

For the current paper, we use only the single-MI approach (like geo/) and need `int-ord-plans/` from Calcite to tell us which one pipeline to target per query.
