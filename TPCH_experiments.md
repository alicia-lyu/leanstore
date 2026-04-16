# TPC-H Experiment Plan

## Overall Goal

Replace the handcrafted geo-benchmark queries with standard TPC-H queries to demonstrate the generality of merged indexes. This directly addresses VLDB reviewer critiques (Reviewers 1, 2, 3) demanding recognized benchmarks. For each query, we implement multiple storage structure variants — traditional indexes, materialized views, and merged indexes — to show where merged indexes help, where they match materialized views, and where they lose.

The secondary goal is honest evaluation. Reviewer 2 (D2) specifically wants to see cases where merged indexes are NOT clearly beneficial, so our query selection must include at least one such case.

## Query Selection and Priority

### Tier 1: Must Implement

| Query | Tables | Why Selected | Calcite Plan Status |
|-------|--------|-------------|-------------------|
| **Q12** | ORDERS, LINEITEM (2) | Proof-of-concept; simplest interesting case. 1 join, 1 filter, 1 aggregate. | Complete: 3 plans in `calcite-integration-info/test-plans/q12/` |
| **Q3** | CUSTOMER, ORDERS, LINEITEM (3) | Reviewer 3 W3 explicitly mentions Q3. 3-table join with ORDER BY + LIMIT. Tests whether MI helps a moderately complex join chain. | Complete: plans in `calcite-integration-info/test-plans/q3ol/` |
| **Q9** | PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION (6) | Most complex TPC-H join (6 tables). 5 non-root pipelines in Calcite plan. Demonstrates scaling of pipeline decomposition. | Complete: plans in `calcite-integration-info/test-plans/q9/` |

### Tier 2: Should Implement (one or two)

| Query | Tables | Why Consider | Calcite Plan Status |
|-------|--------|-------------|-------------------|
| **Q5** | CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION (6) | Reviewer 3 W3 mentions Q5. Star-join pattern around LINEITEM. Similar complexity to Q9 but different join topology. | Not yet generated |
| **Q10** | CUSTOMER, ORDERS, LINEITEM, NATION (4) | Medium complexity. Customer-centric aggregation. Good MI candidate (custkey chain). | Not yet generated |
| **Q7** | SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION x2 (5+) | Two-country shipping query. Self-join on NATION makes prefix compatibility unclear — potential "MI not helpful" case for honest evaluation. | Not yet generated |

### Tier 3: Consider Later

Q8, Q21 — complex multi-way joins that exercise the framework but add diminishing returns for the paper narrative.

### Selection Rationale

The three Tier 1 queries form a complexity gradient:

- **Q12** (2 tables, 1 pipeline) — validates the basic architecture
- **Q3** (3 tables, 2 pipelines) — tests multi-pipeline coordination with LIMIT
- **Q9** (6 tables, 5 pipelines) — stress-tests cascading pipeline maintenance

One Tier 2 query should be chosen where MI provides marginal or no benefit, to honestly address Reviewer 2 D2. Q7 is the best candidate because the self-join on NATION creates a non-hierarchical join graph that does not decompose cleanly into prefix-compatible pipelines.

## Per-Query Plans

### Q12: ORDERS x LINEITEM

**Status**: Design complete (see `frontend/tpch/q12/CLAUDE.md`). Implementation in progress.

**Pipeline decomposition** (from Calcite):

```
Pipeline 0 (leaf):  MI[0] = MergedIndex(ORDERS, LINEITEM) by orderkey
Pipeline 1 (root):  Root MI = projected join results keyed by (l_shipmode, o_orderkey, l_linenumber)
```

**Storage structure variants**: 5

| # | Strategy | Notes |
|---|----------|-------|
| 1 | Traditional indexes + hash join | Full table scans, build hash table on orders |
| 2 | Traditional indexes + merge join | Both tables sorted by orderkey |
| 3 | Materialized view | Pre-aggregated: only 2 rows (one per qualifying shipmode) |
| 4 | MI[0] only | PremergedJoin at query time + filter + aggregate |
| 5 | MI[0] + root MI | Scan root MI + filter + sorted aggregate (the Calcite plan) |

**Key challenge**: Predicate hoisting — root MI stores unfiltered results so it can serve multiple date range queries. Filter applied at query time on options 4 and 5.

### Q3: CUSTOMER x ORDERS x LINEITEM

**Status**: Calcite plans available. Design needed.

**Pipeline decomposition** (from Calcite plans in `q3ol/`):

```
Pipeline 0 (leaf):
  Leaf MI[0] = MergedIndex(LINEITEM, ORDERS) by orderkey
  Index creation: MergedScan MI[0] -> SortedAggregate(group by orderkey, SUM revenue)
                  -> MergeJoin with ORDERS on orderkey

  Leaf MI[1] = same structure, used for the branch pipeline below

Pipeline 1 (branch):
  Branch MI = MergedIndex(view[0], CUSTOMER) by custkey
  Index creation: MergedScan MI on custkey -> MergeJoin(view[0], CUSTOMER)
                  -> Project(l_orderkey, revenue, o_orderdate, o_shippriority)

Pipeline 2 (root):
  Query: MergedIndexScan(branch MI) -> Limit(10) after Sort(revenue DESC, o_orderdate ASC)
```

**Observation from Calcite plans**: The leaf index creation plans (`leaf-1`, `leaf-2`) are identical — both scan MI[0] with LINEITEM sorted-aggregate joined to ORDERS. The branch plan (`branch-1`) joins the leaf view with CUSTOMER via a merged index keyed on custkey. The root query plan is a simple `MergedIndexScan -> Limit(10)`.

**Storage structure variants**: 5

| # | Strategy | Notes |
|---|----------|-------|
| 1 | Traditional indexes + hash join | Hash join LINEITEM x ORDERS, then hash join with CUSTOMER |
| 2 | Traditional indexes + merge join | Merge join chain |
| 3 | Materialized view | Pre-computed top-10 or full grouped result |
| 4 | Leaf MI only | MI(ORDERS, LINEITEM) by orderkey, join CUSTOMER at query time |
| 5 | Leaf MI + branch MI | Full Calcite plan: two levels of merged indexes |

**Key challenges**:

- ORDER BY revenue DESC, o_orderdate ASC + LIMIT 10 means the root pipeline needs a sort or the MI must be keyed to produce results in the right order. Calcite's plan keys the root MI on `[revenue DESC, o_orderdate]` so the scan produces sorted output directly.
- The sorted aggregate in the leaf pipeline (grouping LINEITEM by orderkey, summing revenue) is an interesting operator to implement — it sits between the merged scan and the merge join.

### Q9: 6-Table Profit Query

**Status**: Calcite plans available. Design needed. Most complex query.

**Pipeline decomposition** (from Calcite plans in `q9/`):

```
Pipeline 0 (leaf):
  MI[0] = MergedIndex(ORDERS, LINEITEM) by orderkey
  Index creation: MergedScan MI[0] -> MergeJoin(ORDERS, LINEITEM)

Pipeline 1 (branch):
  MI[partkey] = MergedIndex(view[0], PART) by l_partkey
  Index creation: MergedScan MI[partkey] -> MergeJoin(view[0], PART)

Pipeline 2 (branch):
  MI[partkey, suppkey] = MergedIndex(view[partkey], PARTSUPP) by (l_partkey, l_suppkey)
  Index creation: MergedScan MI -> MergeJoin(view, PARTSUPP)

Pipeline 3 (branch):
  MI[suppkey] = MergedIndex(view[partkey, suppkey], SUPPLIER) by l_suppkey
  Index creation: MergedScan MI -> MergeJoin(view, SUPPLIER)

Pipeline 4 (root):
  Root MI = MergedIndex(view[suppkey], NATION) by s_nationkey
  Query: MergedScan root MI -> Project -> Filter(p_name LIKE '%green%')
         -> Sort(nation, o_year DESC) -> Aggregate(SUM profit)
```

This is a 5-level pipeline cascade: each level adds one table to the join. The merged indexes at each level are keyed by the join column that connects the new table.

**Storage structure variants**: 4 (fewer variants than Q12 since the pipeline hierarchy is deep)

| # | Strategy | Notes |
|---|----------|-------|
| 1 | Traditional indexes + hash join | 5 hash joins |
| 2 | Materialized view | Full 6-way join materialized and grouped |
| 3 | Leaf MI only | MI(ORDERS, LINEITEM) + hash/merge joins for remaining 4 tables |
| 4 | Full pipeline cascade | All 5 merged indexes (the Calcite plan) |

**Key challenges**:

- 5 maintenance plans, one per pipeline level. A base table delta (e.g., new LINEITEM) must cascade through all downstream merged indexes.
- Space overhead: 5 merged indexes storing progressively wider records.
- The `LIKE '%green%'` filter on PART is hoisted to query time — it cannot be pushed into the merged indexes because the MI must serve queries with different color predicates.

### Q5 or Q7 (Tier 2 — TBD)

**Status**: No Calcite plans yet. Blocked on Calcite plan generation.

**Q7 as "MI-unfriendly" candidate**: The query joins NATION with itself (shipping nation vs. customer nation). This creates a non-tree join graph that does not decompose into a clean prefix-key hierarchy. If we implement Q7, we expect the merged index variant to show minimal or no improvement over traditional indexes — which is exactly what Reviewer 2 D2 asks us to demonstrate.

## Storage Structure Convention

The `--storage_structure` flag selects the indexing strategy. The numbering convention is:

| Value | Meaning | Universally Applicable? |
|-------|---------|------------------------|
| 0 | Force data reload (no recovery) | Yes |
| 1 | Traditional indexes + hash join | Yes |
| 2 | Traditional indexes + merge join (Q12 only) OR materialized view (other queries) | Per-query |
| 3 | Materialized view (Q12) OR leaf MI only (other queries) | Per-query |
| 4 | Leaf MI only (Q12) OR full MI cascade (other queries) | Per-query |
| 5 | Full MI cascade (Q12 only) | Q12 only |

**Harmonization needed**: Q12 uses 5 variants because it separates hash join and merge join baselines. For other queries, we should consolidate:

| Value | Harmonized Meaning |
|-------|-------------------|
| 0 | Force reload |
| 1 | Traditional indexes (best join algorithm) |
| 2 | Materialized view |
| 3 | Partial MI (leaf pipeline only, remaining joins at query time) |
| 4 | Full MI (all Calcite-prescribed merged indexes) |

Q12 can keep its 5-variant scheme since it is already designed. New queries (Q3, Q9) should use the 4-variant harmonized scheme.

## Cross-Project Collaboration Contract with Calcite

This section defines the interface between this repo (LeanStore, C++ execution engine) and the [Calcite project](https://github.com/alicia-lyu/calcite) (Java query optimizer). Both repos are developed in parallel by agents that may not share conversation context, so this contract must be self-contained.

### Roles

- **Calcite** generates optimized query plans that prescribe merged index configurations and pipeline decompositions. It decides *which* merged indexes to build and *what join order* to use.
- **LeanStore** translates those plans into C++ execution code. It implements the physical operators, storage structure variants, and experiment infrastructure.

LeanStore **cannot** invent its own merged index configurations — it must follow Calcite's plans. If no plan exists for a query, only the traditional index baseline (storage_structure=1) can be implemented.

### Two Plan Categories

Plans are delivered via two directories under `calcite-integration-info/` in this repo:

#### 1. `test-plans/` — Standard Optimization Plans (EXISTS)

Currently a symlink to `../../calcite/plus/test-dot-output`. Contains plans generated by Calcite's standard interesting-ordering optimization — i.e., Calcite picks the best overall plan using its cost model, which may or may not include merged indexes.

**Status**: Available for Q12, Q3, Q9.

These plans are sufficient for implementing the **full MI cascade** storage structure variant (the variant where every Calcite-prescribed merged index is built). They are the plans we used for the initial Q12 proof-of-concept.

#### 2. `int-ord-plans/` — Interesting-Ordering-Aware Plans (DOES NOT EXIST YET)

This is the **critical missing deliverable** from Calcite. These plans explore alternative join orders that are specifically optimized for merged index compatibility, addressing Reviewer 3 W3/D6.

**What must be in `int-ord-plans/`**: For each TPC-H query, Calcite must generate **three plan variants**:

| Plan Variant | Description | Purpose |
|--------------|-------------|---------|
| **(a) best-no-mi** | Best join order without any merged indexes | Traditional baseline; used by storage_structure=1 |
| **(b) best-with-mi** | Best join order that uses MI on the maximal beneficial sub-join | The "sweet spot" plan; used by storage_structure=3 (partial MI) |
| **(c) max-mi-prefix** | Join order that maximizes MI prefix match, even if overall cost is higher | Tests whether aggressive MI use helps or hurts; used by storage_structure=4 (full MI) |

For each variant, Calcite must export the same set of plan files as in `test-plans/`:

- `before-pipeline.dot` — the plan before pipeline decomposition (for reference)
- `root-pipeline-query-plan.dot` — the final query execution plan
- `*-index-creation-plan.dot` — one per non-root pipeline (how to build each MI)
- `maintenance-*.dot` — one per non-root pipeline (how to propagate deltas)

**Directory structure expected**:

```
calcite-integration-info/int-ord-plans/
  q12/
    best-no-mi/
      before-pipeline.dot
      root-pipeline-query-plan.dot
      ...
    best-with-mi/
      ...
    max-mi-prefix/
      ...
  q3/
    best-no-mi/
    best-with-mi/
    max-mi-prefix/
  q5/
    ...
  q9/
    ...
```

**Why this matters**: Reviewer 3 W3/D6 explicitly asks us to compare join orders. Without these three variants per query, we cannot run the join-order comparison experiment. The `test-plans/` plans only give us one point in the plan space — we need the full spectrum.

### Per-Query Deliverable Status

| Query | `test-plans/` | `int-ord-plans/` | LeanStore Can Implement |
|-------|---------------|-------------------|------------------------|
| **Q12** | ✅ `q12/` | ❌ Not yet | Traditional (1), merge join (2), materialized view (3), MI variants (4,5) from test-plans |
| **Q3** | ✅ `q3ol/` | ❌ Not yet | Traditional (1) now; MI variants blocked on int-ord-plans |
| **Q9** | ✅ `q9/` | ❌ Not yet | Traditional (1) now; MI variants blocked on int-ord-plans |
| **Q5** | ❌ | ❌ | Traditional (1) only; all MI variants blocked |
| **Q7** | ❌ | ❌ | Traditional (1) only; all MI variants blocked |
| **Q10** | ❌ | ❌ | Traditional (1) only; all MI variants blocked |

**Unblocked work** (can proceed without any Calcite deliverables):

- Q12 full implementation (all 5 variants) — uses existing `test-plans/q12/`
- Traditional index baselines for Q3, Q5, Q9 (storage_structure=1, hash/merge join)
- Ad-hoc experiments (selectivity sweep, single-table scan overhead) — reuse Q12 infrastructure
- Operator framework in `frontend/shared/operators/`

**Blocked work** (requires `int-ord-plans/`):

- MI variants for Q3, Q9 (storage_structure=3,4) using the *right* join order
- Join-order comparison experiments (Reviewer 3 W3/D6)
- Materialized view variants that correspond to the MI pipeline (storage_structure=2) — the view must match the pipeline Calcite selects

**Blocked work** (requires new `test-plans/` for Tier 2 queries):

- Any implementation of Q5, Q7, Q10 beyond traditional baselines

### Plan Format

Current format is **DOT (Graphviz)**. Each node label contains the Calcite operator type and parameters. LeanStore hand-translates these into C++ operator trees.

Key conventions in DOT labels:

- `EnumerableMergedIndexScan(mi=[[col]], source=[...], sourceIndex=[N])` — scan a merged index, extracting source N (by table ordinal)
- `EnumerableMergeJoin(condition=[...], joinType=[inner])` — sort-merge join
- `EnumerableSortedAggregate(group=[{cols}], AGG=[fn($col)])` — sorted aggregate
- `view([N])` in source fields refers to the materialized output of pipeline N

**Future**: Calcite exports JSON plans that a C++ interpreter can parse automatically (eliminates hand-translation). This is not blocking for the current set of queries but will be necessary for scaling to JOB.

### Communication Protocol

When Calcite generates new plans:

1. Place DOT files in the appropriate directory (`test-plans/<query>/` or `int-ord-plans/<query>/<variant>/`)
2. Since `test-plans/` is a symlink, new files appear automatically if both repos are on the same machine
3. For `int-ord-plans/`, either symlink or copy — the LeanStore agent will detect new files and proceed with implementation
4. Update this section's status table to reflect the new deliverables

When LeanStore needs a plan that doesn't exist:

1. The LeanStore agent will **refuse to implement** the MI variant and note the dependency here
2. The user (or Calcite agent) must generate the plan before work can proceed
3. This prevents the LeanStore agent from inventing ad-hoc merged index configurations that don't match what the optimizer would actually produce

## Experiment Infrastructure

### Makefile and Targets

Experiments are driven by `generate_targets.py`, which emits Makefile rules into `targets.mk`. Currently it generates targets for `geo_btree` and `geo_lsm`. To add TPC-H queries:

1. Add `q12_lsm`, `q12_btree` (etc.) to `exec_names` in `generate_targets.py`
2. Define `STRUCTURE_OPTIONS` for each (e.g., `"q12_lsm": [1, 2, 3, 4, 5]`)
3. Add `DIFF_DIRS` mappings (e.g., `"q12_lsm": "tpch/q12"`)
4. Run `python3 generate_targets.py > targets.mk` to regenerate

This produces targets like `make q12_lsm scale=15` (all structures) and `make q12_lsm_3` (single structure).

### CSV Output

Experiments output CSV files to `build/<exec>/<scale>-in-<dram>/`. Metrics collected (from geo benchmark pattern):

- Query latency (per-query and aggregate)
- Throughput (queries/sec)
- Maintenance cost (updates/sec for RF1/RF2)
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
make q12_btree_lldb_4

# Reload data (force re-generation)
make q12_lsm_reload
```

## Reviewer Critique Mapping

### Reviewer 1: More TPC-H

| Concern | Experiment |
|---------|-----------|
| "Evaluate on standard benchmarks" | Q12, Q3, Q9 with all storage structure variants |
| "Show generality beyond geo queries" | Complexity gradient: 2-table, 3-table, 6-table |

### Reviewer 2: Honest Trade-offs

| Concern | Experiment |
|---------|-----------|
| D2: "Show where MI loses" | Q7 (non-hierarchical join graph) or Q9 with space overhead measurement |
| "Maintenance overhead" | RF1/RF2 latency across all queries, especially Q9 with 5-level cascade |
| "Space overhead" | Storage size comparison: traditional vs. MI vs. materialized view |

### Reviewer 3: Specific Experiments

| Concern | Code | Experiment |
|---------|------|-----------|
| W2/D5: Larger scan ranges | Vary selectivity | Run Q12 with different date ranges (1-month, 1-year, 3-year windows) |
| W3/D6: Single-table scan overhead in MI | Overhead test | Measure LINEITEM-only scan speed in standalone index vs. interleaved MI |
| W3/D6: Join order comparison | Join orders | For Q3/Q5: (a) best order without MI, (b) best order with MI on maximal sub-join, (c) order maximizing MI prefix match |
| D3-D4: MI vs. materialized views | View comparison | Every query has both MI and materialized view variants; compare query latency, maintenance cost, space |
| "B-tree vs LSM deeper analysis" | Backend comparison | Run all queries on both `_btree` and `_lsm` executables |

### Ad-Hoc Experiments (Reviewer 3)

These do not require full query implementations:

1. **Scan selectivity sweep**: For Q12, vary the date filter window from 1 month to 5 years. Plot query latency vs. selectivity for each storage structure.

2. **Single-table scan overhead**: Load LINEITEM into (a) standalone index, (b) MI with ORDERS interleaved. Measure full-scan time for LINEITEM only. The overhead comes from skipping ORDERS records during scan.

3. **MI density analysis**: For Q9, measure what fraction of MI records belong to each table type. High skew (e.g., 95% LINEITEM, 5% everything else) means single-table scans pay high overhead.

## Implementation Order

1. **Q12** — complete implementation, all 5 variants, both backends. This is the proof-of-concept.
2. **Q3** — implement using the operator framework built for Q12. 4 variants.
3. **Ad-hoc experiments** — selectivity sweep, single-table scan overhead. These reuse Q12 infrastructure.
4. **Q9** — most complex, implement after operator framework is proven. 4 variants.
5. **Q7 or Q5** — Tier 2 query for honest evaluation. Requires Calcite plan generation first.

## Open Design Questions

1. **Harmonizing storage_structure numbering**: Q12 already uses 1-5. Should we retroactively change Q12 to match the 4-variant scheme, or accept the inconsistency?

2. **Operator framework location**: Q12 CLAUDE.md proposes `frontend/shared/operators/`. This is the right call — build it once, reuse for Q3/Q9. But the framework must be generic enough to handle Q9's 5-level pipeline cascade.

3. **Materialized view granularity**: For Q12 option 3, the view is pre-aggregated (2 rows). For Q3, the view could be the full grouped result or the pre-LIMIT top-10. For Q9, the 6-way join is massive. The materialized view definition per query must be documented clearly.

4. **Maintenance plan complexity for Q9**: 5 cascading maintenance plans means a single LINEITEM insert triggers updates to 5 merged indexes. Is this realistic? Should we measure and report this honestly as a cost?

5. **Record type explosion**: Each query needs custom result types (`q12_result_t`, `q3_result_t`, etc.) and potentially intermediate view types. The Q12 CLAUDE.md flags this as a scalability concern. Macro-based or template-based generation may be needed for Q9's many intermediate types.
