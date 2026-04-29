# Session Progress: TPC-H Tier 1 Queries (Q12, Q3, Q9)

## Status

Plans, per-query CLAUDE.md docs, and shared TPC-H infrastructure are complete and spec-compliant. **Ready to start C++ query implementation, beginning with Q12.**

## Scope

Current paper: each Tier 1 query uses **at most one merged index** that interleaves the source tables of a single pipeline (matching the geo benchmark). The four storage structure variants per query are:

| # | Strategy | Description |
| - | -------- | ----------- |
| 1 | Traditional + merge join | Separate per-table B-trees, merge join on shared sort key |
| 2 | Materialized view | **Intermediate pipeline view** (the join output of the same pipeline whose sources the MI interleaves), not the final query result |
| 3 | Single merged index | `MergedIndex(ORDERS, LINEITEM)` by `orderkey`; PremergedJoin at query time |
| 4 | Clustered + hash join | Clustered indexes; hash join chain at query time |

The master document for the experiment plan, plan-source mapping, query selection rationale, and reviewer-critique mapping is [`TPCH_experiments.md`](./TPCH_experiments.md). Multi-MI pipeline cascades (described in `calcite-integration-info/test-plans/` and the original Q12 grand plan) are **next-paper scope** and intentionally out of scope here.

## Completed

- **Q12/Q3/Q9 skeletons** committed (`d9cb0d95`, `83fb6583`, `e78a48e2`). Each query has the full 7-file shape under `frontend/tpch/q{N}/`: `views.hpp`, `workload.hpp`, `per_structure_workload.hpp`, `load.tpp`, `query.tpp`, `executable_rocksdb.cpp`, `executable_leanstore.cpp`. All method bodies are `// TODO(skeleton)` stubs citing the relevant CLAUDE.md section. `frontend/tpch/CLAUDE.md` added as the top-level skeleton guide (`355e3a96`).
- **Shared substrate** (`aefffeab`): `backend.hpp` (RocksDB/LeanStore traits structs collapsing 4 template params to 1), `ol_pipeline.hpp` + `ol_pipeline.tpp` (`OrdersLineitemPipeline<Backend>` with stub join drivers and load helpers shared by all three queries). `views_ol.hpp` tracked (`355e3a96`).
- **Architectural departures from geo** locked in: single `Backend` traits param (no 4-param explosion), no virtual dispatch in per-structure wrappers, monolithic post-join (filter/project/aggregate fused in the join callback).
- **Plans (DOT, Graphviz)** for Q12, Q3, Q9 across all four structures live in `frontend/tpch/<q>/plans/`:
  - `structure_1.dot` — interesting-ordering optimized (Calcite `EnumerableMergeJoin` with explicit sorts)
  - `structure_2_4_default.dot` — default Calcite plan (hash joins)
  - `structure_3_draft.dot` — MI-adapted (innermost merge-join pipeline replaced with `EnumerablePremergedJoin` over an MI)
- **Per-query CLAUDE.md docs**: `frontend/tpch/q12/CLAUDE.md`, `frontend/tpch/q3/CLAUDE.md`, `frontend/tpch/q9/CLAUDE.md`. Each contains:
  - The TPC-H spec SQL, substitution parameters, and validation values.
  - Column-index mappings for every plan variant.
  - Per-structure plan walk-through.
  - "Execution Style: Monolithic vs Cascade" feasibility analysis (recommendation: monolithic post-join for the current paper; see Conceptual decisions below).
- **DOT plan bug fixes** committed:
  - Q12 `CASE` expression corrected to match TPC-H spec (HIGH = `'1-URGENT' OR '2-HIGH'`, LOW = neither) across `structure_1.dot`, `structure_2_4_default.dot`, `structure_3_draft.dot`.
  - Q3 plans: filters pushed down immediately after table scans; `structure_3_draft.dot` rewritten with corrected Project column indices for the PremergedJoin output schema.
- **Shared TPC-H tables/workload rewritten for spec compliance** and moved to `frontend/tpch/` (commits `ccdefa71`, `ef4ec028`, `e7afb10a`). This is **not** a copy-paste migration — ~300 lines changed in `tpch_tables.hpp` and ~450 lines in `tpch_workload.hpp`, covering spec §4.2.3 domain arrays, sparse orderkeys, date-correlated lineitem fields, the `o_custkey % 3` constraint, exactly 4 PARTSUPP rows per part, and hardcoded `NATIONS[25]` / `REGIONS[5]`. See the **Shared TPC-H Infrastructure** section in [`TPCH_experiments.md`](./TPCH_experiments.md) for the full list and rationale. Old `frontend/geo/tpch_*.hpp` are thin `#include` wrappers preserved so the geo benchmark continues to build.

## Conceptual Decisions Captured in Docs

1. **`joined_t` is required by merge-join infrastructure.** Every `frontend/shared/merge-join/*` template (`PremergedJoin`, `BinaryMergeJoin`, `HashJoin`, `JoinState`) takes the join-result type `JR` as a template parameter and requires it to satisfy `Key(Rs::Key...)` and `JR(Rs...)` constructors. Monolithic execution can fuse only **post-join** operators (filter, project, aggregate) into callbacks; the typed join result itself is unavoidable. All three per-query CLAUDE.md docs were corrected to reflect this.
2. **Storage structure 2 = intermediate pipeline view, not the final query result.** Per the paper's "Spectrum of Pre-computation" section, the most apples-to-apples alternative to a merged index is the materialized output of the same pipeline whose sources the MI interleaves. For all Tier 1 queries this is `ORDERS ⋈ LINEITEM`. Comparing MI against a final-result view would require a hybrid query+update workload that is not native to TPC-H; we may build one separately, but it is not the primary comparison.

## Next Steps

### [Short-term] Per-query refactor — move operator drivers and view loading out of pipeline

`OrdersLineitemPipeline` now only owns `populate_merged` + `get_merged_size`. The per-query stubs in `q{N}/load.tpp` reference removed methods (`ol.populate_view`, `ol.get_view_size`). Fix in this order:

- `q12/load.tpp` — replace `ol.populate_view` / `ol.get_view_size` stubs with per-query view adapter calls; wire `Q12Workload` ctor and `load()` / `get_size()`.
- `q12/query.tpp` — implement `scan_merged` (PremergedJoin driver), `merge_join_base` (BinaryMergeJoin driver), `hash_join_base` (HashJoin driver) as per-query methods; add predicates, projection, aggregator bodies with monolithic post-join lambdas.
- `q12/views.hpp` — `Params::defaults()`.
- Build smoke test: scale=1, cross-validate results across all four structures (use `test_load_merged_lsm` to confirm MI[0] loads correctly first).
- Repeat for Q3 (adds CUSTOMER hash join + top-10 sort) and Q9 (adds NATION/SUPPLIER hashmaps + PART/PARTSUPP joins inside callback).

### [Short-term] Build-system integration — `frontend/CMakeLists.txt` + `generate_targets.py`

- Add CMake targets `q12_lsm`, `q12_btree`, `q3_lsm`, `q3_btree`, `q9_lsm`, `q9_btree` mirroring `geo_lsm` / `geo_btree`.
- Add entries to `generate_targets.py` (`exec_names`, `STRUCTURE_OPTIONS`, `DIFF_DIRS`), regenerate `targets.mk`.

### [Medium-term] Ad-hoc experiments (after all three queries compile and validate)

- Scan-selectivity sweep on Q12 (1-month / 1-year / 3-year date windows) — addresses Reviewer 3 W2/D5.
- Single-table `LINEITEM`-only scan: standalone index vs. MI with ORDERS interleaved — addresses Reviewer 3 W3/D6.

## Open / Side-Track Items

- **Tagged row format** for merged adapters/scanners. Still pending. Becomes load-bearing for Q9, whose MI may interleave record types with non-distinct key lengths once we extend beyond `(ORDERS, LINEITEM)`. Migration strategy choice (new default + update geo / parameterize per index / Q12-only new classes) is unresolved.
- `joined_t` flattening: store flattened projection instead of a tuple of input records.
- Rename `JK` → `SK` (sort key) for naming consistency with the paper.
- Investigate `JoinState` overhead in hash join — previously implemented without it.
- LeanStore background-insert bug: records shifting pages causes issues; RocksDB unaffected.

## Long-Term (Next-Paper Scope)

Multi-MI pipeline cascades (each query may have multiple merged indexes — e.g., Q12 with 2 MIs, Q9 with 5) live in `calcite-integration-info/test-plans/` and are documented in `frontend/tpch/q12/CLAUDE.md`. They are explicitly out of scope for the current paper.
