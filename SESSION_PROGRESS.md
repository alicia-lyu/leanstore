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

### Implement Q12 first

Q12 is the simplest Tier 1 query (2 tables, 1 join, 1 filter, 1 aggregate). New files under `frontend/tpch/q12/`:

| File | Purpose |
| ---- | ------- |
| `q12_views.hpp` | `q12_result_t` record (intermediate pipeline view entry); Q12 filter, projection, and aggregate functions |
| `q12_workload.hpp` | `Q12Workload<AdapterType>` (load, query, maintain) |
| `q12_join_query.tpp` | Per-structure query implementations (`by_base`, `by_view`, `by_merged`, `by_hash`) |
| `q12_maintain.tpp` | RF1/RF2 + view maintenance |
| `executable_rocksdb.cpp` | RocksDB entry point (macOS + Linux) |
| `executable_leanstore.cpp` | LeanStore entry point (Linux only) |

Pattern after `frontend/geo/` (in particular `workload.hpp`, `join_search_count.tpp`, `executable_rocksdb.cpp`).

Build-system integration:

- Add `q12_lsm` and `q12_btree` to `exec_names` in `generate_targets.py`, populate `STRUCTURE_OPTIONS` and `DIFF_DIRS`, regenerate `targets.mk`.
- Add CMake targets in `frontend/CMakeLists.txt` mirroring `geo_lsm` / `geo_btree`.

Verification: build, smoke test at scale=1, cross-validate query results across all four storage structures.

### Then Q3 and Q9

Same pattern. Q9 is 6-way; structure 3 keeps `MergedIndex(ORDERS, LINEITEM)` and joins the remaining four tables (`PART`, `SUPPLIER`, `PARTSUPP`, `NATION`) at query time.

### Ad-hoc experiments (after Q12)

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
