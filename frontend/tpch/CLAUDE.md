# TPC-H Tier 1 Workload Guide

Top-level reference for the Q12 / Q3 / Q9 skeleton implementation under
`frontend/tpch/`. Read this before touching any per-query directory.

## Layout

Shared files (used by all three queries):

- `tpch_tables.hpp` — all 8 TPC-H base table record types (`orders_t`,
  `lineitem_t`, `customerh_t`, `part_t`, `supplier_t`, `partsupp_t`,
  `nation_t`, `region_t`).
- `tpch_workload.hpp` — `TPCHWorkload<Adapter>`: loads and recovers all 8
  base tables.
- `views_ol.hpp` — `ol_sort_key_t`, `joined_ol_t` (the ORDERS × LINEITEM
  join result type shared by Q12/Q3/Q9), and the `SKBuilder` specialization.
  Fully implemented with unit tests.
- `test_views_ol.cpp` — 12 unit tests for sort key ordering, match semantics,
  `SKBuilder` round-trips, `joined_ol_t` construction. CMake target:
  `test_views_ol` (macOS/RocksDB-only build).
- `backend.hpp` — `RocksDBBackend` and `LeanStoreBackend` traits structs.
- `ol_pipeline.hpp` / `ol_pipeline.tpp` — `OrdersLineitemPipeline<Backend>`:
  shared join drivers (`scan_merged`, `merge_join_base`, `hash_join_base`)
  and Pipeline Convention load/size methods (`populate_view`, `populate_merged`,
  `get_view_size`, `get_merged_size`). See §Pipeline Convention below.

Per-query subdirectories:

- `q12/` — Shipping Modes and Order Priority (2 tables, 1 join).
- `q3/`  — Shipping Priority (3 tables, 2 joins; adds CUSTOMER adapter).
- `q9/`  — Product Type Profit Measure (6 tables, 5 joins; adds NATION,
  SUPPLIER, PART, PARTSUPP adapters).

Each per-query directory contains the same 8-file shape described in
§Per-query file convention below.

## Architectural Decisions vs `frontend/geo/`

Three deliberate departures from the geo benchmark:

1. **Backend-traits struct collapses 4 template params to 1.** The geo
   benchmark passes `AdapterType`, `MergedAdapterType`, `ScannerType`, and
   `MergedScannerType` separately to every workload class. Here, a single
   `Backend` struct exposes all four as nested alias templates
   (`Backend::template Adapter<T>`, `Backend::template MergedAdapter<Ts...>`,
   etc.). Every workload class takes `template <typename Backend>` only.

2. **No virtual dispatch in per-structure wrappers.** Geo's wrapper structs
   have a virtual destructor. Here `BaseQN / ViewQN / MergedQN / HashQN` are
   plain structs. Structure selection happens at compile time in the
   executable's `switch`; no vtable is needed.

3. **Shared `OrdersLineitemPipeline<Backend>` mixin.** All three queries
   share `MergedIndex(orders_t, lineitem_t)` keyed by orderkey. Factoring
   the three join drivers into one class avoids triplicating that substrate
   across Q12/Q3/Q9. Each per-query workload composes `OrdersLineitemPipeline`
   and supplies its own filter/project/aggregate lambda — the **monolithic
   post-join** execution style documented in `OPERATORS.md §3`.

## Pipeline Convention

A **Pipeline class** owns the secondary structures for one group of tables
and exposes four load/size methods plus query-time join drivers.

### Why pipelines exist

Multiple queries may share the same merged index or intermediate view. Rather
than duplicating load logic in each `Q{N}Workload`, secondary-structure loading
lives in the pipeline class. Per-query `load()` and `get_size()` are one-line
dispatchers that delegate to the pipeline.

### The four load/size methods (concept-level, not virtual)

```cpp
template <typename ViewAdapter>
void populate_view(ViewAdapter& view);

void populate_merged();

template <typename ViewAdapter>
double get_view_size(ViewAdapter& view) const;

double get_merged_size() const;
```

`populate_view` is templated on `ViewAdapter` because the **view row type
belongs to the per-query workload** — each query picks its own projection of
the join output. For Tier 1, all three queries alias the view row to
`joined_ol_t`, so `populate_view` inserts `joined_ol_t` rows regardless of
which query calls it. A future pipeline with a richer or narrower view row
type can constrain `ViewAdapter` with a `requires` clause.

### Ready-for-change rationale

Adding a second pipeline (e.g., `OrdersCustomerPipeline`, `LineitemPartsuppPipeline`)
requires only:

1. A new `orders_customer_pipeline.hpp` / `.tpp` file following this convention.
2. Adding the pipeline as a member of whichever `Q{N}Workload` classes need it.
3. Extending those workloads' `load()` / `get_size()` switch cases if the new
   pipeline introduces additional storage-structure variants.

No change to any shared header, no change to unaffected queries, no virtual
base class to update.

### Current state

The only pipeline today is `OrdersLineitemPipeline<Backend>` (`ol_pipeline.hpp`).
Q12, Q3, and Q9 each hold exactly one instance named `ol` and dispatch all
secondary-structure loading through it.

## Per-query File Convention

Each `q{N}/` directory contains:

| File | Responsibility |
|------|---------------|
| `views.hpp` | `Params` struct with `defaults()`, `q{N}_pipeline_view_t` alias, `q{N}_agg_row_t` with key and payload, predicate declarations. |
| `workload.hpp` | `Q{N}Workload<Backend>`: adapter members, `OrdersLineitemPipeline<Backend> ol`, `query_by_*` / `load` / `get_size` declarations. Ends with `#include "load.tpp"` and `#include "query.tpp"`. |
| `per_structure_workload.hpp` | Plain `BaseQ{N} / ViewQ{N} / MergedQ{N} / HashQ{N}` structs, each holding `Q{N}Workload<Backend>& w` and declaring `query` / `get_size`. |
| `load.tpp` | `Q{N}Workload` ctor, `load()`, `get_size()` template bodies (TODO stubs). |
| `query.tpp` | `query_by_*` bodies, four wrapper `query` / `get_size` bodies, and predicate inline implementations (TODO stubs). |
| `executable_rocksdb.cpp` | `main()` for RocksDB backend. Declares all needed adapters, constructs workload, dispatches on `FLAGS_storage_structure`. |
| `executable_leanstore.cpp` | Same as above, guarded by `#ifndef ROCKSDB_ONLY`, uses `LeanStoreBackend`. |
| `CLAUDE.md` | Per-query SQL, plan descriptions, execution style analysis, column index mappings, and an "Implementation Status (skeleton)" section appended when the skeleton was created. |

## Storage-structure → Wrapper Mapping

| `--storage_structure` | Wrapper struct | Join strategy |
|-----------------------|---------------|---------------|
| 1 | `BaseQ{N}` | Traditional indexes + binary merge join |
| 2 | `ViewQ{N}` | Intermediate pipeline view (materialized `joined_ol_t` rows) |
| 3 | `MergedQ{N}` | `MI[0]` only — `PremergedJoin` at query time |
| 4 | `HashQ{N}` | Traditional indexes + hash join |

Structure 0 (data reload) is handled before the switch in each executable.

## Completed (post-skeleton)

- **`views_ol.hpp` fully implemented** (2026-04-29): `ol_sort_key_t` (match,
  first_diff, matching_keys), `joined_ol_t` (constructors, accessors),
  `SKBuilder<ol_sort_key_t>` (create, project, to_key). Unit-tested via
  `test_views_ol.cpp` (CMake target `test_views_ol`, 12 tests).
- **Shared merge-join infrastructure refactored** (2026-04-29):
  `PremergedJoin` decoupled from record-type `get_jk()` / `Key(JK)` via
  `jk_from_variants` free function and `SKBuilder::to_key<R>`.
  `SKBuilder::get<R>` renamed to `project<R>` across all files.
  Geo backward compatibility verified (`geo_lsm` builds clean).

## What's Needed to Fully Implement Q12/Q3/Q9

- `OrdersLineitemPipeline` method bodies (`ol_pipeline.tpp`): `PremergedJoin`
  driver (`scan_merged`), `BinaryMergeJoin` driver (`merge_join_base`),
  `HashJoin` driver (`hash_join_base`), `populate_view`, `populate_merged`,
  `get_view_size`, and `get_merged_size`. See `frontend/shared/merge-join/`
  for the join templates and `OPERATORS.md §4` for the load patterns.
  Per-query `load()` / `get_size()` already dispatch to these.
- Per-query `Params::defaults()` implementations (one per query).
- Per-query predicate / projection / aggregator bodies inside `query.tpp`.
- Per-query `query_by_*` bodies that call `ol.scan_merged(lambda)`,
  `ol.merge_join_base(lambda)`, or `ol.hash_join_base(lambda)` with
  monolithic post-join lambdas (filter + project + aggregate fused inline).
- Q3-specific: CUSTOMER merge join (sort by `o_custkey`, two-pointer scan
  of filtered CUSTOMER) after the OL aggregate, then top-10 sort.
- Q9-specific: NATION and SUPPLIER hashmap construction before the OL scan;
  PART and PARTSUPP merge joins inside the per-row callback; LIKE filter on
  `p_name` applied immediately after PART lookup.
- CMake targets: `q12_lsm`, `q12_btree`, `q3_lsm`, `q3_btree`, `q9_lsm`,
  `q9_btree` — add to `frontend/CMakeLists.txt` following the `geo_lsm` /
  `geo_btree` pattern.
- `generate_targets.py` Makefile entries for the new targets.

## Out of Scope (Skeleton)

The following are explicitly deferred and not part of this skeleton:

- Update / maintenance paths (RF1 insert, RF2 delete).
- Tagged-row format migration (current fold-length discrimination is
  sufficient for the initial implementation).
- `joined_t` flattening (the generic `joined_t` template from
  `frontend/shared/view_templates.hpp` is used as-is).
