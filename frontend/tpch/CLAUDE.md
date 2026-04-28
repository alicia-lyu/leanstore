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
- `backend.hpp` — `RocksDBBackend` and `LeanStoreBackend` traits structs.
- `ol_pipeline.hpp` / `ol_pipeline.tpp` — `OrdersLineitemPipeline<Backend>`:
  shared join drivers (`scan_merged`, `merge_join_base`, `hash_join_base`)
  and load helpers (`populate_pipeline_view`, `populate_merged_ol`).

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
   post-join** execution style endorsed in `q12/CLAUDE.md §Execution Style`.

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

## What's Needed Beyond Plans to Fully Implement Q12/Q3/Q9

The skeletons compile structurally but all method bodies are TODO stubs.
Implementing a query end-to-end requires filling in:

- `OrdersLineitemPipeline` method bodies: `PremergedJoin` driver
  (`scan_merged`), `BinaryMergeJoin` driver (`merge_join_base`), `HashJoin`
  driver (`hash_join_base`), `populate_pipeline_view`, `populate_merged_ol`,
  and `get_merged_size`. See `frontend/shared/merge-join/` for the join
  templates and `q12/CLAUDE.md §Stages x Options` for the load patterns.
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
