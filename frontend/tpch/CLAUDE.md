# TPC-H Tier 1 Workload Guide

Top-level reference for the Q12 / Q3 / Q9 skeleton implementation under
`frontend/tpch/`. Read this before touching any per-query directory.

## Layout

Shared files (used by all three queries):

- `tpch_tables.hpp` — all 8 TPC-H base table record types (`orders_t`,
  `lineitem_t`, `customerh_t`, `part_t`, `supplier_t`, `partsupp_t`,
  `nation_t`, `region_t`) plus `invoice_t` (~2 invoices per order,
  keyed by `i_invoicekey`). `lineitem_t` carries an `l_invoicekey` payload
  linking each lineitem to its invoice. Date constants `DATE_1994_01_01 = 8766`
  and `DATE_1995_01_01 = 9131` (days since 1970-01-01) are defined here.
- `tpch_workload.hpp` — `TPCHWorkload<Adapter>`: loads and recovers all 8
  base tables plus `invoice_t`. `loadPartsuppLineitem` and `loadLineitem`
  call `orderkey_from_index()` to produce sparse order keys that match
  `loadOrders`; using raw indices would orphan ~75% of lineitems.
  `loadInvoiceAndLinkLineitem()` runs after `loadOrders`: it creates 2
  invoices per order and back-fills `l_invoicekey` on each lineitem
  via a second lineitem pass.
- `views_ol.hpp` — `ol_sort_key_t`, `joined_ol_t` (the ORDERS × LINEITEM
  join result type shared by Q12/Q3/Q9), and the `SKBuilder` specialization.
  `joined_ol_t` is a derived struct (not a `using` alias) with an explicit
  `unfoldKey` override; the generic `joined_t::unfoldKey(fold_pks=false)`
  path does not compile for this type. Fully implemented with unit tests.
- `test_views_ol.cpp` — 12 unit tests for sort key ordering, match semantics,
  `SKBuilder` round-trips, `joined_ol_t` construction. CMake target:
  `test_views_ol` (macOS/RocksDB-only build).
- `backend.hpp` — `RocksDBBackend` and `LeanStoreBackend` traits structs.
- `ol_pipeline.hpp` / `ol_pipeline.tpp` — `OrdersLineitemPipeline<Backend>`:
  constructor, `populate_merged` (dual-write replay over base scanners), and
  `get_merged_size`. Operator drivers and view loading are **not** here — they
  depend on per-query types and live in `q{N}/query.tpp` and `q{N}/load.tpp`.
  See §Pipeline Convention below.
- `views_coli.hpp` — Calcite-style tagged record types for the
  CUSTOMER × ORDERS × LINEITEM × INVOICE 4-table merged index:
  `customer_coli_t`, `orders_coli_t`, `lineitem_coli_t`, `invoice_coli_t`.
  Also defines the aCOLI (pre-aggregated) 2-type pair:
  `customer_acoli_t` (id=49, adds `pre_open_due`) and
  `orders_acoli_t` (id=50, adds `pre_revenue`), used by Q3I S5.
  Each `Key` carries a `using path = tagged_path<IdxId, Steps...>` declaration
  (pointer-to-member steps: `tag_field_step`, `tag_fields2_step`) plus a
  `static bool matches(const u8*, size_t)` override that reads the trailing
  `idx_id` byte. Sentinel tag `index=0` sorts before all domain tags
  (`customer=1`, `invoice=2`, `orders=3`, `lineitem=4`). Within a custkey
  group the byte-lex order is customer → invoice → orders → lineitems:
  invoice (t=2) precedes orders/lineitems (t=3/4) so consumers finalize
  per-custkey sub-aggregates (e.g. `cust_open_due`) before bulk O×L
  records stream by (§3.1.2 sibling pattern). Twelve explicit
  `SKMatcher<R1,R2>` specializations cover all 16 ordered pairs (10 unique +
  reverse delegations).
- `coli_pipeline.hpp` / `coli_pipeline.tpp` —
  `COLIPipeline<Backend>`: owns `MergedAdapter<customer_coli_t,
  orders_coli_t, lineitem_coli_t, invoice_coli_t>`. `populate_merged()`
  does a dual-write replay across customer/orders/lineitem/invoice base
  adapters, resolving each lineitem's `custkey` via an in-memory
  orderkey→custkey map built during the orders pass.
- `per_structure_workload.hpp` — shared `BaseStructure` / `ViewStructure` /
  `MergedStructure` / `HashStructure` templates parametrized by
  `<typename Workload, typename AggRow>` with inline forwarder bodies. Each
  per-query `per_structure_workload.hpp` collapses to alias-only (e.g.
  `using BaseQ12 = ::tpch::BaseStructure<Q12Workload<Backend>, q12_agg_row_t>`).
- `test_load_merged_rocksdb.cpp` / `test_load_merged_leanstore.cpp` —
  standalone load-test binaries that drive `populate_merged()` end-to-end via
  `TPCHWorkload` with no per-query state. CMake targets: `test_load_merged_lsm`
  (macOS + Linux), `test_load_merged_btree` (Linux only).
- `tpch_flags.hpp` — shared gflags definitions for the per-query executables.
  Each executable `#define TPCH_DEFINE_FLAGS` before including; one TU per
  binary defines, the rest declare. `tentative_skip_bytes` is per-executable
  (default differs by backend).
- `tpch_executable.hpp` — `tpch::dispatch_storage_structure<Base, View,
  Merged, Hash, Backend>(workload, result)` template helper. Replaces the
  4-case `switch (FLAGS_storage_structure)` block that used to be duplicated
  across all six per-query executables.

Per-query subdirectories:

- `q12/` — Shipping Modes and Order Priority (2 tables, 1 join).
- `q3/`  — Shipping Priority (3 tables, 2 joins; adds CUSTOMER adapter).
- `q9/`  — Product Type Profit Measure (6 tables, 5 joins; adds NATION,
  SUPPLIER, PART, PARTSUPP adapters).
- `q3i/` — Q3 + Invoice sibling aggregate (COLI MI showcase; all five
  storage structures complete — S1–S4 parity verified, S5 aCOLI MI
  implemented and verified; see `q3i/CLAUDE.md §Implementation Phases`).
- `q5i/` — Q5 + Invoice payment-status split (design doc only; no skeleton yet).
- `q10i/` — Q10 + Customer payment-behaviour overlay (design doc only; no skeleton yet).

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

A **Pipeline class** owns the secondary structures for one group of tables.
The convention is aspirational: a mature pipeline exposes four load/size
methods plus query-time join drivers. The current pipeline (`OrdersLineitemPipeline`)
only implements the truly-shared subset.

### Why pipelines exist

Multiple queries may share the same merged index or intermediate view. Rather
than duplicating load logic in each `Q{N}Workload`, secondary-structure loading
lives in the pipeline class. Per-query `load()` and `get_size()` are one-line
dispatchers that delegate to the pipeline.

### What the pipeline owns (current)

`OrdersLineitemPipeline` exposes only what is genuinely shared across Q12/Q3/Q9:

```cpp
void populate_merged();       // dual-write replay over base scanners
double get_merged_size() const;
```

### What lives in per-query dirs

Operator drivers and view loading depend on per-query types and belong in
`q{N}/query.tpp` and `q{N}/load.tpp`:

- `scan_merged` — Structure 3 PremergedJoin driver
- `merge_join_base` — Structure 1 BinaryMergeJoin driver
- `hash_join_base` — Structure 4 HashJoin driver
- `populate_view` / `get_view_size` — view row type is per-query

> **TODO debt (Q3/Q9 only)**: `q3/load.tpp` and `q9/load.tpp` still reference
> `ol.populate_view` and `ol.get_view_size`, which were removed from the
> pipeline. These will be fixed when Q3/Q9 per-query load bodies land.
> Q12 load.tpp is fully implemented and no longer has this debt.

### Secondary indexes for Structure 1

A pipeline is also responsible for populating any **secondary indexes**
required for its tables under Structure 1 (traditional indexes + binary
merge join). The merge join requires both inputs to be sorted by the join
key; if a base table is not already sorted that way, the pipeline owns the
secondary index that provides the required sort order, and `populate_merged`
should be paired with an analogous `populate_secondary_*` step.

**Not applicable to `OrdersLineitemPipeline`**: the join key is `orderkey`,
which is already the leading key of both `orders_t` (`{o_orderkey}`) and
`lineitem_t` (`{l_orderkey, l_linenumber}`). Base tables are merge-joinable
as-is, no secondary index needed.

Documented here for future pipelines (e.g., an Orders×Customer pipeline
joining on `o_custkey` would need a `custkey`-sorted secondary index over
ORDERS).

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

Two pipelines exist:

- `OrdersLineitemPipeline<Backend>` (`ol_pipeline.hpp`) — 2-table OL merged
  index. Q12, Q3, and Q9 each hold exactly one instance named `ol`.
- `COLIPipeline<Backend>` (`coli_pipeline.hpp`) — 4-table COLI merged index
  (CUSTOMER × ORDERS × LINEITEM × INVOICE). Uses tagged-key format with
  `views_coli.hpp` types. Currently a standalone pipeline not yet wired into
  any per-query workload class.

## Per-query File Convention

Each `q{N}/` directory contains:

| File | Responsibility |
|------|---------------|
| `views.hpp` | Row-shape types only: `q{N}_pipeline_view_t` alias and `q{N}_agg_row_t` with key and payload. `Params` struct and predicate declarations live in `workload.hpp`. |
| `workload.hpp` | `Q{N}Workload<Backend>`: adapter members, `OrdersLineitemPipeline<Backend> ol`, private `orders`/`lineitem` reference members, `Params` struct with `defaults()`, predicate declarations, `query_by_*` / `load` / `get_size` declarations. Ends with `#include "load.tpp"` and `#include "query.tpp"`. |
| `per_structure_workload.hpp` | **Alias-only**: `using BaseQ{N} = ::tpch::BaseStructure<Q{N}Workload<Backend>, q{N}_agg_row_t>` and siblings. All forwarder bodies are in the shared `per_structure_workload.hpp`. |
| `load.tpp` | `Q{N}Workload` ctor, `load()`, `get_size()` template bodies. Q12 is fully implemented; Q3/Q9 remain TODO stubs. |
| `query.tpp` | `query_by_*` bodies, `Params::defaults()` body, `q{N}_agg_row_t::print()` body, and predicate inline implementations. Q12 `Params::defaults()` and `print()` are done; `query_by_*` bodies are TODO. |
| `executable_rocksdb.cpp` | `main()` for RocksDB backend. Declares all needed adapters, constructs workload, dispatches on `FLAGS_storage_structure`. |
| `executable_leanstore.cpp` | Same as above, guarded by `#ifndef ROCKSDB_ONLY`, uses `LeanStoreBackend`. |
| `CLAUDE.md` | Per-query SQL, plan descriptions, execution style analysis, column index mappings, and an "Implementation Status (skeleton)" section appended when the skeleton was created. |

**Invoice-extended queries (q3i/, q5i/, q10i/)**: the workload composes
`CustomerOrdersLineitemInvoicePipeline<Backend> coli;` instead of the OL
pipeline. The `coli` member owns the 4-table COLI merged index; per-query
`load()` delegates S3 population to `coli.populate_merged()`.

## Cross-Structure Result Consistency: XOR Parity

When per-query implementations land, every `query_by_*` driver
(`query_by_base`, `query_by_view`, `query_by_merged`, `query_by_hash`) MUST
fold each result row into a running XOR parity over the row's bytes (or
canonical fields) and emit that parity alongside the result vector. The
four structures must produce **identical** parity values for the same
seed/parameters, otherwise one of them has a correctness bug.

XOR is the right choice because: (a) it's order-independent, so structures
that emit results in different orders (hash vs merge) still match; (b) it's
cheap to compute inline; (c) any single-row corruption flips the parity.

The standalone `test_load_merged_*` binaries don't need parity (they only
verify load), but they DO surface distribution stats so a wrong row count
is obvious at a glance — see `dump_merged_ol_stats` in
`test_load_merged_stats.hpp` (each line tagged `[OK]` / `[FAIL]`, with
expected ranges derived from the TPC-H spec).

## Storage-structure → Wrapper Mapping

| `--storage_structure` | Wrapper struct | Join strategy |
|-----------------------|---------------|---------------|
| 1 | `BaseQ{N}` | Traditional indexes + binary merge join |
| 2 | `ViewQ{N}` | Intermediate pipeline view (materialized `joined_ol_t` rows) |
| 3 | `MergedQ{N}` | `MI[0]` only — `PremergedJoin` at query time |
| 4 | `HashQ{N}` | Traditional indexes + hash join |
| 5 | `AggregatedQ{N}` | aCOLI MI (`MergedAdapter<customer_acoli_t, orders_acoli_t>`) — pre-aggregated fields; no accumulator pass (Q3I only) |

Structure 0 (data reload) is handled before the switch in each executable.

## Tests

Index of all TPC-H tests across this subtree. Commands live next to the
directory that owns the test sources (DRY) — this section is the entry
point. Run from the repo root.

| Test | Owner | Backend(s) | Source |
|------|-------|-----------|--------|
| `test_views_ol` | this dir | RocksDB (mac+Linux) | `tests/test_views_ol.cpp` |
| `test_views_coli` | this dir | RocksDB (mac+Linux) | `tests/test_views_coli.cpp` |
| `test_sk_matcher_compat` | this dir | RocksDB (mac+Linux) | `tests/test_sk_matcher_compat.cpp` |
| `test_load_merged_lsm` | this dir | RocksDB (mac+Linux) | `tests/test_load_merged_rocksdb.cpp` |
| `test_load_merged_btree` | this dir | LeanStore (Linux only) | `tests/test_load_merged_leanstore.cpp` |
| `test_load_coli_lsm` | this dir | RocksDB (mac+Linux) | `tests/test_load_coli_rocksdb.cpp` |
| `test_load_q12_lsm` | `q12/` | RocksDB (mac+Linux) | `tests/q12/test_load_q12_rocksdb.cpp` |
| `test_load_q12_btree` | `q12/` | LeanStore (Linux only) | `tests/q12/test_load_q12_leanstore.cpp` |
| `test_query_q12_lsm` | `q12/` | RocksDB (mac+Linux) | `tests/q12/test_query_q12_rocksdb.cpp` |
| `test_query_q12_btree` | `q12/` | LeanStore (Linux only) | `tests/q12/test_query_q12_leanstore.cpp` |
| `test_query_q3i_lsm` | `q3i/` | RocksDB (mac+Linux) | `tests/q3i/test_query_q3i_rocksdb.cpp` |
| Q3/Q9 tests | `q3/`, `q9/` | — | none yet (load/query bodies TODO) |

### Commands for this directory's tests

```bash
# Build (macOS)
make -C build/frontend test_views_ol test_load_merged_lsm \
    -j$(sysctl -n hw.ncpu)

# Build (Linux adds the leanstore variant)
make -C build/frontend test_load_merged_btree -j$(nproc)

# Run unit tests for views_ol.hpp (12 cases)
./build/frontend/test_views_ol

# Run MI[0] load test
mkdir -p test_data test_csv
./build/frontend/test_load_merged_lsm \
    --ssd_path=./test_data \
    --csv_path=./test_csv \
    --tpch_scale_factor=1
# Expected: prints MI[0] distribution stats with all [OK] tags.
```

```bash
# Build COLI load test (macOS)
make -C build/frontend test_load_coli_lsm -j$(sysctl -n hw.ncpu)

# Run COLI load test
# Each invocation needs a fresh --ssd_path (RocksDB does not cleanly
# overwrite an existing DB; use a new directory or delete the old one).
mkdir -p test_data_coli test_csv_coli
./build/frontend/test_load_coli_lsm \
    --ssd_path=./test_data_coli \
    --csv_path=./test_csv_coli \
    --tpch_scale_factor=1
# Expected: row-count, FK-resolution, and sentinel-ordering checks,
# all [OK] at SF=1.
```

**Note**: `--ssd_path` and `--csv_path` MUST be distinct directories.
RocksDB writes its info log inside `ssd_path`; the Logger calls
`create_directories(csv_path)` which fails if `csv_path` collides with
that log file. Don't reuse `--ssd_path=.` (collides with the default
`--csv_path=./log`).

### Per-query test commands

- Q12 — see [`q12/CLAUDE.md §Tests`](q12/CLAUDE.md#tests)
- Q3, Q9 — none yet
- Q3I — see [`q3i/CLAUDE.md §Tests`](q3i/CLAUDE.md#tests)

## Completed (post-skeleton)

- **Q3I S5 aCOLI MI + wrap-up** (2026-05-02): `customer_acoli_t` /
  `orders_acoli_t` added to `views_coli.hpp` (ids 49/50); `populate_aggregated()`
  3-pass algorithm in `coli_pipeline.tpp`; `query_by_aggregated` in
  `q3i/query.tpp` (no accumulators — direct field reads). All five paths
  produce identical digest at SF=1 (7 rows); `acoli_total=486` vs
  `mi_records_visited=10918` (22× scan reduction). Also landed this session:
  multi-level active markers design (`customer_active` / `order_active`)
  documented in `PLAYBOOK.md §7.1` — not yet refactored into coli_pipeline
  (current code still uses `wants_skip_group()` / `wants_skip_order()`);
  `RocksDBLogger::capture_baseline()` for SSTWrite baseline-subtract
  (documented in `PLAYBOOK.md §10`); `StageTimer` per-stage attribution and
  per-query averages in production stats output.

- **`views_ol.hpp` fully implemented** (2026-04-29): `ol_sort_key_t` (match,
  first_diff, matching_keys), `joined_ol_t` (constructors, accessors),
  `SKBuilder<ol_sort_key_t>` (create, project, to_key). Unit-tested via
  `test_views_ol.cpp` (CMake target `test_views_ol`, 12 tests).
- **Shared merge-join infrastructure refactored** (2026-04-29):
  `PremergedJoin` decoupled from record-type `get_jk()` / `Key(JK)` via
  `jk_from_variants` free function and `SKBuilder::to_key<R>`.
  `SKBuilder::get<R>` renamed to `project<R>` across all files.
  Geo backward compatibility verified (`geo_lsm` builds clean).
- **`ol_pipeline` trimmed to truly-shared scope; load-test executables added**
  (2026-04-29): `OrdersLineitemPipeline` now only owns `populate_merged` and
  `get_merged_size`. Operator drivers and view loading removed (moved to
  per-query dirs in follow-up). `test_load_merged_rocksdb.cpp` and
  `test_load_merged_leanstore.cpp` added as standalone MI[0] load-test binaries
  (CMake targets `test_load_merged_lsm` / `test_load_merged_btree`).
- **Shared `per_structure_workload.hpp` hoisted; per-query files collapse to
  aliases** (2026-04-30): `BaseStructure` / `ViewStructure` / `MergedStructure`
  / `HashStructure` templates with inline forwarder bodies moved to
  `frontend/tpch/per_structure_workload.hpp`. Q12/Q3/Q9 `per_structure_workload.hpp`
  files are now single-line `using` aliases.
- **`views.hpp` narrowed to row types; `Params` and predicates moved to
  `workload.hpp`** (2026-04-30): Q12 `Params` struct, `Params::defaults()`,
  `q12_predicate_lineitem`, and `q12_predicate_joined` now live in
  `q12/workload.hpp`. `q12/views.hpp` contains only `q12_pipeline_view_t` and
  `q12_agg_row_t`.
- **`joined_ol_t::unfoldKey` override added** (2026-04-30): `joined_ol_t`
  changed from a `using` alias to a derived struct with an explicit `unfoldKey`
  that uses the existing `joined_ol_t::Key(const ol_sort_key_t&)` constructor.
  Unblocks `RocksDBAdapter<joined_ol_t>` instantiation (pipeline view adapter).
- **Lineitem sparse-key data generation bug fixed** (2026-04-30):
  `loadPartsuppLineitem` and `loadLineitem` in `tpch_workload.hpp` now call
  `orderkey_from_index()` at all three call sites. Previously raw indices were
  used, making ~75% of lineitems orphans.
- **Q12 `load.tpp` fully implemented; `populate_q12_view` uses manual merge**
  (2026-04-30): ctor, `load()`, `get_size()`, and free function
  `populate_q12_view` all have real bodies. View loading uses a two-pointer
  manual merge (not `BinaryMergeJoin`) because the merge-join's wildcard
  semantics emitted ~1 row per order group instead of N rows per (order,
  lineitem) pair.
- **Q12 view load-test added** (2026-04-30): `test_load_merged_stats.hpp`
  refactored to return a `MergedOlStats` struct. New
  `q12/test_load_view_stats.hpp` provides `ViewStats<Backend>`,
  `dump_view_stats<Backend>`, and `compare_mi_and_view` (cross-check with
  [OK]/[FAIL] tags). New binaries `test_load_q12_rocksdb.cpp` and
  `test_load_q12_leanstore.cpp` build MI[0] + pipeline view and cross-check
  cardinality and orderkey ranges. CMake targets: `test_load_q12_lsm`
  (macOS + Linux) and `test_load_q12_btree` (Linux only). All [OK] checks
  pass at scale factor 1.
- **Q12 query bodies + cross-structure parity test** (2026-04-30):
  All four `query_by_*` methods in `q12/query.tpp` implemented (S1
  `BinaryMergeJoin` with filter-pushdown, S2 view scan + post-join
  filter, S3 `PremergedJoin` over MI[0], S4 `HashJoin` with the same
  filter-pushdown as S1). `q12_predicate_lineitem` /
  `q12_predicate_joined` implemented; the joined variant delegates to
  the lineitem variant via `j.line()` to keep S1–S4 semantically
  identical. New `test_query_q12_lsm` / `test_query_q12_btree` binaries
  run all four paths in one process and check XOR parity over the
  aggregate result rows. Parity at SF=1: all four paths produce digest
  `0x9000007000003c` (`[OK]`). Limitation: digest is over aggregate
  fields only, not per-orderkey — see `q12/CLAUDE.md §XOR parity scope`.
  Also fixed a latent variant-dispatch bug in
  `shared/merge-join/premerged_join.hpp`: the tentative-skip path now
  uses `jk_from_variants<JK>` instead of calling
  `SKBuilder<JK>::create` on variant operands.
- **F1 admission filter in PremergedJoin** (2026-04-30):
  `PremergedJoin::scan_next` accepts an optional `admit` callback; records
  rejected before emplace. `query_by_merged` threads `q12_predicate_lineitem`
  through it. Closes the ~2× merged-vs-hash perf gap to ~5–10%.
- **Q12Stats cardinality counters** (2026-04-30): `Q12Stats` struct in
  `q12/workload.hpp` tracks scanned/admitted/join/agg counters per path.
  All `query_by_*` paths increment when `stats != nullptr`; test binaries
  print `[card]` lines. Confirmed at SF=1: 6004 scanned → 30 passed → 30
  join callbacks → 2 agg rows across all four paths.
- **OPERATORS.md aligned with current code** (2026-04-30): updated §3 op 1
  (TableScan ownership moved to per-query drivers), §3 op 4 (S1/S3 named the
  actual primitives plus a new "load vs query" paragraph), §4 Q12 (unfiltered
  view + manual two-pointer merge), §5 Q12 example (concrete primitive
  instantiations), §8 pointers (trimmed pipeline contract). Added
  cross-reference comments pointing to OPERATORS.md from each per-query
  `workload.hpp` / `query.tpp` / `load.tpp` so programmers filling in
  `query_by_*` bodies land on the right operator strategy.
- **View size reporting fixed; MI/view size cross-check added** (2026-04-30):
  `RocksDB.hpp::get_size<Record>()` now uses `SizeApproximationOptions` with
  `include_memtables=true`, `include_files=true`, `files_size_error_margin=0.1`,
  fixing spurious 0.00 MiB readings for sub-CF prefix ranges at small scale
  factors. `compare_mi_and_view` in `q12/test_load_view_stats.hpp` prints an
  `[OK]/[FAIL]` bound (0.3×–5× MI[0] size). At SF=1 the view reports 0.28 MiB
  and the cross-check is `[OK]`. The `get_size` fix benefits all per-Record
  sizing calls across the codebase, not just Q12.
- **TPC-H §4.2.3 derived fields now computed** (2026-04-30): `part_t::computeRetailPrice(partkey)`
  factored out as a pure function; `lineitem_t::generateRandomRecord` now
  computes `l_extendedprice = l_quantity * p_retailprice` instead of
  `randomNumeric(0,100)`. `TPCHWorkload` accumulates a per-orderkey
  `OrderAggregate {totalprice, line_count, ostatus_count}` while generating
  lineitems; `loadOrders` then materializes orders with finalized
  `o_totalprice = Σ l_extendedprice * (1+l_tax) * (1-l_discount)` and
  `o_orderstatus` derived from lineitem statuses ('O'/'F'/'P'). `loadOrders`
  was moved after `loadPartsuppLineitem` and a new `prepopulate_order_dates`
  step seeds `order_dates` first so lineitems still have access to
  `o_orderdate`. The placeholder fields flagged in the
  `data-generator-evolution` plan are gone; only `o_clerk` and various
  `randomastring` comment fields remain placeholders (none are query-critical).
- **`invoice_t` schema + loader** (2026-05-01): `invoice_t` added to
  `tpch_tables.hpp` (keyed by `i_invoicekey`). `lineitem_t` gained
  `l_invoicekey` payload. `TPCHWorkload::loadInvoiceAndLinkLineitem()`
  runs after `loadOrders`, creates ~1.5× orders count invoices, and
  back-fills `l_invoicekey` on each lineitem.
- **COLI 4-table merged index pipeline** (2026-05-01): `views_coli.hpp`
  defines `customer_coli_t`, `orders_coli_t`, `lineitem_coli_t`,
  `invoice_coli_t` with Calcite-style tagged keys (`tagged_path` helper,
  pointer-to-member `tag_field_step` / `tag_fields2_step`). Sentinel
  tag `index=0` ensures parent rows sort before children within each key
  group (`customer=1`, `invoice=2`, `orders=3`, `lineitem=4`). Twelve
  explicit `SKMatcher<R1,R2>` specializations cover all 16 ordered pairs.
  `COLIPipeline<Backend>` in `coli_pipeline.{hpp,tpp}` owns the
  `MergedAdapter<...>` and resolves `custkey` for lineitems via an
  in-memory orderkey→custkey map built during the orders pass.
  `test_load_coli_lsm` (`tests/test_load_coli_rocksdb.cpp`) verifies row
  counts, FK resolution, and hierarchical scan order at SF=1 — all [OK].
- **Merged-adapter `accepts_key` dispatch hook** (2026-05-01, renamed from
  `matches` 2026-05-02 to disambiguate from `SKMatcher::match` join-pair
  matching): `LeanStoreMergedAdapter::toType()` now tries
  `Record::accepts_key(key, key_len)` via SFINAE before falling back
  to the `(maxFoldLength, sizeof(payload))` heuristic. Existing OL/Q12
  records opt out silently; tagged COLI records opt in via explicit
  `static bool accepts_key(...)`. Additive — existing tests pass byte-for-byte.
  `toType` also `memcpy`s the value bytes (instead of `reinterpret_cast`) to
  guard against potential alignment UB on platforms where RocksDB value
  buffers aren't 8-byte aligned.
- **`SKMatcher` abstraction + `sk_for_t` rename** (2026-05-01):
  `SKMatcher<R1,R2>` per-pair join-matching abstraction introduced in
  `frontend/shared/view_templates.hpp`. Default specialization wraps legacy
  `SKBuilder<JK>::create + JK::match` so OL pipelines require no changes.
  `SortKeyFor<R>` opt-in trait and `HasSharedSKBuilder` concept gate the
  default. Sort-key alias renamed from `sort_key_t<R>` to `sk_for_t<R>` to
  avoid collision with `geo::sort_key_t`. `test_sk_matcher_compat.cpp`
  (4 tests) proves sign/zero equivalence for OL records.
  `test_views_coli.cpp` (6 tests) covers tagged-key encoding, `matches`
  dispatch, and sentinel ordering. Both test suites pass.
  `test_query_q12_lsm` XOR-parity digest unchanged (`0x90000070006039`)
  confirming the fold-length fallback path is unaffected.
- **Data-generation load order fixed** (2026-04-30): `TPCHWorkload::load()`
  now runs `loadCustomer → loadOrders → loadPartsuppLineitem` (previously
  partsupp+lineitem ran before orders/customer). The earlier order left
  `order_dates` empty during lineitem generation, so `o_orderdate`
  defaulted to 0 and lineitem dates were in `[2, 151]` instead of
  `[orderdate+2, orderdate+151]` — collapsing Q12's receipt-date filter
  to zero matches. After the fix, `test_query_q12_lsm` reports ~25–30
  matching lineitems at SF=1 and all shape + parity checks pass.
  Spec-compliant date math in `lineitem_t::generateRandomRecord` is
  preserved.

## Q12 Completed — Lessons for Q3/Q9

### Reusable Infrastructure (copy directly)

- `OrdersLineitemPipeline<Backend>` — same MI(ORDERS, LINEITEM), same
  `populate_merged()` / `get_merged_size()`
- Join drivers: `BinaryMergeJoin`, `PremergedJoin`, `HashJoin` all typed on
  `ol_sort_key_t` / `joined_ol_t` from `views_ol.hpp`
- `TpchExecutableHelper` for throughput measurement
- Per-structure wrapper aliases (`per_structure_workload.hpp`)
- `load.tpp` structure: `tpch.load()` → `populate_view()` →
  `ol.populate_merged()`
- F1 admission filter pattern in `PremergedJoin::scan_next` via `admit`
  callback

### What must be reimplemented per query

- **View types + view loading**: Q12 uses manual two-pointer merge in
  `populate_q12_view`; each query needs its own view type and loading
  function
- **Predicates and aggregates**: Different filter conditions, different
  grouping keys, different output shape (`q{N}_agg_row_t`)
- **`Params` struct**: Different substitution parameters per query
- **Downstream joins**: Q3 needs CUSTOMER hash join; Q9 needs PART,
  SUPPLIER, PARTSUPP, NATION lookups
- **Post-aggregate processing**: Q3 needs ORDER BY + LIMIT 10; Q9 needs
  GROUP BY (nation, year) with no LIMIT

### Performance observations (SF=40)

| Structure | TX/s | SSTRead(us)/TX | Size (MiB) |
|-----------|------|----------------|------------|
| base_merge_join (S1) | 10.74 | 24845 | ~37 |
| pipeline_view (S2) | 10.40 | 31579 | 35.50 |
| mi_premerged (S3) | 11.51 | 25143 | 32.15 |
| base_hash_join (S4) | 11.21 | 26269 | ~37 |

- MI advantage modest (~7%) for 2-table join — expect similar for Q3/Q9
  since the OL pipeline dominates and extra joins are small-table lookups
- Hash beats merge because build side fits in memory — need memory-pressure
  experiments (see `TPCH_experiments.md §Memory-Pressure Experiments`)
- Pipeline view slowest — fat rows increase scan cost proportionally
- MI advantage should widen under I/O pressure (higher SF, lower DRAM)
- MI is actually *smaller* than base tables because it shares the orderkey
  prefix

### Checklist for implementing Q3

1. Fix `q3/load.tpp` — replace removed `ol.populate_view()` /
   `ol.get_view_size()` with per-query `populate_q3_view()` and local size
2. Define `q3_pipeline_view_t` (replace `using = joined_ol_t` placeholder)
3. Implement `Params::defaults()` (mktsegment=BUILDING,
   orderdate < 1995-03-15, shipdate > 1995-03-15)
4. Implement predicates: `q3_predicate_orders`, `q3_predicate_lineitem`,
   `q3_predicate_joined`
5. Implement all 4 `query_by_*` bodies — same monolithic post-join pattern
   as Q12, adding CUSTOMER hash lookup inside the callback
6. Add top-10 selection (priority queue or partial sort)
7. Add XOR parity test (`test_query_q3_lsm`)
8. Add CMake targets and `generate_targets.py` entries

## What's Needed to Fully Implement Q12/Q3/Q9

- **Q3I**: production `q3i_lsm` / `q3i_btree` executables wired into
  `frontend/CMakeLists.txt` and `generate_targets.py`. Builds clean on
  macOS; full Phase 3 done.
- **Q12**: `query_by_*` bodies, predicates, F1 admission filter
  (`PremergedJoin` admit callback plumbed through `query_by_merged`),
  and `Q12Stats` cardinality counters all implemented and tested.
  XOR-parity test (`test_query_q12_lsm` / `_btree`) passes at SF=1;
  `[card]` output confirmed: 6004 lineitems scanned, 30 admitted/passed,
  30 join callbacks, 2 aggregator rows across all four paths.
  Performance ordering `base ≈ hash < merged < view` is stable.
  **Remaining**: `q12_lsm` / `q12_btree` flag-dispatch executables,
  CMake targets, and `generate_targets.py` Makefile entries.
- **Q3/Q9**: `load.tpp` ctor/`load()`/`get_size()` bodies (still reference
  removed pipeline methods — fix first). Then `query_by_*` bodies, predicate
  implementations, and `Params::defaults()`.
- Per-query predicate / projection / aggregator bodies inside `query.tpp` for
  Q3 and Q9.
- Q3-specific: CUSTOMER merge join (sort by `o_custkey`, two-pointer scan of
  filtered CUSTOMER) after the OL aggregate, then top-10 sort.
- Q9-specific: NATION and SUPPLIER hashmap construction before the OL scan;
  PART and PARTSUPP merge joins inside the per-row callback; LIKE filter on
  `p_name` applied immediately after PART lookup.
- CMake targets: `q12_lsm`, `q12_btree`, `q3_lsm`, `q3_btree`, `q9_lsm`,
  `q9_btree` — add to `frontend/CMakeLists.txt` following the `geo_lsm` /
  `geo_btree` pattern. (Load-test targets `test_load_q12_lsm` /
  `test_load_q12_btree` are already added.)
- `generate_targets.py` Makefile entries for the new targets.

## Known Design Limitations

### No project pushdown below secondary-structure loading

Today, every secondary structure — merged indexes (MI[0], COLI MI),
COLI custkey-sorted secondaries, and per-query pipeline views —
stores **the full base record** for each row it carries. Filter
pushdown into the load path is also avoided on purpose so that
secondaries stay reusable across param sets (see Q12 §4 predicate
hoisting, Q3I `plans/family_logical.dot`). Project pushdown is the
companion optimisation that we have **not** taken: the secondaries
materialise every column whether the queries need it or not.

Concretely:

- `MI[0]` (Q12 OL merged index) stores full `orders_t` and full
  `lineitem_t` records, even though Q12's `query_by_*` only reads
  `o_orderpriority`, `o_orderkey`, `l_shipmode`, `l_orderkey`,
  `l_commitdate`, `l_receiptdate`, `l_shipdate`.
- `MI[COLI]` (4-table) stores full `customer_coli_t`, `orders_coli_t`,
  `lineitem_coli_t`, `invoice_coli_t`, even though Q3I (the first
  consumer) only touches a small projection from each.
- `q12_pipeline_view_t`, `q3i_pipeline_view_t` (planned), and the COLI
  secondaries (`Adapter<orders_coli_t>` etc.) all carry the full
  source-record payload for the same reason.

This is intentional for the current paper — keeping secondaries
schema-faithful makes them reusable across query variants and keeps
the comparison axis clean (S1/S2/S3 read the same logical row, only
the physical operator differs). But it also means MI scans pay a
cache-line cost proportional to the **widest** consumer's record,
not the narrowest active query's projection.

**Worth exploring later, especially under any of these triggers:**

1. **Cache-bound MI scans become a bottleneck** at scale factors
   where the secondary's row width dominates wall-clock time. We can
   measure this directly: profile L2/L3 miss rate against a
   projected-payload variant.
2. **A query needs only a narrow projection** of a wide base record
   (e.g. a future query that only reads `c_acctbal` from
   `customer_coli_t`'s 200+-byte payload).
3. **Materialised pipeline views balloon at high SF** because their
   row width is the union of every column the family logical plan
   touches. Per-query projection-pushed views could shrink them
   substantially.

The principled fix is a per-secondary projection schema that ships
only the columns the query family actually reads, with a
`from_base()` helper analogous to `*_coli_t::from_base` but
projecting rather than just retagging. Compile-time templating on
the projection would keep the comparison axis honest (S1 reads the
same projected record as S3). Calcite's `EnumerableProject` already
expresses these projections at the planner level — wiring them
through to the secondary-loading path is the missing piece.

Until then: when adding a new query that touches a merged index, do
not optimise this prematurely. Note the projection cost in the
per-query CLAUDE.md §Implementation Status and revisit as part of
performance-tuning, not initial bring-up.

## Out of Scope (Skeleton)

The following are explicitly deferred and not part of this skeleton:

- Update / maintenance paths (RF1 insert, RF2 delete).
- Tagged-row format migration for OL records (fold-length discrimination
  is sufficient for Q12/Q3/Q9; COLI already uses tagged keys — see
  §Completed above).
- `joined_t` flattening (the generic `joined_t` template from
  `frontend/shared/view_templates.hpp` is used as-is).
- Wiring `COLIPipeline` into Q5I / Q10I workloads (pipeline exists and
  load-tested; Q3I is the first consumer, pending Phase 1 bodies).

## Invoice Extension Candidates

`INVOICE_EXTENSION_CANDIDATES.md` (this directory) is the **authoritative
record** of which TPC-H queries are natural COLI MI extension candidates and
which are not — including the rationale for each decision. Read it before
proposing new `q{N}i/` directories.

Q3I is a §3.1.2 sibling-aggregate showcase, not a true 4-way M:N join — see
[q3i/CLAUDE.md §Cardinality structure](q3i/CLAUDE.md#cardinality-structure-not-a-true-4-way-mn).
