# TPC-H Tier 1 Workload Guide

Top-level reference for the Q12 / Q3 / Q9 skeleton implementation under
`frontend/tpch/`. Read this before touching any per-query directory.

**Reading guide**: If you need current query status or the Q3I perf
headline, read [`STATUS.md`](STATUS.md) only. If you need historical
completed-work log or Q12 lessons, read [`HISTORY.md`](HISTORY.md). If
you need cross-cutting conventions (wildcard semantics, OutClass rule,
anti-patterns), read [`CONVENTIONS.md`](CONVENTIONS.md). If you need the
full new-query bring-up walkthrough, read [`PLAYBOOK.md`](PLAYBOOK.md).
The sections below are timeless reference — read once, then navigate by
sibling doc.

## Sibling Docs

Every non-`CLAUDE.md` Markdown file at this level. Read each on the
trigger described:

- [`STATUS.md`](STATUS.md) — read **when you need current per-query
  implementation state** (which queries are done, what's pending, Q3I
  perf headline). Updated in place; not a log.
- [`HISTORY.md`](HISTORY.md) — read **when reconstructing a prior
  decision** or reviewing completed-work context (append-only log of
  landed features, Q12 lessons, historical perf tables).
- [`CONVENTIONS.md`](CONVENTIONS.md) — read **before writing any sort-key,
  OutClass, or anti-pattern-sensitive code**: wildcard semantics
  (`WILDCARD_KEY` / `wildcard_match` / `matching_keys`), post-pipeline
  OutClass contract, performance instrumentation reference, size
  diagnostics pattern, anti-pattern table, contract violations rule.
- [`OPERATORS.md`](OPERATORS.md) — read **before filling in any
  `query_by_*` body**: monolithic-vs-cascade translation rules from
  Calcite operators to C++, primitive instantiations, comparison-axis
  contract.
- [`PLAYBOOK.md`](PLAYBOOK.md) — read **when implementing a new TPC-H
  query end-to-end** across all four storage structures (uses Q3I as
  the canonical reference template).
- [`INVOICE_EXTENSION_CANDIDATES.md`](INVOICE_EXTENSION_CANDIDATES.md)
  — read **before proposing a new `q{N}i/` invoice-extended query**:
  authoritative record of which TPC-H queries are natural COLI MI
  candidates and which are not, with rationale.
- [`MULTI_TABLE_MI_ANALYSIS.md`](MULTI_TABLE_MI_ANALYSIS.md) — read
  **when picking which pipeline a new MI should target**: argues for
  >2-table MIs over the 2-table OL pipeline, framed against the
  paper's "combinatorial advantage" claim.

For supplemental cross-cutting context, the top-level
[`TPCH_experiments.md`](../../TPCH_experiments.md) owns the experiment
matrix (memory-pressure design, reviewer-concern mapping) and indexes
per-query CLAUDE.md docs.

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
- `tpch_family/` — Track-1 vanilla-set-shared substrate (used by all
  vanilla queries Q3 / Q5 / Q10 / Q12 / Q9, independent of pipeline
  choice). See [`tpch_family/CLAUDE.md`](tpch_family/CLAUDE.md) for
  the full membership decision tree and file inventory. Contents:
  `views_ol.hpp`, `views_col.hpp`, `views_coli.hpp`,
  `ol_pipeline.{hpp,tpp}`, `col_pipeline.{hpp,tpp}`. Note:
  `views_coli.hpp` lives here despite its name because half its types
  (`customer_coli_t`, `orders_coli_t`) are reused by Q3 vanilla; a
  future Phase-3 split moves the genuinely-extended bits to
  `tpchi_family/views_invoice.hpp`.
- `test_views_ol.cpp` — 12 unit tests for sort key ordering, match semantics,
  `SKBuilder` round-trips, `joined_ol_t` construction. CMake target:
  `test_views_ol` (macOS/RocksDB-only build).
- `backend.hpp` — `RocksDBBackend` and `LeanStoreBackend` traits structs.
- `tpch_family/views_coli.hpp` — Calcite-style tagged record types for the
  CUSTOMER × ORDERS × LINEITEM × INVOICE 4-table merged index:
  `customer_coli_t`, `orders_coli_t`, `lineitem_coli_t`, `invoice_coli_t`.
  Also defines the aCOLI (pre-aggregated) 3-type set used by Q3I S5:
  `customer_acoli_t` (id=49, tagged key, adds `pre_open_due`),
  `orders_coli_t` (id=1, **reused** — `orders_acoli_t` was retired Step 4b
  2026-05-03 because after tagged-key adoption its key bytes are identical to
  `orders_coli_t`; id=50 is reserved/retired), and
  `lineitem_acoli_t` (id=53, tagged key, no `invoicekey` segment — revenue
  recomputed at query time). The aCOLI `MergedAdapter` is therefore
  `MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>`.
  All three aCOLI-family keys use `tagged_path` encoding (uniform with COLI)
  and carry `accepts_key` dispatch hooks that read the trailing `idx_id` byte
  (49, 1, 53 — all distinct). `orders_acoli_t` previously used plain
  `keyfold`; the collapse to `orders_coli_t` was blocked until tagged-key
  adoption made the bytes identical.
  Each `Key` carries a `using path = tagged_path<IdxId, Steps...>` declaration
  (pointer-to-member steps: `tag_field_step`, `tag_fields2_step`) plus a
  `static bool accepts_key(const u8*, size_t)` hook that reads the trailing
  `idx_id` byte. Sentinel tag `index=0` sorts before all domain tags
  (`customer=1`, `invoice=2`, `orders=3`, `lineitem=4`). Within a custkey
  group the byte-lex order is customer → invoice → orders → lineitems:
  invoice (t=2) precedes orders/lineitems (t=3/4) so consumers finalize
  per-custkey sub-aggregates (e.g. `cust_open_due`) before bulk O×L
  records stream by (§3.1.2 sibling pattern). Twelve explicit
  `SKMatcher<R1,R2>` specializations cover all 16 ordered pairs (10 unique +
  reverse delegations).
- `tpchi_family/` — Track-2 invoice-extended-set-shared substrate
  (used by Q3I, Q5I, Q10I — composes on top of `tpch_family/`).
  See [`tpchi_family/CLAUDE.md`](tpchi_family/CLAUDE.md). Contents:
  `tpchi_tables.hpp` (invoice schema), `tpchi_workload.hpp`
  (`TPCHIWorkload` with `loadInvoiceAndLinkLineitem`), and
  `coli_pipeline.{hpp,tpp}` (`COLIPipeline<Backend>` —
  `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t,
  invoice_coli_t>`, dual-write replay resolving `custkey` via an
  in-memory orderkey→custkey map built during the orders pass).
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

Shared scaffolding for the Q3 query family:

- `q3_family/` — building blocks shared by Q3 and Q3I (and future Q3-flavoured
  queries). Full symbol inventory in [`q3_family/CLAUDE.md`](q3_family/CLAUDE.md).
  Contains seven headers:

  - `accumulators.hpp` — `LineitemRevenueAccumulator<P>` templated on the
    per-query Params type, with overloads for `lineitem_t`, `lineitem_coli_t`,
    and `lineitem_acoli_t`.
  - `predicates.hpp` — the three spec-shared filters: `q3_predicate_customer`,
    `q3_predicate_orders`, `q3_predicate_lineitem`, each templated on Params.
  - `params.hpp` — 10-entry SUBSTITUTION-PARAMETER rotation table covering all
    5 segments × 5 March-15 dates in [1993, 1997]; `set_params_for_iter`
    consumer pattern.
  - `agg_row.hpp` — `q3_agg_row_base_t`: the four base output fields
    `o_orderkey`, `revenue`, `o_orderdate`, `o_shippriority` plus
    `print_base()`, `print()`, and the `cmp` comparator for `apply_topN`.
    Q3 aliases this directly; Q3I derives `q3i_agg_row_t` from it to add
    `cust_open_due`. Hoisted Phase A2.
  - `stats.hpp` — `Q3FamilyStats`: cardinality and timing counters shared by
    Q3 and Q3I. Q3 uses it directly; Q3I's `Q3IStats` derives from it and
    adds invoice-specific counters. Hoisted Phase A3.
  - `coli_visitors.hpp` — `Q3FamilyVisitor<Derived, Params, LineitemType,
    AggRow, Stats>`: CRTP base implementing the C×O×L core of the group-walk
    visitor. Three customisation hooks (`per_order_admit_check`,
    `extra_emit_fields`, `on_record_visited_hook`) let Q3I overlay
    invoice/threshold logic while Q3 uses base no-op defaults. Hoisted Phase A4.
  - `view_loaders.hpp` — `populate_q3_view_core<…>`: two-pointer merge kernel
    over orders × lineitem; caller provides an emit callback. Q3I's callback
    attaches `cust_open_due`; Q3's callback omits it. Hoisted Phase A5.

  Q3I's `query.tpp` aliases `LineitemRevenueAccumulator` from here and
  delegates its predicate bodies to the family functions; Q3 will wire up at
  Phase 0.5 when its bodies land. Q3I-only pieces (`CustomerOpenDueAccumulator`,
  invoice predicates, `COLIGroupWalkVisitor`) remain in `q3i/`.

Per-query subdirectories:

- `q12/` — Shipping Modes and Order Priority (2 tables, 1 join).
- `q3/`  — Shipping Priority (3 tables, 2 joins; adds CUSTOMER adapter).
- `q5/`  — Local Supplier Volume (3 tables in the COL chain + 3 small
  dimension tables — REGION, NATION, SUPPLIER — as reduce-side hash
  builds). §3.1.3 pure-hierarchical sibling to Q3; S1–S4 implemented
  and parity-verified at SF=1 (no S5 — see
  `q5/CLAUDE.md §Storage Structure Options`). Production targets
  wired; Linux perf sweep pending (`LINUX_PENDING.md`).
- `q9/`  — Product Type Profit Measure (6 tables, 5 joins; adds NATION,
  SUPPLIER, PART, PARTSUPP adapters).
- `q10/` — Returned Item Reporting (4 tables: CUSTOMER × ORDERS ×
  LINEITEM in the COL chain + NATION as a per-customer INL on PK).
  §3.1.3 pure-hierarchical sibling to Q3 / Q5; per-customer top-20
  shape (the only Track-1 query with that grouping). **Phase 0
  design doc complete**; skeleton + bodies pending. Experimental
  scope: 5L cell only (paper-axis headline). No S5 — same
  soundness-rule argument as Q5. See
  [`q10/CLAUDE.md §Implementation Phases`](q10/CLAUDE.md).
- `q3i/` — Q3 + Invoice sibling aggregate (COLI MI showcase; S1–S4
  are the paper-reported axis and parity-verified. S5 aCOLI MI is
  implemented and parity-verified but **deferred from the paper
  sweep** — see [`PLAYBOOK.md §S5`](PLAYBOOK.md). Full status in
  `q3i/CLAUDE.md §Implementation Phases`).
- `q5i/` — Q5 + Invoice payment-status split. S1–S4 paper-reported
  axis, parity-verified at SF=1 (strict 4-way XOR digest). S5 deferred
  (PLAYBOOK §S5). Production `q5i_lsm` / `q5i_btree` targets wired;
  Linux perf sweep pending (`LINUX_PENDING.md`). See
  [`q5i/CLAUDE.md §Implementation Status`](q5i/CLAUDE.md). Canonical
  Pattern B reference (view loader reuses S3 group-walk).
- `q10i/` — Q10 + return-payment-status partition (paid/open/late
  buckets on returned revenue, joined via `l_invoicekey =
  i_invoicekey`). **Phase 4 complete** on worktree branch
  `worktree-agent-a86745538133069de`: all four `query_by_*` bodies
  parity-verified at SF=1, SF=5, SF=10 (strict 4-way XOR digest).
  **Worktree-only — does NOT merge to main until paper revision is
  submitted**, because `lineitem_coli_t` was widened with
  `l_returnflag` (D14) and that invalidates Q3I/Q5I persisted images
  on main. See [`q10i/CLAUDE.md §Status`](q10i/CLAUDE.md).

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

> **TODO debt (Q9 only)**: `q9/load.tpp` still references `ol.populate_view`
> and `ol.get_view_size`, which were removed from the pipeline. This will
> be fixed when Q9 per-query load bodies land. Q12 and Q3 `load.tpp` are
> fully implemented and no longer have this debt (Q3 uses
> `CustomerOrdersLineitemPipeline` instead of OL; its view loader
> `populate_q3_view` lives in `q3/load.tpp`).

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

Three pipelines exist:

- `OrdersLineitemPipeline<Backend>` (`tpch_family/ol_pipeline.hpp`) — 2-table OL
  merged index. Q12, Q3, and Q9 each hold exactly one instance named `ol`.
- `COLIPipeline<Backend>` (`tpchi_family/coli_pipeline.hpp`) — 4-table
  COLI merged index (CUSTOMER × ORDERS × LINEITEM × INVOICE). Uses
  tagged-key format with `tpch_family/views_coli.hpp` types. Wired into
  Q3I; standalone load-test passes at SF=1.
- `CustomerOrdersLineitemPipeline<Backend>` (`tpch_family/col_pipeline.hpp`)
  — 3-table COL merged index (CUSTOMER × ORDERS × LINEITEM). Strict subset
  of COLI: reuses `customer_coli_t` and `orders_coli_t` verbatim; introduces
  `lineitem_col_t` (no `invoicekey` key segment). Uses tagged-key format with
  `tpch_family/views_col.hpp` types. Wired into Q3 (full bodies) and Q5
  (skeleton).

## Per-query File Convention

Each `q{N}/` directory contains:

| File | Responsibility |
|------|---------------|
| `views.hpp` | Row-shape types only: `q{N}_pipeline_view_t` alias and `q{N}_agg_row_t` with key and payload. `Params` struct and predicate declarations live in `workload.hpp`. |
| `workload.hpp` | `Q{N}Workload<Backend>`: adapter members, `OrdersLineitemPipeline<Backend> ol`, private `orders`/`lineitem` reference members, `Params` struct with `defaults()`, predicate declarations, `query_by_*` / `load` / `get_size` declarations. Ends with `#include "load.tpp"` and `#include "query.tpp"`. |
| `per_structure_workload.hpp` | **Alias-only**: `using BaseQ{N} = ::tpch::BaseStructure<Q{N}Workload<Backend>, q{N}_agg_row_t>` and siblings. All forwarder bodies are in the shared `per_structure_workload.hpp`. |
| `load.tpp` | `Q{N}Workload` ctor, `load()`, `get_size()` template bodies. Q12 and Q3 are fully implemented; Q9 remains TODO stubs. |
| `query.tpp` | `query_by_*` bodies, `Params::defaults()` body, `q{N}_agg_row_t::print()` body, and predicate inline implementations. Q12, Q3, and Q3I are fully implemented (cross-structure XOR parity verified at SF=1); Q9 `query_by_*` bodies remain TODO. |
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
| 5 | `AggregatedQ{N}` | aCOLI MI (`MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>`) — `pre_open_due` read directly; revenue recomputed from unaggregated lineitems (Q3I only). **Deferred from paper sweep** — see [`PLAYBOOK.md §S5`](PLAYBOOK.md). |

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
| `test_load_col_lsm` | `q3/` | RocksDB (mac+Linux) | `tests/q3/test_load_col_lsm.cpp` |
| `test_load_col_btree` | `q3/` | LeanStore (Linux only) | `tests/q3/test_load_col_leanstore.cpp` |
| `test_views_acoli` | this dir | RocksDB (mac+Linux) | `tests/test_views_acoli.cpp` |
| `test_projection_widths` | this dir | RocksDB (mac+Linux) | `tests/test_projection_widths.cpp` |
| `test_q3_family` | this dir | RocksDB (mac+Linux) | `tests/test_q3_family.cpp` |
| `test_lineitem_i_upgrade` | this dir | RocksDB (mac+Linux) | `tests/test_lineitem_i_upgrade.cpp` |
| `test_workload_load_ext` | this dir | RocksDB (mac+Linux) | `tests/test_workload_load_ext.cpp` |
| `test_load_q12_lsm` | `q12/` | RocksDB (mac+Linux) | `tests/q12/test_load_q12_rocksdb.cpp` |
| `test_load_q12_btree` | `q12/` | LeanStore (Linux only) | `tests/q12/test_load_q12_leanstore.cpp` |
| `test_query_q12_lsm` | `q12/` | RocksDB (mac+Linux) | `tests/q12/test_query_q12_rocksdb.cpp` |
| `test_query_q12_btree` | `q12/` | LeanStore (Linux only) | `tests/q12/test_query_q12_leanstore.cpp` |
| `test_query_q3i_lsm` | `q3i/` | RocksDB (mac+Linux) | `tests/q3i/test_query_q3i_rocksdb.cpp` |
| `test_query_q3i_btree` | `q3i/` | LeanStore (Linux only) | `tests/q3i/test_query_q3i_leanstore.cpp` |
| `test_query_q3_lsm` | `q3/` | RocksDB (mac+Linux) | `tests/q3/test_query_q3_rocksdb.cpp` |
| `test_query_q3_btree` | `q3/` | LeanStore (Linux only) | `tests/q3/test_query_q3_leanstore.cpp` |
| `test_query_q5_lsm` | `q5/` | RocksDB (mac+Linux) | `tests/q5/test_query_q5_rocksdb.cpp` |
| `test_query_q5_btree` | `q5/` | LeanStore (Linux only) | `tests/q5/test_query_q5_leanstore.cpp` |
| `test_side_tables` | `q5/` | RocksDB (mac+Linux) | `tests/q5/test_side_tables.cpp` |
| `test_query_q10_lsm` | `q10/` | RocksDB (mac+Linux) | `tests/q10/test_query_q10_rocksdb.cpp` |
| `test_query_q10_btree` | `q10/` | LeanStore (Linux only) | `tests/q10/test_query_q10_leanstore.cpp` |
| `test_query_q10i_lsm` | `q10i/` | RocksDB (mac+Linux) | `tests/q10i/test_query_q10i_rocksdb.cpp` |
| `test_query_q10i_btree` | `q10i/` | LeanStore (Linux only) | `tests/q10i/test_query_q10i_leanstore.cpp` |
| Q9 tests | `q9/` | — | none yet (load/query bodies TODO) |

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
# Scratch convention: keep all RocksDB/CSV scratch under build/scratch/<tag>/
# (build*/ is gitignored). Do NOT create test_data*/ test_csv*/ in the project
# root — they accumulate and pollute the working tree.
mkdir -p build/scratch/merged_ol/{data,csv}
./build/frontend/test_load_merged_lsm \
    --ssd_path=./build/scratch/merged_ol/data \
    --csv_path=./build/scratch/merged_ol/csv \
    --tpch_scale_factor=1
# Expected: prints MI[0] distribution stats with all [OK] tags.
```

```bash
# Build COLI load test (macOS)
make -C build/frontend test_load_coli_lsm -j$(sysctl -n hw.ncpu)

# Run COLI load test
# Each invocation needs a fresh --ssd_path (RocksDB does not cleanly
# overwrite an existing DB; use a new directory or delete the old one).
mkdir -p build/scratch/coli/{data,csv}
./build/frontend/test_load_coli_lsm \
    --ssd_path=./build/scratch/coli/data \
    --csv_path=./build/scratch/coli/csv \
    --tpch_scale_factor=1
# Expected: row-count, FK-resolution, and sentinel-ordering checks,
# all [OK] at SF=1.
```

```bash
# Build COL load test (macOS)
make -C build/frontend test_load_col_lsm -j$(sysctl -n hw.ncpu)

# Run COL load test (fresh directory required — same RocksDB constraint)
mkdir -p build/scratch/col/{data,csv}
./build/frontend/test_load_col_lsm \
    --ssd_path=./build/scratch/col/data \
    --csv_path=./build/scratch/col/csv \
    --tpch_scale_factor=1
# Expected: customer/orders/lineitem counts, FK-resolution, hierarchical
# order, and split-index checks — all [OK] at SF=1.
```

**Note**: `--ssd_path` and `--csv_path` MUST be distinct directories.
RocksDB writes its info log inside `ssd_path`; the Logger calls
`create_directories(csv_path)` which fails if `csv_path` collides with
that log file. Don't reuse `--ssd_path=.` (collides with the default
`--csv_path=./log`).

**Scratch directory convention**: every RocksDB-backed test/load run
needs a fresh `--ssd_path` (RocksDB does not cleanly overwrite an
existing DB). Put scratch under `build/scratch/<tag>/{data,csv}` —
`build*/` is gitignored, so this never lands in the project root.
**Do not** create `test_data_*/` or `test_csv_*/` in the project root;
those have accumulated by the dozen in past sessions and have all
been moved to `TRASH/`.

### Per-query test commands

- Q12 — see [`q12/CLAUDE.md §Tests`](q12/CLAUDE.md#tests)
- Q3 load — `test_load_col_lsm` (see commands above)
- Q3 query — `test_query_q3_lsm` (Phase-0.5: digests all 0x0, `[OK]` parity)
- Q5 — see [`q5/CLAUDE.md §Tests`](q5/CLAUDE.md#tests) for
  `test_query_q5_{lsm,btree}` (strict S1–S4 XOR parity at SF=1) and
  `test_side_tables` (REGION/NATION/SUPPLIER hashmaps).
- Q10 — `test_query_q10_lsm` (Phase 1 commit 2: stubs return empty,
  digests all 0x0, `[OK]` parity at SF=1; strict-equality cardinality
  + sentinel-ordering land in commit 3)
- Q10I — `test_query_q10i_lsm` (Phase 1: stubs return empty, digests
  all 0x0, `[OK]` parity at SF=1; full strict-equality cardinality
  + sentinel-ordering checks already in place; **worktree-only** —
  not on main, see q10i/CLAUDE.md §Status)
- Q9 — none yet (load/query bodies TODO)
- Q3I — see [`q3i/CLAUDE.md §Tests`](q3i/CLAUDE.md#tests)

## Project pushdown — primary indexes are full, secondaries are projected

**Rule** (resolved 2026-05-03; G8 audit applied across TPCH):

- A merged index whose participating sub-index is a **primary index
  of its rows in the active storage variant** carries the **full
  base record**. It *is* the primary storage of those rows; pruning
  columns would mean information loss.
- Every other secondary structure — pipeline views, pre-aggregated
  MIs (aCOLI), co-located secondaries inside a primary-anchored MI —
  carries **only the columns required by queries** that consume it.
- When a future query needs an additional column from a secondary,
  *modify the existing record type in place* to add the field. Do
  not fork a wider variant.

The primary-vs-secondary axis is **per storage variant**: a record
type can be the primary in one variant and a secondary in another
(see `customer_acoli_t` below). The same record type may also be
shared across MI and split-adapter layouts; projecting once narrows
both paths.

This is **project pushdown** complementing **filter pushdown**:
parameterised filters are still avoided at load time so secondaries
remain reusable across param sets, but column projection is applied
at load time because pruning unused columns shrinks scan cost
without losing reusability inside the consuming query family.

| Type | Variant | Role | Columns carried |
|------|---------|------|----------------|
| `orders_t` / `lineitem_t` in `MI[0]` | Q12 S3 | **Primary** of orders + lineitem (in MI form) | Full base records |
| `customer_coli_t` (id=30) | COLI MI (S3) and split (S1) | **Primary** of customer | Full `customerh_t` payload |
| `orders_coli_t` (id=31) | COLI MI (S3) and split (S1) | **Secondary** | `{o_orderdate, o_shippriority}` (G8a) |
| `lineitem_coli_t` (id=32) | COLI MI (S3) and split (S1) | **Secondary** | `{l_extendedprice, l_discount, l_shipdate}` (G8a) |
| `invoice_coli_t` (id=33) | COLI MI (S3) and split (S1) | **Secondary** | `{i_totaldue, i_status}` (G8a) |
| `customer_acoli_t` (id=49) | aCOLI MI (Q3I S5) | **Primary** of customer in S5 (S3 not present) | Full `customerh_t` + `pre_open_due` |
| ~~`orders_acoli_t` (id=50)~~ | **Retired** (Step 4b, 2026-05-03) | Collapsed into `orders_coli_t` after tagged-key adoption made keys identical | id=50 reserved; use `orders_coli_t` (id=1) |
| `orders_coli_t` (id=1) | aCOLI MI (Q3I S5) — **reused** | **Secondary** | `{o_orderdate, o_shippriority}` (G8a/G8b — same projection) |
| `lineitem_acoli_t` (id=53) | aCOLI MI (Q3I S5) | **Secondary** | `{l_extendedprice, l_discount, l_shipdate}` (G8c) |
| `q12_pipeline_view_t` (id=34) | Q12 S2 view | **Secondary** (pre-aggregated) | `{l_shipmode, o_orderpriority, l_shipdate, l_commitdate, l_receiptdate}` (G8d) |
| `q3i_pipeline_view_t` (id=43) | Q3I S2 view | **Secondary** (pre-aggregated) | `{l_extendedprice, l_discount, l_shipdate, cust_open_due, c_mktsegment, o_orderdate, o_shippriority}` |

`joined_ol_t` is a **join-output** type used by `PremergedJoin` /
`BinaryMergeJoin` / `HashJoin` at query time, not a stored secondary;
it carries full base payloads by construction (the join operators
need them) and is out of scope for this rule.

### Worth widening *only* under these triggers

1. **A real future query needs an additional column.** Modify the
   existing record type in place, add a field, re-load.
2. **A new secondary structure consumes the same data with a
   superset of column requirements.** Same response — extend, not
   fork.

Speculative widening on the rationale that "some future query might
want it" is exactly what this rule rejects.

## Tagged-key uniformity within a MergedAdapter family

**Rule** (landed Step 4b, 2026-05-03):

Every record type co-located inside a `MergedAdapter` family that uses
tagged-key dispatch **must itself use tagged-key encoding
(`tagged_path<…>`)**.  Mixing tagged and plain-fold record types within
one `MergedAdapter` breaks variant dispatch in the merged scanner:
`accepts_key` hooks rely on reading a trailing `idx_id` byte whose
position is only stable when every co-resident key is encoded with the
same `tagged_path` scheme.

**Carve-out**: the OL pipeline (`MI[0]` for Q12/Q3/Q9) keeps
fold-length discrimination because the base TPC-H tables (`orders_t`,
`lineitem_t`) cannot carry COLI domain tags without modifying the
vanilla schema — which would break the schema split between vanilla
and invoice-extended workloads.  Fold-length discrimination is safe
there because the two key sizes (4 bytes vs 8 bytes) are permanently
distinct by TPC-H spec.

**Consequence that motivated this rule**: `orders_acoli_t` previously
used plain `keyfold` (8-byte key) while `customer_acoli_t` and
`lineitem_acoli_t` used `tagged_path`.  This mixed encoding made
`accepts_key` dispatch unreliable for `orders_acoli_t` rows.  Switching
all three aCOLI types to `tagged_path` made `orders_acoli_t::Key`
byte-identical to `orders_coli_t::Key`, enabling the type collapse
(`using orders_acoli_t = orders_coli_t`).

## Contract violations & fail-fast

Operators that detect a contract violation (illegal hook returns, malformed
records, broken invariants) **must throw**, not silently continue — see
[`CONVENTIONS.md §Contract violations & fail-fast`](CONVENTIONS.md#contract-violations--fail-fast).

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

Q3I S3 perf investigation tracked in
[q3i/PERFORMANCE.md](q3i/PERFORMANCE.md). See [`STATUS.md`](STATUS.md)
for the headline result.
