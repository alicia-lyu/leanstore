# Session Progress: TPC-H Tier 1 Queries (Q12, Q3I, Q5I, Q10I)

## Status

Q12 four-structure implementation complete (XOR parity passing, SF=40 benchmarked).
COLI 4-table merged index infrastructure landed and load-tested (`test_load_coli_lsm` passes at SF=1).
Q3I Phase 1 (S3 merged path) complete — `query_by_merged` returns ~129 rows at SF=1, exit criterion satisfied.
Active work: Q3I Phase 2 (baseline S1/S2/S4 paths + XOR parity) and Q3I Phase 3 (top-10 + CMake targets).
Q5I/Q10I are design-doc only; skeletons pending Q3I Phase 3.
Stock-TPC-H track (Q3/Q9 baseline structures) reframed per Goetz feedback to add a stock-TPC-H comparison arm (commit `2ad654ee`).

## Scope

Current paper: each Tier 1 query uses **at most one merged index** interleaving the source tables of a single pipeline.
The four storage structure variants per query are:

| # | Strategy | Description |
| - | -------- | ----------- |
| 1 | Traditional + merge join | Separate per-table B-trees, merge join on shared sort key |
| 2 | Materialized view | Intermediate pipeline view (join output of the MI-source pipeline) |
| 3 | Single merged index | `PremergedJoin` at query time |
| 4 | Clustered + hash join | Traditional indexes, hash join chain at query time |

The master experiment plan is [`TPCH_experiments.md`](./TPCH_experiments.md).
Multi-MI pipeline cascades are **next-paper scope**.

## Completed

- **Q12/Q3/Q9/Q3I skeletons** — full 7-file shape under `frontend/tpch/q{N}/`.
- **Shared substrate** — `backend.hpp`, `ol_pipeline.hpp`, `views_ol.hpp` (fully implemented + 12 unit tests),
  `per_structure_workload.hpp` (shared `BaseStructure`/`ViewStructure`/`MergedStructure`/`HashStructure` templates),
  `tpch_executable.hpp` (`dispatch_storage_structure` helper).
- **Spec-compliant TPC-H data generation** — `tpch_tables.hpp` / `tpch_workload.hpp` rewritten: sparse orderkeys,
  date-correlated lineitem fields, `o_totalprice` / `o_orderstatus` derived from lineitems, correct load order
  (`loadCustomer → loadOrders → loadPartsuppLineitem`). `randomNumeric` fixed (was dividing by 2^31, yielding
  ~1e8 magnitudes instead of `[0, 0.1]`).
- **Q12 fully implemented** — all four `query_by_*` paths, F1 admission filter in `PremergedJoin`, `Q12Stats`
  cardinality counters, XOR-parity test (`test_query_q12_lsm`) passing at SF=1 (digest `0x90000070006039`).
  SF=40 benchmark: MI ~7% ahead of base merge join; hash beats merge at low DRAM (fits in memory).
  **Remaining**: `q12_lsm` / `q12_btree` flag-dispatch executables + CMake targets + `generate_targets.py` entries.
- **`invoice_t` schema + loader** — `tpch_tables.hpp`; `lineitem_t` gains `l_invoicekey`;
  `TPCHWorkload::loadInvoiceAndLinkLineitem()` back-fills FK links.
- **COLI 4-table merged index** — `views_coli.hpp` (tagged-key types with `tagged_path`, `accepts_key`,
  sentinel ordering `customer=1 < invoice=2 < orders=3 < lineitem=4`); `COLIPipeline<Backend>`
  (`coli_pipeline.hpp/.tpp`); `test_load_coli_lsm` all [OK] at SF=1.
- **`SKMatcher` + `accepts_key` dispatch** — `SKMatcher<R1,R2>` per-pair join-matching abstraction
  (`view_templates.hpp`); `LeanStoreMergedAdapter::toType` tries `Record::accepts_key` via SFINAE before
  fold-length fallback; `sk_for_t<R>` rename (was `sort_key_t`). Tests: `test_sk_matcher_compat` (4),
  `test_views_coli` (6). Q12 parity digest unchanged — fold-length path unaffected.
- **Defensive `memcpy` in `variant_utils::toType`** — guards against alignment UB on platforms where
  RocksDB value buffers are not 8-byte aligned (commit `d8980426`).
- **Q3I Phase 1 (S3 merged path)** — `query_by_merged` with `coli_group_walk` Visitor,
  `CustomerOpenDueAccumulator` + `LineitemRevenueAccumulator` factored at namespace scope for Phase 2 reuse,
  `Params::defaults()` (`BUILDING / 1995-03-15 / threshold=0`), `load.tpp` S3/S1 dispatch, full `main()`
  in both executables. Phase 1 harness `test_query_q3i_phase1_lsm` exits `[OK]` (row_count ≈ 129 at SF=1).
  Stale test artifacts moved to TRASH (commit `cbd7a870`).
- **MI candidate analysis reframed** — `INVOICE_EXTENSION_CANDIDATES.md` updated to add stock-TPC-H
  comparison track per Goetz feedback (commit `2ad654ee`).

## Conceptual Decisions

1. **`joined_t` is required** — merge-join infrastructure takes `JR` as a template param; monolithic execution
   fuses only post-join operators into callbacks.
2. **Structure 2 = intermediate pipeline view** — materialized output of the MI-source pipeline
   (ORDERS ⋈ LINEITEM for OL queries; not the final query result).
3. **Accumulator structs at namespace scope** — Q3I Phase 1 factored `CustomerOpenDueAccumulator` and
   `LineitemRevenueAccumulator` outside the Visitor so Phase 2's S1/S4 paths can reuse them directly
   (comparison-integrity guarantee: identical accumulation logic across all structures).

## Next Steps

### [Short-term] Q3I Phase 2 — baseline S1, S2, S4 paths (`frontend/tpch/q3i/`)

- `query.tpp` — `query_by_base` (S1): pre-build `cust_open_due` hashmap from INVOICE; `BinaryMergeJoin(OL)` +
  CUSTOMER hash lookup; threshold filter.
- `query.tpp` — `query_by_view` (S2): same hashmap pre-build; view scan + CUSTOMER hash filter.
- `query.tpp` — `query_by_hash` (S4): hashmap pre-build; `HashJoin(OL)` + CUSTOMER hash lookup.
- `views.hpp` — widen `q3i_pipeline_view_t` beyond `joined_ol_t` alias to include `cust_open_due` field.
- `load.tpp` — S2 `populate_q3i_view` body (two-pointer merge + invoice hashmap); S4 base-only path.
- Exit criterion: XOR parity across all four paths at SF=1.

### [Short-term] Q3I Phase 3 — top-10 + CMake targets + experiment harness (`frontend/tpch/q3i/`)

- `query.tpp` — top-10 `ORDER BY revenue DESC` via priority queue across all four paths.
- `frontend/CMakeLists.txt` — add `q3i_lsm` (mac+Linux) and `q3i_btree` (Linux) targets.
- `tests/` — `test_load_q3i_lsm` and `test_query_q3i_lsm` (parity + shape check).
- `frontend/tpch/CLAUDE.md §Tests` — update index with Q3I rows.
- `generate_targets.py` — entries for `q3i_lsm` / `q3i_btree`.

### [Short-term] Q12 build-system integration (`frontend/`)

- `frontend/CMakeLists.txt` — add `q12_lsm` / `q12_btree` targets (load-test targets already present).
- `generate_targets.py` — `exec_names`, `STRUCTURE_OPTIONS`, `DIFF_DIRS` entries; regenerate `targets.mk`.

### [Medium-term] Q5I and Q10I body implementation

- Q5I (Local Supplier Volume + invoice payment-status split): skeleton pending; follows Q3I 3-phase pattern.
- Q10I (Returned Item Reporting + customer payment-behaviour overlay): same.
- Both share `COLIPipeline<Backend>`; per-query `load.tpp` delegates S3 to `coli.populate_merged()`.

### [Medium-term] Q3/Q9 baseline body implementation

- Fix `q3/load.tpp` and `q9/load.tpp` references to removed `ol.populate_view` / `ol.get_view_size`.
- Q3: per-query view type, `BinaryMergeJoin` + CUSTOMER hash join, top-10 sort.
- Q9: NATION/SUPPLIER hashmap construction; PART/PARTSUPP merge joins inside callback; LIKE filter on `p_name`.

### [Medium-term] Ad-hoc Q12 experiments

- Scan-selectivity sweep (1-month / 1-year / 3-year date windows) — Reviewer 3 W2/D5.
- Single-table LINEITEM-only scan: standalone index vs. MI with ORDERS interleaved — Reviewer 3 W3/D6.
- Memory-pressure sweep (low DRAM / high SF) to confirm MI advantage widens under I/O pressure.

## Open / Side-Track Items

- **LeanStore background-insert bug**: records shifting pages causes issues under concurrent inserts;
  RocksDB unaffected. Deferred — all current experiments use RocksDB backend.
- **`joined_t` flattening**: store flattened projection instead of tuple of input records. Deferred.
- **Stock-TPC-H Q3/Q9 baseline comparison arm**: reframed in `INVOICE_EXTENSION_CANDIDATES.md` per Goetz
  feedback; experiment design not yet written up in `TPCH_experiments.md`.

## Long-Term (Next-Paper Scope)

Multi-MI pipeline cascades (each query with multiple merged indexes — Q12 with 2 MIs, Q9 with 5) are
documented in `calcite-integration-info/test-plans/` and `frontend/tpch/q12/CLAUDE.md`. Out of scope here.
General plan interpreter (JSON → C++ operator tree) replacing manually-coded templates. Out of scope here.
