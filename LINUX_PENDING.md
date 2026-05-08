# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps).  See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline.

## Q3 perf sweep (post-Phase-4, 2026-05-04)

- **Targets**: `q3_lsm` and `q3_btree` at `--storage_structure 1..4`,
  SF=15 (per Makefile default) — wired in `frontend/CMakeLists.txt`
  and `generate_targets.py` (lines 19, 458–470).
- **Reference shape**: Q3I S3 ≥ S2 > S1/S4 pitch (see
  `frontend/tpch/q3i/PERFORMANCE.md §A1`: +356% at SF=15, +116× at
  SF=40 disk-bound).
- **Expectation**: Q3 is the no-invoice baseline of the same COL
  family; S3 should still match/beat S2 and exceed S1/S4, but with
  smaller absolute gaps because the §3.1.2 sibling-aggregate
  amortisation (Q3I-only) is absent.  Use this comparison to
  isolate the §3.1.3 hierarchical-prefix benefit from the §3.1.2
  sibling-aggregate benefit.
- **Pre-flight**: load-path files
  (`frontend/tpch/tpch_workload.hpp`,
  `frontend/tpch/q3/load.tpp`) gate the persisted JSON image —
  any merge that bumps their mtime forces a full reload on the
  next perf run.  Schedule `make q3_lsm scale=15 dram=0.1`
  (or `--load_only_structure=N` per structure) to refresh
  before clean numbers.
- **Verification harness already green on macOS**:
  `test_query_q3_lsm` strict cross-structure XOR parity at SF=1,
  rows=10, identical digest across S1–S4.

## Linux-only test targets (already wired, run when on Linux)

- `test_query_q3_btree` — Linux-only LeanStore variant of the
  cross-structure parity harness.
- `test_load_col_btree` — Linux-only LeanStore variant of the COL
  load test.

## `_btree` smoke-test SEGV (2026-05-08, branch `calcite-integration`)

Observed on a fresh Ubuntu 22.04 / clang 14 bring-up against
commit 7115cc76. RocksDB-backed (`_lsm`) smoke tests at SF=1 all
pass with full S1–S5 cross-structure parity. The four
LeanStore-backed (`_btree`) smoke tests SEGV (exit 139) during
the load phase:

- `test_load_merged_btree`, `test_load_q12_btree`,
  `test_query_q12_btree` — SIGSEGV right after
  `Loaded 1500 orders records.` (no stderr / no assert).
- `test_query_q3i_btree` — passes the prior
  `loadInvoiceAndLinkLineitem` step (prints
  `Inserted 3000 invoices, rewrote 6112 lineitems.`) but then
  SEGVs while building the customer→orders / customer→lineitems
  maps; preceded by multiple
  `JUMP in backend/leanstore/storage/btree/core/BTreeGenericIterator.hpp:369`.

Notes:

- The old LINUX_SETUP.md known-issue (`ensure(false)` in
  `BTreeVI.cpp` from the invoice loader) is no longer the
  proximate cause for q3i — the loader completes. Either the
  fix landed and the new SEGV is downstream, or the failure
  mode shifted with the recent TPCHWorkload split.
- The non-q3i variants don't even touch invoice and still SEGV,
  so this is broader than the previously documented invoice path.
- LSM correctness is intact, so unblocked work on macOS /
  RocksDB targets is unaffected; this gates only LeanStore-backed
  end-to-end perf runs.

## `geo_*` executables: `invoice_t` regression (FIXED 2026-05-08)

Recorded for awareness in case it recurs. Both geo executables
(`frontend/geo/executable_leanstore.cpp`,
`frontend/geo/executable_rocksdb.cpp`) declared a stray
`Adapter<invoice_t>` and passed it to the `TPCHWorkload`
constructor — a leftover from before the `TPCHWorkload`
(vanilla) / `TPCHIWorkload` (invoice-extended) split (commit
238e6829). Geo doesn't use invoice; the fix was to drop the
adapter declaration, the `Adapter(db, "invoice")` assignment,
and the trailing `invoice` ctor argument from both files. Caught
on Linux because `geo_btree` isn't compiled on macOS;
`geo_lsm` would have failed identically had it been built on
macOS recently.
