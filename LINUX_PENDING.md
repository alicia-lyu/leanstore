# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps).  See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline.


## Closed by 2026-05-08 bring-up

These items were drained on the bring-up; recorded here for
traceability since they used to live above this header.

- **`q3i_btree` COLI tagged-key variant-dispatch failure** (was: an
  assertion `tag == static_cast<u8>(Tag)` in
  `views_coli.hpp:110` aborted `test_query_q3i_btree` during the
  COLI MI scan; the `_lsm` equivalent passed). Two compounding
  causes:
  1. `frontend/shared/variant_utils.hpp` only checked
     `R::accepts_key`, but the COLI types define the hook on
     `R::Key` (the byte layout owner). The concept was silently
     false for all four COLI records, so `record_matches` fell
     through to the legacy `(maxFoldLength, sizeof(R))` heuristic.
     `orders_coli_t` and `invoice_coli_t` share both values
     (12-byte tagged key, 16-byte payload), so the first record in
     the variadic pack always won — `orders_coli_t::unfoldKey`
     ran on invoice keys and tripped the inner-tag assert. Fix:
     added a `HasKeyAcceptsKey<R>` concept that probes
     `R::Key::accepts_key`; `record_matches` now tries the outer
     hook, then the Key-level hook, before falling back to the
     legacy heuristic.
  2. `tests/q3i/test_query_q3i_leanstore.cpp` called
     `pipeline_view.size()` / split / merged / acoli `.size()` on
     the main thread (lines 302 and 368). Like
     `test_load_col_leanstore.cpp` before 708daa84, those size
     paths walk inner B-tree pages via `HybridPageGuard` and need
     a Worker TLS that the main thread does not have, so they
     SEGV'd. Fix: hoisted the six `.size()` calls into a single
     `crm.scheduleJobSync(0, ...)` block and reused the captured
     values at line 368.
  Result: `test_query_q3i_btree` runs to completion with full
  `[OK]` parity across S1/S2/S3/S4/S5 at SF=1 (5/5 consecutive
  runs). RocksDB `test_query_q3i_lsm` digest unchanged (the
  variant-dispatch fix is additive — RocksDB happened to escape
  the heuristic-collision because per-CF inserts feed the scanner
  records of one type at a time in test paths that exercise it,
  but the dispatch is now deterministic on both backends). The
  production `q3i_btree` binary still completes a 3 s S3 smoke at
  SF=1 cleanly — the BTreeLL adapter fix in 708daa84 had been
  papering over the dispatch bug for the long-running TX loop;
  this fix closes the underlying defect.

- **`_btree` smoke-test SEGV** (was: SIGSEGV right after
  `Loaded 1500 orders records.` and during q3i custkey-map build).
  Root cause: `LeanStoreAdapter::getScanner()` and
  `LeanStoreMergedAdapter::{ctor, getScanner, getSelectiveScanner}`
  branched on `FLAGS_vi`, casting to `BTreeVI*`. `LeanStoreAdapter`
  has always registered `BTreeLL` (per its ctor comment), so with
  `--vi=true` (the default since SI/MVCC) the `dynamic_cast` to
  `BTreeVI*` returned null; `*static_cast<BTreeGeneric*>(nullptr)`
  produced a null reference and the first iterator op deref'd
  `btree.dt_id` at offset 0x10 → SIGSEGV. Fix: removed the
  `FLAGS_vi` branches from both adapters; both unconditionally
  use `BTreeLL`. Also fixed `test_load_col_leanstore.cpp` to
  wrap two `merged_col.size()` / `col_pipe.get_split_size()`
  calls in `crm.scheduleJobSync` — the LeanStore size paths walk
  inner B-tree pages and need a Worker TLS, which the main
  thread doesn't have.
  Result: `test_load_merged_btree`, `test_load_q12_btree`,
  `test_load_col_btree`, `test_query_q12_btree`,
  `test_query_q3_btree` all pass at SF=1 with full `[OK]`
  output and `q3_btree scale=15 dram=0.1` completes the S1–S4
  sweep with byte-identical 10-row results across structures.

- **Linux-only test targets** (was: "run when on Linux"):
  `test_query_q3_btree` strict cross-structure XOR parity at SF=1
  passes with digest `0xede9eaca9c7a6000` across S1/S2/S3/S4;
  `test_load_col_btree` SF=1 passes with full `[OK]`
  distribution + secondary checks.

- **Q3 perf sweep**: `q3_lsm` and `q3_btree` both ran cleanly at
  SF=15 across structures 1–4. **Cross-structure correctness
  confirmed at production scale**, both backends.

- **Q3 throughput-shape wiring** (was: `q3_{lsm,btree}` ran the query
  once and exited, with no `--tx_seconds` loop and no `TPut.csv`).
  Fix: switched Q3's `executable_{rocksdb,leanstore}.cpp` from the
  one-shot `tpch::dispatch_storage_structure` helper to the same
  `TpchExecutableHelper::run` → `tput_tx` driver Q12 / Q3I use;
  both Q3 binaries now produce a 4-row `build/q3_{lsm,btree}/TPut.csv`
  per sweep. Smoke at SF=15 dram=0.1 LSM: S3 mi_col_walk 210.52,
  S2 pipeline_view 97.54, S1 base_merge_join 66.97, S4 base_hash_join
  65.02 TX/s — pure-§3.1.3 hierarchical-prefix shape S3 ≥ S2 >
  S1 ≈ S4 (steeper S3 gap than Q3I as predicted; no §3.1.2
  sibling-aggregate confound).

- **`geo_*` `invoice_t` regression** (FIXED earlier 2026-05-08):
  `frontend/geo/executable_{leanstore,rocksdb}.cpp` declared a
  stray `Adapter<invoice_t>` and passed it to `TPCHWorkload`'s
  ctor, leftover from the vanilla/invoice-extended workload split
  (commit 238e6829). Fix: dropped the adapter and the trailing
  ctor argument from both files.
