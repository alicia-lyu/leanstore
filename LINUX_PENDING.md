# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps).  See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline.

## Q3 throughput-shape comparison (post-bring-up follow-up)

The 2026-05-08 Linux bring-up confirmed that **q3 produces identical
top-10 results across S1/S2/S3/S4** at SF=15 on both backends — i.e.
the cross-structure correctness story holds at production scale on
both LSM and B-tree. What it did **not** capture is the per-structure
**throughput** comparison the original entry asked for (the S3 ≥ S2 >
S1/S4 shape relative to Q3I PERFORMANCE.md §A1).

Reason: `frontend/tpch/q3/executable_{rocksdb,leanstore}.cpp` and the
shared `tpch::dispatch_storage_structure<…>` helper run the query
once and print the result rows, with no `--tx_seconds` throughput
loop and no per-structure CSV. Q3I has its own
`TpchExecutableHelper::tput_tx` driver wired up in
`q3i/executable_*.cpp`; Q3 doesn't yet.

- **What's needed**: lift Q3I's `tput_tx` wiring into Q3's two
  executables (or hoist into a shared helper if both Q3 family
  members can share it). Then `make q3_{lsm,btree} scale=15
  dram=0.1` will produce comparable per-structure numbers.
- **Reference shape** (Q3I, both backends, post-A2c+A3): S3 ≥ S2 >
  S1/S4 — see `frontend/tpch/q3i/PERFORMANCE.md §A1` (+356% at
  SF=15, +116× at SF=40 disk-bound on LeanStore).
- **Expectation for Q3**: S3 should still match/beat S2 and exceed
  S1/S4, but with smaller absolute gaps than Q3I because the
  §3.1.2 sibling-aggregate amortisation is invoice-only. Comparing
  the two ranks isolates §3.1.3 hierarchical-prefix from §3.1.2
  sibling-aggregate.
- **Pre-flight**: any merge that bumps the mtime of
  `frontend/tpch/tpch_workload.hpp` or `frontend/tpch/q3/load.tpp`
  invalidates the persisted `$(data_disk)/q3_*/build/$(scale).json`
  image. After such a merge, run `make q3_lsm scale=15 dram=0.1`
  once to refresh before taking clean numbers.

## `q3i_btree` COLI tagged-key variant-dispatch failure (2026-05-08)

`test_query_q3i_btree` aborts with an assertion in the merged
scanner's variant dispatch:

```
views_coli.hpp:110  Assertion `tag == static_cast<u8>(Tag)' failed.
  in tag_field_step<coli_domain_tag::orders, &orders_coli_t::Key::orderkey>::unfoldStep
  called from orders_coli_t::Key::keyunfold
  called from toType<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>
  called from LeanStoreMergedScanner<…>::current
```

at `frontend/tpch/coli_pipeline.tpp:323` inside
`tpch::coli_group_walk` (called from
`tests/q3i/test_query_q3i_leanstore.cpp:258`).

Symptom: scanner is iterating over the COLI MI and dispatching to
the `orders_coli_t` variant on a key whose leading domain-tag byte
isn't 0x03 (the orders tag) — i.e. the SFINAE `accepts_key` chain
in `LeanStoreMergedAdapter::toType` picked the wrong variant. The
key bytes seen in gdb: `\001\200…` — first byte 0x01
(`coli_domain_tag::customer`).

Notes:

- The `_lsm` equivalent (`test_query_q3i_lsm`) passes with full
  S1–S5 parity, so the bug is specific to either how BTreeLL
  stores the tagged keys, how `LeanStoreMergedScanner::current`
  reconstructs the key slice, or how `accepts_key` is invoked
  on the LeanStore path. RocksDB's path is unaffected.
- This is the only `_btree` smoke test still failing after the
  2026-05-08 adapter fixes (see §"_btree adapter cleanup" below).
  Q12 and Q3 `_btree` smoke tests + perf sweep at SF=15 pass with
  full S1–S4 parity.
- Likely starting point for triage:
  `frontend/shared/adapter-scanner/LeanStoreMergedScanner.hpp:124`
  (the `current()` method that calls `toType`) and
  `frontend/shared/adapter-scanner/variant_utils.hpp:44-47` (the
  variant-dispatch fold). Compare to RocksDB's working path in
  `RocksDBMergedScanner.hpp` to spot the divergence.
- Until this lands: Q3I's S3/S5 paper-axis numbers on LeanStore
  remain blocked. Q3I LSM is the source of truth.

## Closed by 2026-05-08 bring-up

These items were drained on the bring-up; recorded here for
traceability since they used to live above this header.

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
  confirmed at production scale**, both backends. Throughput-shape
  follow-up tracked above (§"Q3 throughput-shape comparison").

- **`geo_*` `invoice_t` regression** (FIXED earlier 2026-05-08):
  `frontend/geo/executable_{leanstore,rocksdb}.cpp` declared a
  stray `Adapter<invoice_t>` and passed it to `TPCHWorkload`'s
  ctor, leftover from the vanilla/invoice-extended workload split
  (commit 238e6829). Fix: dropped the adapter and the trailing
  ctor argument from both files.
