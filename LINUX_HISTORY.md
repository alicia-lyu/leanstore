# Linux / LeanStore Resolved Items

Append-only archive of Linux / LeanStore bring-up items that have been
drained. Items live here once they are no longer actionable; the active
worklist lives in [`LINUX_PENDING.md`](LINUX_PENDING.md). Do not edit
prior entries — append new sections in chronological order.

## Closed by 2026-05-08 bring-up

These items were drained on the 2026-05-08 Linux bring-up; recorded
here for traceability.

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
  **Follow-up 2026-05-11**: the "5/5 consecutive runs" above was a
  lucky sample, not a true close. A fresh CloudLab bring-up
  reproduced an S1-only digest mismatch (rows=10 across paths but
  S1 `aggregator_rows_out`=12 vs S3/S4=13) at ~10% repro rate. Root
  cause was a *separate* defect in `BinaryMergeJoin` (see the
  2026-05-11 entry below), not a regression of the variant-dispatch
  fix. The `HasKeyAcceptsKey` + `scheduleJobSync` fixes above were
  real prerequisites — without them the test asserts or SEGVs
  before it can run — and the BMJ-flush bug was probabilistic-silent
  on top of those. Both fixes are needed; neither is wasted.

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

## Closed by 2026-05-11

- **BMJ final-group flush — silently-dropped last-group join**
  (was: `test_query_q3i_btree` at SF=1 `--wal=true` produced
  rows=10 across all paths but S1's digest diverged from
  S2/S3/S4/S5, with `aggregator_rows_out` S1=12 vs S3/S4=13.
  `[FAIL] cross-structure aggregator_rows_out` plus `[FAIL] S1
  base topK row 2/3 differs`; ~10% repro rate driven by
  `std::random_device`-seeded TPC-H data). Root cause:
  `BinaryMergeJoin::refresh_join_state()` in
  `frontend/shared/merge-join/binary_merge_join.hpp` early-returned
  when both fetch sides exhausted in the same `next_jk()` pass
  after their pre-exhaust JKs were equal, skipping the final
  `join_state.refresh(...)` that drains the last group's matched
  records. Records emplaced for the final shared key sat in
  `records_to_join` without producing their cartesian product —
  silent drop of last-group join output. Fires only when the
  rightmost JK is simultaneously the last surviving group on both
  BMJ sides. Fix: flush the final group from `next()` in
  `frontend/shared/merge-join/binary_merge_join.hpp` (commit
  `92336200`). Verification: 50/50 consecutive `test_query_q3i_btree`
  SF=1 runs pass `[OK]` parity across S1–S5; `test_query_q3_btree`
  and `test_query_q12_btree` (other BMJ consumers) still pass — no
  regression.

- **Q3 S1/S2/S4 vs S3 access-pattern fairness** (was: only S3 had
  physical custkey seek-skip via `WalkAction::SkipGroup`; S1/S2
  streamed every row and S4 used a logical hash filter, so the
  perf sweep wasn't apples-to-apples on physical-seek mechanics).
  Three sibling commits today close the asymmetry:
  - `bbc15e68`: S2 view physical seek-skip on custkey-miss,
    mirroring S3's `WalkAction::SkipGroup` at
    `tpch_family/col_pipeline.tpp:294-296`. New counter
    `view_groups_skipped`.
  - `f62a0149`: S1 BMJ-chain pre-scans the 150-customer table;
    `fetch_ord` physical-seeks `ord_scan` past gaps between
    accepted custkeys (orders-side closed; lineitem-aggregator-side
    deferred — BMJ refill race blocker documented inline). New
    counters `s1_groups_skipped`, `s1_orders_skipped` (reserved).
  - `f62a0149` (S4 portion of same commit): inverts the lineitem
    pass to physical-seek per qualifying
    orderkey instead of streaming all lineitems and filtering by
    `orders_map.find()`. New counter `s4_orderkey_seeks`.
  Result: S1/S2/S3/S4 now apples-to-apples on physical-seek
  mechanics. SF=1 parity preserved on both backends. Linux
  prerequisite for the upcoming Q3 perf sweep.

## Closed by 2026-05-23 analyzer fix

- **paper-data diagnostics columns empty across all sweep rows**:
  rotated from [`LINUX_PENDING.md`](LINUX_PENDING.md). Root cause
  was twofold — (1) the four LeanStore btree binaries emit
  `query/<method>/{bm,cpu,cr,dt}.csv`, but the leading `c_hash`
  column is unquoted comma-separated subfields that shifted every
  real column right by 6 positions; (2) `analyze_paper_sweep.py`'s
  hardcoded header keys (`"workers Cycles / TX"`, `"bm_free_pct"`,
  `"cr_restarts"`, `"TXT P50"`) did not match the actual schemas
  (`cycle`, `free_pct`, `cc_snapshot_restart`, no `latency.csv` at
  all). Analyzer rewritten with a `read_detail_csv()` that detects
  the c_hash shift per-row, `aggregate_workers()` that means cpu
  rows whose `key` starts with `worker_`, and corrected column-name
  mappings. Coverage after fix on `-b`: btree binaries 144/144 across
  cpu/bm/cr/dt diagnostics; LSM binaries 144/144 for surrogate
  `cpu_cycles_per_tx`/`cpu_util_pct`/`sst_*` from TPut, with LeanStore
  counters legitimately N/A. Followups for the gaps that surfaced
  (LSM detail emission, latency capture, c_hash quoting) appended
  to LINUX_PENDING.md. Plot verified: `diag_explore_all.pdf` shows
  real S1-S4 spread in LLC misses + cycles; `diag_explore_q3i_lsm.pdf`
  confirms the S2/S3 cycles inversion at q3i_lsm large-DRAM that
  SWEEP_LOG.md flags. Analyzer-only fix; no re-sweep.

## Closed by 2026-05-23 post-merge triage

Two of the three follow-up gaps appended by the analyzer fix above were
triaged and closed; the third (`latency.csv`) was deferred to the next
project and stays in [`LINUX_PENDING.md`](LINUX_PENDING.md).

- **LSM binaries don't emit per-tx detail CSVs / rocksdb counters**
  (was: q3/q5/q3i/q5i_lsm + geo_lsm write only `TPut.s<N>.csv` +
  `size.s<N>.csv` + an empty `structure<N>_stderr.txt`; no
  `query/<method>/{bm,cpu,cr,dt}.csv`, no rocksdb block-cache lines).
  **Resolution: expected / by design.** `RocksDBLogger::log_details()`
  is an intentional no-op (`frontend/shared/logger/rocksdb_logger.hpp:81`)
  — the per-method detail tables are LeanStore worker-counter tables
  (`LeanStoreLogger::log_details`, `leanstore_logger.hpp:43`) with no
  RocksDB analogue on the normal emit path. The analyzer's TPut-derived
  surrogates (`cpu_cycles_per_tx`, `cpu_util_pct`, `sst_*`) are the
  accepted resolution for LSM rows; LeanStore-only counters (`bm_*`,
  `cr_*`, `dt_*`, `cpu_llc_miss_per_tx`) are legitimately N/A for LSM.
  No code change.

- **LeanStore `c_hash` emitted with thousands-separator commas**
  (was: the leading `c_hash` column in
  `<run>/<tx>/<method>/{bm,cpu,cr,dt}.csv` printed like
  `7,978,927,218,222,587,432`, shifting every real column right and
  breaking strict CSV parsers; the analyzer worked around it with a
  per-row shift detector). **Fixed at emit time:**
  `frontend/shared/logger/logger.cpp:155` changed `csv << config_hash`
  → `csv << std::to_string(config_hash)`. `config_hash` is a `u64`
  (`ConfigsTable::hash()`); the stream's active grouping locale was
  inserting separators, while `std::to_string` is locale-agnostic and
  matches the integer path in `ProfilingTable::Column::to_string`.
  Output is now a clean unquoted integer — no downstream re-quoting
  needed, and the analyzer's shift workaround becomes a harmless no-op.

## Closed by 2026-05-17 bring-up

- **Q5I btree first Linux perf sweep**: harness-bug parity gate
  resolved 2026-05-17 (commit `82927bf8`,
  [`LINUX_PENDING.md`](LINUX_PENDING.md) entry rotated); SF=620
  DRAM=0.4 GiB beyond-memory sweep landed at commit `82927bf8`
  with shape **S2 (0.198) > S3 (0.164) > S1 (0.042) > S4 (0.018)
  TX/s** and secondaries ~2.5 GiB (ratio ≈ 6.4× over DRAM). Same
  S2 > S3 inversion as Q5/Q3I — paper-acknowledged COL-walker
  infrastructure gap. Full record at
  [`frontend/tpch/q5i/RUNS.md`](frontend/tpch/q5i/RUNS.md)
  (2026-05-17 10:44 MDT entry). The prior 00:43 MDT entry was
  reclassified there from BLOCKED to "cache-resident data point"
  — the original BLOCKED label was a false alarm: the
  `test_query_q5i_btree` harness skipped the populate hooks but
  `Q5IWorkload::load()` at `q5i/load.tpp:77-81` always populated
  them, so the end-to-end binary's numbers were valid. The
  harness fix commit (`82927bf8`) also ported the cardinality +
  sentinel-ordering diagnostic block from the RocksDB harness so
  any future drift surfaces as a `[FAIL]` row rather than a
  vacuous-parity surprise.

- **Q5I LSM first Linux perf sweep**: fresh CloudLab Ubuntu 22.04
  node brought up per `LINUX_SETUP.md`; q5i_lsm + q5i_btree built;
  parity gate run for both (`test_query_q5i_lsm` SF=5 strict
  4-way at digest `0x2f31fe8244b728c1`, rows=3); SF=15 LSM sweep
  landed in
  [`frontend/tpch/q5i/RUNS.md`](frontend/tpch/q5i/RUNS.md).
  Measured shape **S2 (161.66) > S3 (76.29) > S1 (24.39) ≈ S4
  (15.30) TX/s** — the predicted Q5 inversion (S2 > S3) recurred,
  same COL-family walker infrastructure gap; not Q5I-specific.
  (The q5i_btree sweep originally appeared to fail parity, was
  tracked as a separate Active item, and was closed later the
  same day after the test-harness bug was found — see the btree
  entry above this one.)

- **Q5I test_query_q5i_{lsm,btree} first build on Linux**:
  CMake targets existed (`frontend/CMakeLists.txt:285-296` LSM,
  `:489-493` btree) but had never been built on this machine
  before. Both built clean on the first try (no Linux-only
  preprocessor regressions in the q5i path); LSM passed parity at
  SF=1/5, btree passed at SF=1 (vacuous) and failed at SF=5
  (rotated to the new Q5I-btree-parity LINUX_PENDING item).

## Closed by 2026-05-14 bring-up

- **Q5 first Linux perf sweep**: fresh CloudLab Ubuntu 22.04 node
  brought up per `LINUX_SETUP.md`; Q5 binaries built on top of the
  Step 4 set; SF=1 parity verified on both backends; SF=15 sweep
  landed in [`frontend/tpch/q5/RUNS.md`](frontend/tpch/q5/RUNS.md)
  and the headline propagated to
  [`frontend/tpch/STATUS.md`](frontend/tpch/STATUS.md). Measured
  shape both backends: **S2 > S3 > S1 > S4** — view materialisation
  beats COL group walk at SF=15, same S2/S3 inversion Q3I exhibits
  (cross-ref `q3i/PERFORMANCE.md`). Two macOS-side regressions
  surfaced and were fixed inline before the parity gate:
  - `test_query_q5_leanstore.cpp` was passing 4 args to the 5-arg
    `populate_q5_view` (the loader signature widened during Phase
    10B to take the `nation` adapter; the `_btree` test wasn't
    rebuilt on macOS).
  - `test_side_tables.cpp` still referenced `Q5SideTables::n_name_map`
    and the map-typed `supplier_nation` removed in Phase 10B.
  Plus one substantive Q5 correctness bug that macOS hid: the
  `q5_pipeline_view_t.n_name` field was declared `std::string`, but
  the view is persisted via the memcpy-based record_traits.
  libstdc++ `std::string` is not standard-layout — SSO bits don't
  round-trip across `insert`/`getScanner`, so S2 diverged from
  S1/S3/S4 on Linux. macOS libc++ happened to round-trip them
  byte-stably and the SF=1 macOS data hit one bucket that masked
  the fallout. Fix landed in `a1fbdc15`: switched the field to
  `Varchar<25>` and converted at the two call sites
  (`q5/load.tpp` emit, `q5/query.tpp` S2 scan). SF=1 / SF=5
  parity verified post-fix on both backends.

## Closed by 2026-05-24 — DBToaster baseline (Phase 0)

- **DBToaster RF1/RF2 baseline (`dbtoaster/`): Phase 0 install + build +
  sweep** (was Active in [`LINUX_PENDING.md`](LINUX_PENDING.md), 2026-05-24).
  All Phase-0 sub-items landed:
  - **Install** (`LINUX_SETUP.md §6`): DBToaster 2.3 at `/opt/dbtoaster`,
    `openjdk-11`, tpch-dbgen — done on `c220g2-011011`.
  - **Codegen + harness**: `make refresh_sales.hpp` compiles
    `refresh_sales.sql` (the Q3 and Q5 pipeline-view `SELECT`s) to C++ via
    the DBToaster compiler; `main.cpp` is complete — it streams base+RF1
    inserts through `process_stream_event`, fires typed `on_delete_ORDERS` /
    `on_delete_LINEITEM` triggers for RF2, and has an `RF_INTERLEAVE` mode that
    replays RF1/RF2 as size-stable pairs. Built to `dbtoaster/build/refresh_sales`.
  - **Measured** (results in `dbtoaster/results/`): interleaved size-stable
    pairs **33.2k pairs/s** (66.4k order-events/s; commit `df63c5cb`) at the
    ~5 GiB working-set anchor (`SF≈0.36`, pinned by extrapolating VmRSS from
    SF 0.01/0.1). VmRSS ≈ 5.0 GiB after warmup, ≈ 8.25 GiB peak (~1.7×).
  - **Correctness**: the two maintained views (`QUERY_1`/`QUERY_2`) hold equal,
    size-stable row counts (2160142 each) across RF1/RF2 pairs — used as the
    correctness signal (row-count consistency + size-stable delta; no separate
    DuckDB join cross-check was run).
  - **Methodology note**: the originally-planned `ulimit` ladder (unlimited /
    1.0 GiB / 0.4 GiB) was **dropped for DBToaster** — it is pure in-memory, so
    a cap below the working set OOMs rather than degrades. Replaced by an
    unlimited-memory run reporting working-set/peak RSS as the RAM lower bound.
    Full rationale in `dbtoaster/results/README.md`.
  - **Comparison target**: LeanStore S2 ≈ 22.6k (btree) / 4.2k (lsm) RF1
    inserts/s at SF=1; in-memory prewarm9 (btree) pairs S1 41k … S2 10.4k. The
    LeanStore-side **LSM** in-memory counterpart is closed by the 2026-05-25
    entry below.

## Closed by 2026-05-25 — Q10 first Linux 5L sweep + refresh LSM 9 GiB

- **Q10 5L perf sweep (first Linux)** — rotated from
  [`LINUX_PENDING.md`](LINUX_PENDING.md). Built `q10_{lsm,btree}` +
  `test_query_q10_{lsm,btree}` clean on Linux. **Parity gate** (SF=1, strict
  4-way XOR, 20 rows, both default + off-default param iters): `test_query_q10_lsm`
  green (iter0 `0xe8eb55a779a831df`, iter1 `0xc6a760d87c0005bc`);
  `test_query_q10_btree` green (iter0 `0xa0c771f080c2ab35`, iter1
  `0xe78cdec20e02a9d2`) — needs `--wal=true --trunc=true` (the test runs
  `--vi=true`; production `make q10_btree` runs `--vi=false` so it needs
  neither). **5L sweep** (c0, DRAM=1.0, SF=3850 lsm / 1550 btree, isolated,
  fresh `q10.load()` at each scale; commit `8aac715a`): ms/query — LSM **S3
  11,326** < S1 16,059 < S2 17,169 ≪ S4 77,989; btree **S1 23,535** < S3 35,262
  ≪ S4 127,486 < S2 174,260. **Supports** the §3.1.3 COL-MI claim: S3 ≥ S2 and
  S3 ≫ S4 on both backends (btree S3 beats the 8.6 GiB view ~5×); S3 wins
  outright on LSM, while on btree the dense custkey-sorted split S1 edges S3. No
  q3i-style S2>S3 anomaly. Full numbers in
  [`frontend/tpch/q10/RUNS.md`](frontend/tpch/q10/RUNS.md) (2026-05-25 entry).
  (Benign `turnPage→gotoPage` scan fallbacks fired ~31× during btree S3 under
  page pressure; query completed exit 0, correctness pinned by the SF=1 gate.)

- **refresh_sales LSM 9 GiB in-memory run** — rotated from
  [`LINUX_PENDING.md`](LINUX_PENDING.md). The LSM counterpart to the btree
  prewarm9 ran via `build/scratch/run_refresh_lsm9_ssd.sh` (SF=3850, DRAM=9,
  `--prewarm=false`), closing the DBToaster comparison on the LSM side.
  **Finding: LSM has no in-memory speedup for refresh** — pairs/s S1 781 / S2
  473 / S3 606 / S4 672, i.e. ~0.5× the 5L (DRAM=1.0) rates, because LSM refresh
  is write/compaction-bound (more block cache hurts), and `--prewarm` actively
  backfires on RocksDB (S1 59.6 pairs/s with prewarm vs 780 without — fills the
  strict-capacity block cache and starves the write path). Headline LSM number
  stays the 5L run. Kept as a **separate** `summary/lsm_9gib_refresh.csv` (not
  merged into `pair_vs_dbtoaster.csv`) precisely because there is no in-memory
  advantage to compare apples-to-apples. Committed `8daaf0f2`; README addendum +
  `refresh_sales/RUNS.md` entry landed.

## Closed 2026-05-25 — Q10I merge + aCOLI (S5) + fair S2 view

- **Q10I merged to calcite-integration + S5 aCOLI added** (was
  worktree-only on `worktree-agent-a86745538133069de`). The merge widened
  the shared `lineitem_coli_t` (`l_returnflag`, D14), which invalidated the
  Q3I/Q5I COLI 5L images — reloaded on merge. Fixed a stale Linux-only
  `test_query_q10i_btree` harness (never populated the S2 view; the author's
  Phase-4 parity was macOS/LSM-only) and a `generate_targets` mis-mapping
  (q10i was wired to recover from the q3i/q5i family image, which lacks
  q10i's view → degenerate S2; q10i now loads standalone).
  Smoke test reproduced Q10's two btree anomalies (per-lineitem S2 strawman;
  S3<S1, prune below the COLI co-location grain), so the Q10 fixes were
  ported: a per-order **pre-aggregated** S2 view (`q10i_pipeline_view_preagg_t`,
  `--q10i_view_variant=preagg`) and an **S5 aCOLI MI**
  (`<customer_coli_t, orders_acoli_q10i_t>`, per-order paid/open/late baked,
  lineitems+invoices dropped, hand-rolled `acoli_group_walk`). D3 overturned
  (per-order grain is soundly bakeable). Parity green at SF=1 both backends
  (S1≡S2≡S3≡S4≡S2-preagg≡S5). **The aCOLI is the fastest structure on both
  backends** at 5L: btree S5 181 ms ≪ S2-B 21,349 / S3 47,296; lsm S5 1,561 ms
  < S2-B 2,014 / S3 16,570. Validates ACOL_ACOLI_PLAYBOOK §6. The
  S3-physical-skip characterization for the 4-table `coli_group_walk` was NOT
  done — superseded by S5 (the aCOLI is the fast path; revisit via the
  playbook only if a raw-S3 q10i number is needed). See
  `frontend/tpch/q10i/{RUNS.md,CLAUDE.md}`.

## Closed 2026-05-25 — refresh_sales 5H memory-pressure A/B

- **refresh_sales at 5H (c3, DRAM 0.4) — LSM strength under memory pressure**
  (was the only Active item in `LINUX_PENDING.md`). Ran
  `build/scratch/run_refresh_5H_ssd.sh` + `summarize_refresh_5H_ssd.sh`, and
  re-ran the 5L cell at the same commit (`83240c0a`, re-stamped from
  `c9b5f594`) so the A/B is clean: same SF (1550 btree / 3850 lsm), same
  per-structure-copy + drop-caches cold-start; only DRAM changes 1.0→0.4 GiB.
  **Hypothesis confirmed.** LSM throughput is flat under pressure (±2% across
  S1–S4: write-optimized SST flush/compaction doesn't depend on buffer-pool
  residency), while btree degrades 21–59% from random page eviction once the
  working set spills the pool (S4 the worst, 5531→2269 pair_tps, 0.41×). At
  5H, LSM overtakes btree on S1/S2/S3 and btree's S4 lead collapses 4.2×→1.8×.
  Pairs with `2026-05-24-refresh-prewarm9` (the abundant-memory regime, where
  LSM showed no in-memory speedup) — the pressured regime is where LSM wins.
  Tags `paper-data/2026-05-24-refresh-{5L,5H}-ssd`; SWEEP_LOG entry landed.

## Closed 2026-05-28 (LSM recover + sweep verification)

- **LSM recover-from-image now correctly configured.** Diagnostic
  worktree traced the prior abuse to the RocksDB destructor running
  `Flush + WaitForCompact + close_db` unconditionally (minutes per
  CF at SF=3850) plus `get_size()` forcing a bottommost compaction
  every recover-only run. Fix landed as ef1d422a (skip both on
  `!FLAGS_persist` / `FLAGS_recover`), merged via acf78a35.
  Verified by the 2026-05-28 LSM rep=0 sweep: 27 cells of the 5L
  cell completed in 21 min 53 s wall (vs. multi-hour reload prior).
  Each cell shows `RocksDB::~RocksDB() Recover-only mode — skipping
  flush + compact.` and `Recovering last ids...` instead of the
  `Loading X records` reload path. No aborts, no `std::out_of_range`
  (the logger fix 35a8650e covered the bg=2 cohort failure mode).
  Results land in `build/<binary>/3850-in-1.0/structure<N>.log`;
  size_audit WARN lines surface the known LSM default-CF
  under-counts (per-adapter sum vs image file size).
