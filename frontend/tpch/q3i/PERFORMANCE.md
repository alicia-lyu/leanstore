# Q3I S3 Performance Investigation — Active Worklist

This is the **forward-looking** worklist. Every hypothesis below has
already been examined; only their *status* lives here. For evidence
trails, A/B findings, and reverted optimisations, see
[`archive/PERFORMANCE-2026-05-03.md`](archive/PERFORMANCE-2026-05-03.md).

---

## §1 — Status snapshot

S3 (COLI MI `PremergedJoin`) is **~15% slower than S1/S4** at SF=15
dram=0.1 on both RocksDB and LeanStore — the opposite of the paper's
pitch. Storage-engine independent (H5 refuted). At SF=40 dram=0.1 raw
paths collapse to ~0.4 TX/s under DRAM pressure; **S5 (aCOLI MI) keeps
a ~260× lead over the raw paths and ~38–55% trails S2's view**, which
is the merged-index pitch the paper actually earns. The S3-vs-S1 gap
is the cost of parameter flexibility on a raw merged index — but
*why* the gap exists is the open question, and the next round of A/B
tests is wired and ready (Phase 1 commit `b7ebebc8`).

| Path | RocksDB SF=40 dram=0.1 | LeanStore SF=15 dram=0.1 | LeanStore SF=40 dram=0.1 |
|------|------------------------|--------------------------|--------------------------|
| S1 base_merge | 7.73 | 30.36 | 0.3926 |
| S2 view       | 198.3 | 437.17 | 163.33 |
| S3 mi_coli    | 6.70 | 25.41 | 0.3667 |
| S4 hash       | 8.09 | 28.38 | 0.3397 |
| S5 aCOLI      | 123.17 | 265.63 | 89.67 |

(TX/s; full provenance in archive §1 + §3.)

---

## §2 — Hypothesis ledger

Compact status; full evidence in archive §2.

| ID | Hypothesis | Status | One-line takeaway |
|----|------------|--------|-------------------|
| H1 | MI is too large per row | **REFUTED** | `get_size` reporting artefact; content/row 177 vs 150–190 splits |
| H2 | Iterator overhead on rejected groups | **CONFIRMED, fix reverted on RocksDB; PENDING on LeanStore** | Forward iteration cheaper than physical Seek on RocksDB; B-tree branch unexplored (A3) |
| H3 | Walker visits entire MI per query | **CONFIRMED uniform** (subsumed by H6) | ~425k records/q at SF=40; but S1/S4 also full-scan their inputs — doesn't explain the S3-vs-S1 gap |
| H4 | Per-record dispatch overhead | **OPEN, narrowed; A2c confirmed cross-backend** | A1 macOS + Linux refute tagged-key decode (`tuples_advanced/q` identical between S1 and S3 on LeanStore). Linux SF=15 shows S3 `iter_next_cpu/q` is 1.7× S1's per-call (364 vs 217 ns) — accounts for the entire 25 ms query-time gap. A2c (`fused_emit`) is the live remediation. |
| H5 | Storage-engine specific (RocksDB block layout) | **REFUTED** | Same ~16% gap on LeanStore at SF=15 |
| H6 | Low filter selectivity | **CONFIRMED uniform** | All raw paths full-scan; doesn't explain S3-vs-S1 gap; explains S5 win |
| H7 | SSTWrite during read-only queries | **OPEN, RocksDB-specific** | Read-only workload but histogram inflated; A4 attributes to source |
| H8 | Shared-DB cache pollution | **OPEN** | Symmetric across S3/S1 so doesn't explain gap; inflates absolute times; A5 quantifies |
| —  | aCOLI size anomaly | **CLOSED (RocksDB)** | Was a `RocksDB::get_size` stale-cache bug; fixed in `50fd2052`. LeanStore inflation tracked as A7. |

---

## §3 — Next-step A/B tests (priority order)

Each entry: **WHAT** to vary, **WHERE** in the code, **WHAT TO MEASURE**
(Q3IStats fields with `--micro_perf=true`), **WHAT A WIN LOOKS LIKE**.

### A1 — Capture diagnostic stats on Linux (UNBLOCKER)

Not a code A/B; the data run that picks A2 vs A3 vs A4.

- **WHAT**: run `q3i_lsm` and `q3i_btree` with `--micro_perf=true
  --cfstats=true` at SF=15 and SF=40, dram=0.1, all five paths.
- **WHERE**: production binaries (`make q3i_lsm scale=15`, etc.).
- **MEASURE**: `user_key_comparison_count/q`, `block_cache_hit_count`,
  `block_read_count`, `block_read_byte/q`, `iter_next_cpu_nanos`,
  per-CF block-cache stats.
- **DECISION TREE**:
  - `user_key_comparison_count/q` on S3 ≥ 2× S1's → **A2a** (decode).
  - `iter_next_cpu_nanos/record` on S3 ≥ 2× S1's per-iterator total →
    **A2c** (scanner emit).
  - S3 cache hit rate < S1's → H8 differential, jump to **A5**.
  - None stand out → **A2b** (variant dispatch) by elimination.

#### macOS A1 partial result (2026-05-03, SF=15 dram=0.1, RocksDB, cache-resident)

| Path | TX/s | user_key_cmp/q | iter_next_cpu/q | iostats bytes_read/q | hit_rate |
|------|-----:|---------------:|----------------:|---------------------:|---------:|
| S1 base_merge  | 1.67 | 274 336 | 549.7 ms | 17.4 MiB | 100% |
| S2 view        | 13.03 | 12 | 72.0 ms | 2.7 KiB | 100% |
| S3 mi_coli     | 1.58 | 159 663 | 578.2 ms | 16.5 MiB | 100% |
| S4 base_hash   | 1.70 | 40 | 541.8 ms | 16.1 MiB | 100% |
| S5 aCOLI       | 11.63 | 24 749 | 80.0 ms | 9.1 KiB | 100% |

**Findings:**

- **A2a (fast_decode) refuted.** S3 does *fewer* comparisons than S1
  (160k vs 274k) — S1's 3-iterator BMJ chain calls the comparator more
  times. Tagged-key decode is **not** the bottleneck. (S4 hash join is
  even lower at 40 — joins via hash, not comparator.)
- **A2c (fused_emit) is the live suspect.** `iter_next_cpu_nanos/q`
  is ~5% higher on S3 (578 ms vs 550 ms), matching the ~5% TX/s gap
  on macOS exactly. Per-record `next()` payload construction is the
  remaining differential.
- **Run is cache-resident** (100% hit rate across all paths). The
  merged-index disk-pressure pitch can't be evaluated here — bumps
  A6 (memory-pressure sweep) earlier in priority. macOS at
  dram=0.02 GiB or SF=40 needed to force eviction.
- **macOS dram=0.025 follow-up (DB ~33 MiB > 26 MiB cache)**:
  S3=1.59, S1=1.61, S4=1.62, S2=12.27, S5=11.70. Block-cache hit
  rate still 100% — **OS page cache absorbs the I/O even when
  RocksDB's internal cache is undersized**. macOS isn't a useful
  testbed for the disk-bound regime; Linux (or RocksDB
  `use_direct_reads=true`) needed. Notably user_key_comparison_count
  drops from ~274k → 34 between the two runs — likely a
  warm-vs-cold-iterator artefact in PerfContext (bloom-filter /
  index-block lookups counted on first traversal only).
- **macOS gap is 5%, Linux is 16%** at same SF/dram. Awaiting user's
  q3i_btree Linux numbers to compare scanner-emit attribution
  cross-backend.

#### Linux q3i_btree A1 result (2026-05-03, dram=0.1, LeanStore)

LeanStore has no `PerfContext` analog; A1 metrics ported via
`WorkerCounters::dt_*` arrays + a chrono-instrumented per-`next()`
accumulator (`tpch::scanner_perf::iter_next_ns_acc`). `user_key_cmp/q`
is mapped to `tuples_advanced/q` (Σ `dt_next_tuple`), which captures
the same per-record-cost asymmetry the decision tree reads.

The chrono hook costs ~50ns per `next()` call when `--micro_perf=true`
— at 160k tuples/q this inflates absolute `iter_next_cpu/q` by ~8 ms/q
and lowers TX/s by ~25% vs §1 baselines on the cache-resident paths.
**S3-vs-S1 ratios are robust** (both pay the same per-call overhead);
absolute numbers should be read as upper bounds.

**SF=15 dram=0.1 (cache-resident, 99.5–100% hit rate):**

| Path | TX/s | tuples_advanced/q | iter_next_cpu/q | bytes_read/q | hit_rate |
|------|-----:|------------------:|----------------:|-------------:|---------:|
| S1 base_merge  | 22.96  | 160 041 | 34.78 ms | 153.8 KiB | 99.7% |
| S2 view        | 285.97 | 22 501  | 2.69 ms  | 0.5 KiB   | 100%  |
| S3 mi_coli     | 14.56  | 160 038 | 58.28 ms | 242.8 KiB | 99.5% |
| S4 base_hash   | 20.85  | 160 041 | 38.16 ms | 159.9 KiB | 99.7% |
| S5 aCOLI       | 216.58 | 24 751  | 3.65 ms  | 1.3 KiB   | 100%  |

**SF=40 dram=0.1 (DRAM-bound on raw paths):**

| Path | TX/s | tuples_advanced/q | iter_next_cpu/q | bytes_read/q | hit_rate |
|------|-----:|------------------:|----------------:|-------------:|---------:|
| S1 base_merge  | 0.26   | 426 224 | 3722 ms  | 113.7 MiB | 22.5% |
| S2 view        | 106.15 | 60 001  | 7.4 ms   | 3.6 KiB   | 99.9% |
| S3 mi_coli     | 0.26   | 426 221 | 3778 ms  | 114.0 MiB | 21.0% |
| S4 base_hash   | 0.37   | 426 224 | 2638 ms  | 99.3 MiB  | 26.4% |
| S5 aCOLI       | 74.37  | 66 001  | 10.97 ms | 9.8 KiB   | 99.9% |

**Findings:**

- **A2a (fast_decode) refuted on Linux** (same direction as macOS).
  `tuples_advanced/q` is **identical** between S1 and S3 (160 041 vs
  160 038 at SF=15; 426 224 vs 426 221 at SF=40). The COLI walker
  and the 4-way custkey-merge advance the same set of records at the
  data-tree level. Tagged-key decode is not the bottleneck.
- **A2c (fused_emit) confirmed on Linux at SF=15.**
  `iter_next_cpu/q` is 58.28 ms on S3 vs 34.78 ms on S1 —
  **68% gap, ~1.7× per-call cost** (S3 ≈ 364 ns/call vs S1 ≈ 217
  ns/call after subtracting the ~50 ns chrono-hook tax). Crucially,
  `iter_next_cpu/q` accounts for ~80% of the S1 per-query wall-clock
  (35 ms of 43.55 ms); the 23 ms iter_next gap entirely covers the
  25 ms total query-time gap (S1 = 43.55 ms vs S3 = 68.70 ms). On
  LeanStore the `MergedScanner::next()` returns
  `std::variant<customer_coli_t, orders_coli_t, lineitem_coli_t,
  invoice_coli_t>` per record vs S1's typed `LeanStoreScanner<R>`
  emitting `std::pair<R::Key, R>` — the variant construction +
  per-record memcpy of the widest payload is the live differential.
- **macOS gap = 5%, Linux gap = 68%.** Same direction, much stronger
  on Linux. Two contributing factors: (a) the chrono hook captures
  the *whole* per-`next()` body including the variant copy, while
  RocksDB's PerfContext only times its iterator-internal advance;
  (b) LeanStore's `LeanStoreMergedScanner::next()` payload emission
  (the `current()` body that builds two variants via `unfoldKey` +
  `reinterpret_cast` of the buffer-pool slot) may genuinely be
  fatter than RocksDB's analog. Either way, A2c is the right next
  step on both backends.
- **SF=40 collapses S1/S3/S4 to ~0.26–0.37 TX/s** on the chrono-hooked
  run (vs §1 baseline 0.34–0.39). The diagnostic adds modest overhead
  even under DRAM pressure. `bytes_read/q` ≈ 99–114 MiB on the raw
  paths confirms full-MI scan dominates at this scale; `hit_rate`
  drops to ~21–26% (vs 100% on macOS at SF=15) — Linux does provide
  the disk-bound regime macOS couldn't.
- **H8 stays open but ungapped.** S3 vs S1 hit_rate at SF=40 = 21.0%
  vs 22.5% — within noise; not a differential-pollution signal.
  A5's isolated-DB experiment can still confirm/refute the uniform-
  overhead hypothesis, but it's no longer competing with A2c for
  next-step priority.

**Decision: A2c remains the priority A-test**, now confirmed on both
backends. Move forward with `fused_emit` /
`RocksDBMergedScanner::next()` and `LeanStoreMergedScanner::next()`
specialisation as the next implementation plan.

### A2 — Walker dispatch variants (`--coli_walker_variant=...`)

A1 narrowed this: tagged-key decode (A2a) is refuted on macOS;
scanner emit (A2c) is the live suspect. Variants coexist behind one
flag for within-process A/B; XOR parity across variants is mandatory.

- **A2c `fused_emit` (PRIORITY, A1-confirmed cross-backend)**:
  specialise `*MergedScanner::next()` to emit raw
  `(tag, k_view, v_view)` triple — no `std::variant` construction.
  Skip variant copy when visitor returns void.
  WHERE: `frontend/shared/adapter-scanner/RocksDBMergedScanner.hpp`
  + `frontend/shared/adapter-scanner/LeanStoreMergedScanner.hpp`
  + walker call site in `frontend/tpch/coli_pipeline.tpp`.
  WIN: close the 5% (RocksDB macOS cache-resident) / 68%
  (LeanStore Linux SF=15) iter_next_cpu_nanos gap.
- **A2b `template_dispatch`**: hand-rolled templated dispatch over a
  tag-byte switch; skip variant construction in the dispatcher.
  Probably subsumed by A2c if the bottleneck is the variant itself.
- **A2a `fast_decode` (DEPRIORITISED, A1 REFUTED)**: replace SFINAE
  `accepts_key` chain with direct `key[key_len-1]` tag-byte switch.
  Predicted 1ns/record × 425k ≈ 400 µs/q win, but A1 macOS shows S3's
  comparator count is *lower* than S1's — fast_decode optimises the
  wrong axis. Keep on the bench; revisit only if A2c is also refuted.
- WIN bar (any variant): ≥5% TX/s improvement on RocksDB SF=15
  dram=0.1 with parity intact, OR a clean declaration that the gap is
  fundamental.

### A3 — Re-enable physical seek-skip on LeanStore (H2 B-tree branch)

- **WHAT**: gate `USE_PHYSICAL_SEEK_SKIP` on the `Backend` trait so
  LeanStore takes the Seek branch while RocksDB stays on forward
  iteration.
- **WHERE**: `frontend/tpch/coli_pipeline.tpp:539` (constexpr
  declaration site).
- **MEASURE**: SF=15 / SF=40 dram=0.1 LeanStore TX/s vs current
  baseline (S3=25.41, S3=0.3667).
- **WIN**: ≥10% lift on LeanStore SF=15. Predicted because rejected
  custkey groups span 10–50 records each; B-tree Seek is `O(log N)`
  page touches with no prefetch buffer to invalidate.

### A4 — H7 SSTWrite anomaly: four targeted tests in one binary

- **WHAT**: `--h7_test={none,nobg,opt,nocommit,sstdelta}` flag isolates
  one source of write traffic during read-only queries.
- **WHERE**: `frontend/shared/RocksDB.hpp` (DB open path,
  TransactionDB → OptimisticTransactionDB switch); `tpch_executable.hpp`
  (`PauseBackgroundWork` wrap; commit elision).
- **MEASURE**: SSTWrite/TX with each variant.

| Variant | Implementation | Expected if cause |
|---------|----------------|-------------------|
| `nobg` | `db->PauseBackgroundWork()` around `helper.run()` | Compaction-driven |
| `opt` | Open as `OptimisticTransactionDB` | WAL-driven |
| `nocommit` | Skip `txn->Commit()` for read-only | WAL-commit-marker |
| `sstdelta` | Per-level SST count diff before/after run | Confirms L0→L_n promotions |

- **WIN**: at least one variant drops SSTWrite/TX to ~0.

### A5 — Isolated-DB benchmark (H8 quantification)

- **WHAT**: build separate `--ssd_path` directories each containing
  only base tables + the single queried secondary. Compare absolute
  TX/s and the S3-vs-S1 relative gap to the current shared-DB setup.
- **WHERE**: harness-level — separate Makefile targets or a
  `--load_only_structure=N` flag in `Q3IWorkload::load()`.
- **MEASURE**: TX/s + `block_cache_hit_rate` for each path, isolated
  vs shared.
- **WIN**:
  - S3-vs-S1 gap unchanged → H8 confirmed as uniform overhead.
  - Gap shifts → pollution is differential; revisit shared-DB load
    strategy.

### A6 — RocksDB memory-pressure sweep

- **WHAT**: dram ∈ {0.05, 0.1, 0.5, 1.0, full} at SF=40, RocksDB, all
  five paths.
- **WHERE**: pure run sweep, no code change. Use the existing
  `make q3i_lsm dram=$X scale=40` per cell.
- **MEASURE**: TX/s, `block_cache_hit_rate`, `block_read_byte/TX` vs
  dram.
- **WIN**: identify the operating point (if any) where S3 beats
  S1/S4. If it doesn't exist on this axis, S3's pitch as
  "merged-index advantage at memory pressure" is dead and S5 carries
  the showcase alone.

### A7 — LeanStore aCOLI size content-walk

- **WHAT**: mirror the RocksDB `[content/row]` + `[overhead]` walk
  added in `tests/q3i/test_query_q3i_rocksdb.cpp` for the LeanStore
  harness, so the LeanStore aCOLI inflation (141 MiB at SF=40 vs
  ~30 MiB cardinality estimate) is traceable to leaf-fill ratio
  rather than a measurement bug.
- **WHERE**: `tests/q3i/test_query_q3i_leanstore.cpp`
  + `frontend/shared/adapter-scanner/LeanStoreMergedAdapter.hpp`
  (add a content-walk method).
- **MEASURE**: estimatePages × page_size vs key+value content sum;
  estimateLeafs vs internal-node count; per-record bytes from a
  typed scanner walk.
- **WIN**: account for the ~5× inflation entirely (e.g. "70% leaf
  fill + 18% internal nodes") or surface a second measurement bug.

---

## §4 — Reviewer relevance

REVIEWS.md §4.2 (R3-W2 / R3-D3-5) asks for evidence that merged
indexes outperform traditional joins on medium-to-large scans.
**Lead the reviewer response with S5**: it collapses scan
cardinality 22× by baking simple aggregates into the MI as included
columns, runs ~260× faster than S1/S3/S4 at SF=40 dram=0.1 (RocksDB
123 TX/s vs ~0.4) on disk pressure, and remains parameter-flexible
across mktsegment/threshold/orderdate. S3 is the
parameter-flexible-but-raw counterpart; the S3-vs-S1/S4 gap is the
*cost* of that flexibility, not a failure of merged indexes
generally. Full discussion in archive §5.

---

## §5 — Process

- Active worklist updates land **here**.
- Long-form evidence and refutations go to the archive (`git log`
  is the audit trail; new dated archive snapshots when this file
  becomes long again).
- Each completed A-test promotes the relevant H-row in §2 to
  CONFIRMED/REFUTED with a one-line evidence pointer (commit SHA or
  archive section).
- Implementation plans for A2–A7 each get their own follow-up plan
  file in `.claude/plans/` when picked up.
