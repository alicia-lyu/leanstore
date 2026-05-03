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
| H4 | Per-record dispatch overhead | **OPEN** | Not `std::visit` alone (geo precedent); A2 variants will attribute |
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

### A2 — Walker dispatch variants (`--coli_walker_variant=...`)

Run only after A1 attributes the gap to per-record cost. Three
variants coexist behind one flag for within-process A/B; XOR parity
across variants is mandatory.

- **A2a `fast_decode`**: replace SFINAE `accepts_key` chain with
  direct `key[key_len-1]` tag-byte switch.
  WHERE: `frontend/tpch/coli_pipeline.tpp` walker dispatch site +
  `frontend/shared/adapter-scanner/RocksDBMergedAdapter.hpp::toType`.
  WIN: ~1ns/record × 425k = ~400 µs/query at SF=40.
- **A2b `template_dispatch`**: hand-rolled templated dispatch over the
  tag-byte switch; skip variant construction when visitor is void.
- **A2c `fused_emit`**: specialise `RocksDBMergedScanner::next()` to
  emit raw `(tag, k_view, v_view)` triple — no `std::variant`
  construction.
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
