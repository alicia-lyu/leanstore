# Q3I S3 Performance Investigation — Active Worklist

Forward-looking worklist. Evidence trails for completed runs live in
[`archive/PERFORMANCE-2026-05-03b.md`](archive/PERFORMANCE-2026-05-03b.md)
(A1 + A5 + A2c) and
[`archive/PERFORMANCE-2026-05-03.md`](archive/PERFORMANCE-2026-05-03.md)
(pre-A1 history).

---

## §1 — Where we are

Two remediations have landed; the merged-index pitch on LeanStore is
now solid across both regimes:

- **A2c (`fused_emit`, SF=15 cache-resident).** S3 matches/beats S1 on
  both backends. H4 closed.
- **A3 (Backend-trait Seek-skip, LeanStore only).** Customer-level
  Seek across rejected custkey groups: SF=15 23.06 → 105.27 TX/s
  (+356%); SF=40 disk-bound 0.29 → 33.69 TX/s (+116×). H2 closed on
  LeanStore. RocksDB unchanged (Seek invalidates prefetch buffer; A/B
  refuted earlier — trait stays off there).

LeanStore S3 (iso, fused_emit, dram=0.1) reference numbers:

| stage              | SF=15 TX/s | SF=40 TX/s |
|--------------------|-----------:|-----------:|
| baseline (no A2c)  | 19.52      | ~0.30      |
| + A2c              | 23.06      | ~0.30      |
| + A2c + A3         | **105.27** | **33.69**  |
| S1 (ref)           | 22.63      | 0.30       |
| S4 (ref)           | 21.61      | 0.32       |

Open question: on **RocksDB**, what gets disk-pressure S3 above S4?
A4 (SSTWrite source) and A6 (memory-pressure sweep on LSM, post-A2c)
are the live levers. On both backends, **H12 (load-amortized cost vs
query count)** is the second-order story the paper should tell anyway.

---

## §2 — Hypothesis ledger

| ID | Hypothesis                              | Status | One-line takeaway |
|----|-----------------------------------------|--------|-------------------|
| H1 | MI too large per row                    | **REFUTED** | `get_size` artefact (closed in `50fd2052`) |
| H2 | Iter overhead on rejected groups        | **CONFIRMED + REMEDIATED on LeanStore (A3, `83870b48`); REVERTED on RocksDB (`8d10782b`)** | B-tree Seek to next custkey: +356% SF=15, +116× SF=40 |
| H3 | Walker visits entire MI per query       | **CONFIRMED uniform (subsumed by H6)** | Doesn't explain S3-vs-S1 gap |
| H4 | Per-record dispatch overhead            | **CONFIRMED + REMEDIATED at SF=15 (A2c, `88088305`)** | fused_emit closes per-call gap to ~3% of S1; neutral at SF=40 disk-bound |
| H5 | Storage-engine specific                 | **REFUTED** | Same direction on LeanStore |
| H6 | Low filter selectivity                  | **CONFIRMED uniform** | Explains S5 win |
| H7 | SSTWrite during read-only queries       | **OPEN, RocksDB-specific (A4)** | Histogram inflated; source unattributed |
| H8 | Shared-DB cache pollution               | **SPLIT: differential SF=15, symmetric SF=40 (LeanStore)** | SF=15 baseline numbers were partly artefact |
| H9 | Per-record-width tax                    | **REFUTED** | No wide-union padding on disk; `bytes_read/q` parity at SF=40 |
| H10| Compression masks locality              | **REFUTED** | Cross-backend disk-bound TX/s consistent |
| H11| S3 vs S1 is the wrong baseline          | **PARTIALLY REFUTED on LeanStore (post-A3)** | LeanStore S3-vs-S4 at SF=40 is now +100×; merged-index pitch is alive on B-tree. Open on RocksDB. |
| H12| Load-amortized cost is the real metric  | **OPEN** | Crossover (queries vs total time) is the right axis |

Full evidence: archive `PERFORMANCE-2026-05-03b.md` §3.

---

## §3 — Active worklist

### A6 — RocksDB memory-pressure sweep, post-A2c (top priority)

Now that LeanStore disk-bound is solved by A3, **RocksDB is the only
backend where S3 still doesn't dominate at disk pressure**. A6 sweeps
the operating envelope to find the cell (or prove there isn't one).

- **WHAT**: dram ∈ {0.05, 0.1, 0.5, 1.0, full} at SF=40, all five
  paths, **shared and iso**, with `--coli_walker_variant=fused_emit`,
  RocksDB only.
- **WHERE**: `make q3i_lsm dram=$X scale=40
  coli_walker_variant=fused_emit`; iso variants via
  `q3i_lsm_iso_N` targets.
- **MEASURE**: TX/s, `block_cache_hit_rate`, `block_read_byte/TX`,
  `iter_next_cpu/q`, S3-vs-S4 gap (per cell).
- **OUTCOME**:
  - S3 beats S4 at any cell → merged-index pitch lives on RocksDB
    too; report the envelope.
  - S3 never beats S4 → RocksDB disk-pressure pitch is dead; lead
    paper with LeanStore A3 numbers + S5 cross-backend.

### H12 — Load-amortized cost vs query count

The reviewer-facing question is "how many queries justify a merged
index?" Steady-state TX/s alone can't answer it.

- **TEST**: instrument `load()` to emit per-secondary build time;
  compute `total_time(N) = load_time + N / TX_per_sec` for each
  structure on both backends; identify the crossover point.
- **OUTCOME**: report the crossover as a primary metric. Likely
  S4 wins low-N (no build cost), S1/S3 win high-N. Useful even if
  the steady-state numbers favour S3 already.

### A4 — H7 SSTWrite source attribution (RocksDB only)

Read-only Q3I reports nonzero SSTWrite/TX. Mechanism candidates:
compaction during run, WAL writes, commit markers, L0→L_n promotions.

- **TEST**: `--h7_test={nobg,opt,nocommit,sstdelta}` flag.
- **WHERE**: `frontend/shared/RocksDB.hpp` (DB open path);
  `tpch_executable.hpp` (`PauseBackgroundWork` wrap; commit elision).

| Variant     | Implementation                                    | Confirms if SSTWrite/TX → 0 |
|-------------|---------------------------------------------------|-----------------------------|
| `nobg`      | `db->PauseBackgroundWork()` around `helper.run()` | Compaction                  |
| `opt`       | Open as `OptimisticTransactionDB`                 | WAL                         |
| `nocommit`  | Skip `txn->Commit()` for read-only                | WAL commit marker           |
| `sstdelta`  | Per-level SST count diff before/after run         | L0→L_n promotion            |

### A7-followup — Unblock LeanStore parity harness, then run content-walk

Code landed in `83870b48`
(`LeanStoreMergedAdapter::content_bytes_walk()`,
`LeanStoreAdapter::content_bytes_walk()`, test wiring). **Measurement
blocked**: the LeanStore parity harness segfaults during
"Populating secondaries" with `--vi=false --mv=false
--isolation_level=ser` at SF=1 (pre-existing; also fails before A7
landed — see commit `d05aa719`). With `--vi=true --wal=true` it aborts
earlier in `loadInvoiceAndLinkLineitem`.

- **NEXT**: separate plan to get LeanStore parity harness green at
  SF=1, then emit the `[content/row]` / `[fill]` triple per
  structure. Decision (real low-fill vs measurement bug) deferred
  until then.

---

## §4 — Reviewer relevance

REVIEWS.md §4.2 (R3-W2 / R3-D3-5) asks for evidence merged indexes
beat traditional joins on medium-to-large scans. The story now reads:

- **S5 (aCOLI MI)** carries the showcase: ~260× over raw paths at
  SF=40 dram=0.1, parameter-flexible. The "merged-index +
  pre-aggregation" point is uncontested.
- **S3 on LeanStore** with fused_emit + Backend-trait Seek-skip beats
  S1/S4 by 100× at SF=40 disk-bound — the parameter-flexible-but-raw
  merged index *also* wins on B-tree. Hits the reviewer ask
  directly.
- **S3 on RocksDB** still neutral at disk pressure (A6 will resolve).
- **H12 crossover** answers "how many queries justify a merged
  index" — second-order story regardless of how A6 lands.

---

## §5 — Process

- Active worklist updates land here.
- Evidence and refutations → archive.
- Completed A-test → promote H-row in §2 with commit SHA.
- Per-A-test implementation plans live in `.claude/plans/`.

### Refuted/closed (recorded so they aren't re-investigated)

- **H1, H5, H9, H10**: refuted; see §2.
- **H2 (RocksDB), H4 (SF=15)**: remediated; see §2.
- **A2a (fast_decode), A2b (template_dispatch)**: deprioritised. A2c
  closed the per-call gap to ~3%; reopen only if a new test surfaces
  per-call asymmetry.
- **A3 RocksDB equivalent**: not viable. SST prefetch buffer is
  invalidated by physical Seek (~5× regression at SF=40 disk-bound
  per archived A/B). Forward iteration stays the default for RocksDB.
