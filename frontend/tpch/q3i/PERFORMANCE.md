# Q3I S3 Performance Investigation — Active Worklist

This is the **forward-looking** worklist. Evidence trails for completed
runs live in
[`archive/PERFORMANCE-2026-05-03b.md`](archive/PERFORMANCE-2026-05-03b.md)
(A1 + A5 + A2c cross-backend) and
[`archive/PERFORMANCE-2026-05-03.md`](archive/PERFORMANCE-2026-05-03.md)
(pre-A1 hypothesis history).

---

## §1 — Where we are

**A2c is done at SF=15.** With `--coli_walker_variant=fused_emit`, S3
matches or beats S1 on both backends in the cache-resident regime.
H4 is CONFIRMED + REMEDIATED. **A2c is neutral at SF=40 disk-bound**
on RocksDB; on LeanStore A3 (customer-level Seek-skip, gated by
Backend trait) lifts SF=40 S3 from 0.29 → 33.69 TX/s and SF=15 from
23.06 → 105.27 — see §2 H2 and §3 A3.

The open question pivots: *is there a `(dram, SF)` operating point
where S3 (with fused_emit + A3) actually beats S1/S4 on both
backends?* On LeanStore SF=40 S3 = 33.69 already looks dominant; A6
needs to remeasure all five paths with the trait on to confirm
relative ordering. RocksDB still trends neutral at SF=40 disk-bound
(trait stays off; Seek invalidates the SST prefetch buffer).

S3 reference numbers (LeanStore SF=15 dram=0.1):

| variant       | shared TX/s | iso TX/s | per-call iter_next |
|---------------|------------:|---------:|-------------------:|
| baseline      | 14.56       | 19.52    | 279 ns             |
| fused_emit    | 21.91       | 23.06    | 227 ns             |
| S1 (ref)      | 22.96       | 22.63    | 221 ns             |

Full per-path tables (all five paths, both backends, both SFs) live
in `archive/PERFORMANCE-2026-05-03b.md` §1 + §3.

---

## §2 — Hypothesis ledger

| ID | Hypothesis                              | Status | One-line takeaway |
|----|-----------------------------------------|--------|-------------------|
| H1 | MI too large per row                    | **REFUTED**                       | `get_size` artefact (closed in `50fd2052`) |
| H2 | Iter overhead on rejected groups        | **CONFIRMED + REMEDIATED on LeanStore (A3, commit 83870b48); REVERTED on RocksDB (8d10782b)** | B-tree Seek to next custkey: SF=15 iso 23.06→105.27 TX/s (+356%); SF=40 iso 0.29→33.69 (+116×). RocksDB unchanged (trait stays false). |
| H3 | Walker visits entire MI per query       | **CONFIRMED uniform (subsumed by H6)** | Doesn't explain S3-vs-S1 gap |
| H4 | Per-record dispatch overhead            | **CONFIRMED + REMEDIATED at SF=15 (A2c)** | fused_emit closes per-call gap to ~3% of S1; S3 matches/beats S1 cache-resident; neutral at SF=40 disk-bound |
| H5 | Storage-engine specific                 | **REFUTED**                       | Same direction on LeanStore |
| H6 | Low filter selectivity                  | **CONFIRMED uniform**             | Explains S5 win, not S3-vs-S1 gap |
| H7 | SSTWrite during read-only queries       | **OPEN, RocksDB-specific (A4)**   | Histogram inflated; source unattributed |
| H8 | Shared-DB cache pollution               | **SPLIT: differential SF=15, symmetric SF=40 (LeanStore)** | SF=15 baseline §1 numbers were partly artefact; SF=40 numbers are clean |

Full evidence: archive `PERFORMANCE-2026-05-03b.md` §3 (A1 / A5 / A2c).

---

## §3 — Active worklist (priority order)

### A6 — Memory-pressure sweep with `fused_emit` (highest priority)

A2c closed the cache-resident gap and is **neutral at SF=40
disk-bound**. The live question is now: with fused_emit on, is there
any `(dram, SF)` cell where S3 beats S1/S4? Doubles as the H8
boundary map (differential at SF=15, symmetric at SF=40 — find the
crossover).

- **WHAT**: dram ∈ {0.05, 0.1, 0.5, 1.0, full} at SF=40, all five
  paths, **shared and iso**, **with `--coli_walker_variant=fused_emit`**
  (now also exposed via the Makefile `coli_walker_variant=` variable).
  RocksDB primary; LeanStore as cross-check at the 0.1 / 0.5 corners.
- **WHERE**: pure run sweep —
  `make q3i_lsm dram=$X scale=40 coli_walker_variant=fused_emit`;
  iso variants via `q3i_lsm_iso_N` targets.
- **MEASURE**: TX/s, `block_cache_hit_rate`, `block_read_byte/TX`,
  `iter_next_cpu/q`, S3-vs-S1 gap (shared) vs (iso) per dram cell.
- **WIN**:
  - Find a `(dram, SF)` cell where S3 beats S1/S4. If none exists,
    S3's "merged-index advantage at memory pressure" pitch is dead
    and S5 carries the showcase alone.
  - Map where H8 transitions from differential → symmetric.
  - Identify what *is* the live cost at SF=40: if `block_read_byte/TX`
    is uniform across S1/S3 it's pure paging; if S3's `bytes_read` is
    higher, the COLI MI's wider per-record layout is the disk-pressure
    tax (motivates a project-pushdown follow-up — see
    `frontend/tpch/CLAUDE.md` "No project pushdown" section).

### A6-mini — Top-up regression at RocksDB SF=15 dram=0.025

5-minute confirmation, not a new investigation.

- **WHAT**: rerun the macOS A1 dram=0.025 corner from
  `archive/PERFORMANCE-2026-05-03b.md §3 A1 macOS` with
  `--coli_walker_variant=fused_emit`. Baseline S3=1.59 vs S1=1.61.
- **WIN**: S3 closes the residual; nothing surprising surfaces.

### A3 — DONE (commit 83870b48): customer-level seek-skip lifts LeanStore S3 dramatically on both SF cells.

- `Backend::USE_PHYSICAL_SEEK_SKIP` trait added to `frontend/tpch/backend.hpp`
  (RocksDB=false, LeanStore=true). Both walker variants
  (`coli_group_walk` baseline + `coli_group_walk_fused_emit`) in
  `coli_pipeline.tpp:557, :790` now read the trait. **Customer-level
  seek only** — order-level skip stays as forward iteration on both
  backends (order groups too small to pay back tree descent).
- **Result, LeanStore iso, fused_emit, dram=0.1:**
  - SF=15: 23.06 → **105.27 TX/s** (+356%). iter_next_calls/q drop
    from ~160k (forward iter through every rejected group's records)
    to 33,888.
  - SF=40: 0.29 → **33.69 TX/s** (+116×). The forward-iter path was
    paging in cold leaf pages for ~50 records per rejected custkey
    (~80% of customers fail mktsegment); Seek is O(log N) page
    touches against mostly-cached internal nodes.
- **RocksDB regression check** (trait stays false, behaviour
  unchanged): SF=15 iso S3 fused_emit = 2.68 TX/s (vs A2c reference
  ~2.20, within noise); SF=40 iso = 0.78 TX/s.
- **Implication for A6:** the SF=40 disk-bound regime now has a clear
  S3 lift on LeanStore — A6 should remeasure the (dram, SF) sweep
  with this trait on; H8 and the S3-vs-S1 question may both shift.

### A4 — H7 SSTWrite source attribution (RocksDB only)

- **WHAT**: `--h7_test={none,nobg,opt,nocommit,sstdelta}` flag isolates
  one source of write traffic during read-only queries.
- **WHERE**: `frontend/shared/RocksDB.hpp` (DB open path);
  `tpch_executable.hpp` (`PauseBackgroundWork` wrap; commit elision).
- **MEASURE**: SSTWrite/TX with each variant.

| Variant     | Implementation                                    | Expected if cause       |
|-------------|---------------------------------------------------|-------------------------|
| `nobg`      | `db->PauseBackgroundWork()` around `helper.run()` | Compaction-driven       |
| `opt`       | Open as `OptimisticTransactionDB`                 | WAL-driven              |
| `nocommit`  | Skip `txn->Commit()` for read-only                | WAL-commit-marker       |
| `sstdelta`  | Per-level SST count diff before/after run         | Confirms L0→L_n promotions |

- **WIN**: at least one variant drops SSTWrite/TX to ~0.

### A7 — Code landed (commit 83870b48); measurement blocked by harness

- **Code**: `LeanStoreMergedAdapter::content_bytes_walk()` and the
  typed `LeanStoreAdapter::content_bytes_walk()` added (driven via
  `next_raw()` to bypass variant construction); `[content/row]` /
  `[fill]` triple wired into `tests/q3i/test_query_q3i_leanstore.cpp`.
- **Blocker**: the LeanStore parity harness segfaults during
  `Populating secondaries` with `--vi=false --mv=false
  --isolation_level=ser` at SF=1 — pre-existing issue independent of
  this change (also fails before the A7 code lands; see the BTreeVI
  re-insert limitation noted in commit `d05aa719`'s message). With
  `--vi=true --wal=true` the harness aborts earlier in
  `loadInvoiceAndLinkLineitem`.
- **Next step (separate plan)**: get the LeanStore parity harness
  green at SF=1, then call `content_bytes_walk()` and emit the
  `[content/row]` / `[fill]` triple. The walk method itself is
  validated by linkage and per-row arithmetic in the test code.
  Measurement deferred until harness lands; pause-and-report rather
  than back-fill via the production binary.

### A2-followups (DEPRIORITISED — H4 closed at SF=15)

A2a (fast_decode) refuted on both backends. A2b (template_dispatch)
likely subsumed by A2c — A2c closed the per-call gap to ~3% of S1
and S3 now matches/beats S1 in every cache-resident cell. Reopen
only if A6 surfaces a new per-call asymmetry at some intermediate
dram cell.

---

## §4 — Reviewer relevance

REVIEWS.md §4.2 (R3-W2 / R3-D3-5) asks for evidence merged indexes
beat traditional joins on medium-to-large scans. **Lead with S5**:
collapses scan cardinality 22× by baking simple aggregates as
included columns, runs ~260× faster than S1/S3/S4 at SF=40 dram=0.1
(RocksDB 123 vs ~0.4 TX/s) on disk pressure, and stays
parameter-flexible across mktsegment/threshold/orderdate. With A2c
landed, **S3 also matches S1 in the cache-resident regime** — the
parameter-flexible-but-raw merged index is no longer a clear
slowdown there. The remaining S3 question is whether disk pressure
ever flips it positive (A6). Full discussion: archive
`PERFORMANCE-2026-05-03.md` §5.

---

## §5 — Process

- Active worklist updates land **here**.
- Long-form evidence and refutations go to the archive.
- Each completed A-test promotes the relevant H-row in §2 with a
  one-line evidence pointer (commit SHA or archive section).
- Implementation plans for each A-test get their own follow-up plan
  file in `.claude/plans/` when picked up.
