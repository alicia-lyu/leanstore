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
H4 is CONFIRMED + REMEDIATED. **A2c is neutral at SF=40 disk-bound** —
variant cost is masked by page-fault wait, so the live cost there is
something else (paging, decompression, or LSM merge logic).

The open question pivots: *is there a `(dram, SF)` operating point
where S3 (with fused_emit) actually beats S1/S4?* If yes →
merged-index pitch strengthens. If no → S5 carries the showcase alone.

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
| H2 | Iter overhead on rejected groups        | **CONFIRMED, fix reverted on RocksDB; OPEN on LeanStore (A3)** | Forward iter cheaper on RocksDB; B-tree branch unexplored |
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

### A3 — Re-enable physical seek-skip on LeanStore (H2 B-tree branch)

- **WHAT**: gate `USE_PHYSICAL_SEEK_SKIP` on the `Backend` trait so
  LeanStore takes the Seek branch while RocksDB stays on forward
  iteration.
- **WHERE**: `frontend/tpch/coli_pipeline.tpp` (constexpr declaration
  site).
- **MEASURE**: SF=15 / SF=40 dram=0.1 LeanStore TX/s vs current iso
  baseline (S3 fused_emit = 23.06 / ~0.30).
- **WIN**: ≥10% lift on LeanStore SF=15 S3 over fused_emit. Predicted:
  rejected groups span 10–50 records each; B-tree Seek is `O(log N)`
  page touches with no prefetch buffer to invalidate.

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

### A7 — LeanStore aCOLI content-walk

- **WHAT**: mirror the RocksDB `[content/row]` + `[overhead]` walk for
  the LeanStore harness so the LeanStore aCOLI inflation (141 MiB at
  SF=40 vs ~30 MiB cardinality estimate) is traceable to leaf-fill
  ratio rather than another measurement bug.
- **WHERE**: `tests/q3i/test_query_q3i_leanstore.cpp` +
  `frontend/shared/adapter-scanner/LeanStoreMergedAdapter.hpp` (add a
  content-walk method).
- **MEASURE**: estimatePages × page_size vs key+value content sum;
  estimateLeafs vs internal-node count.
- **WIN**: account for the ~5× inflation (e.g. "70% leaf fill + 18%
  internal nodes") or surface a second measurement bug.

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
