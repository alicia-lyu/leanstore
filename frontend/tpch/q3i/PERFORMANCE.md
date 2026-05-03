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

## §3 — Active worklist (hypothesis-driven, priority order)

The framing question is: *what mechanism would let S3 beat the
baselines at disk pressure, and what's the falsifying test?* Each
entry below names a mechanism, the prediction it makes, and the
remediation if confirmed.

### H11 — S3 vs S1 is the wrong baseline (cheapest test, biggest reframe)

**Mechanism.** S1 is a 4-way merge over **custkey-sorted secondaries
that we built at load time**. S3 is the merged-index version of the
same logical plan. They both stream sorted-by-custkey at SF=40 and
read ~114 MiB/q. The merged-index pitch is *not* "we beat custkey
sorted secondaries" — it's "we don't need to maintain those
secondaries." The valid disk-pressure baseline is **S4 (hash join over
unsorted base tables)**, which has no custkey-sorted prerequisite.

- **PREDICTION**: at SF=40 dram=0.1, S3-fused vs S4 should show a
  meaningful gap (in either direction); S3-vs-S1 should stay tied.
- **TEST**: pull the existing SF=40 numbers from
  `archive/PERFORMANCE-2026-05-03b.md §3 A1` (LeanStore S3 0.26 vs S4
  0.37 — S3 is *slower* than S4 by 30% under disk pressure already).
  No new run needed for the headline; rerun with fused_emit at SF=40
  to confirm A2c doesn't move it.
- **REMEDIATION** if confirmed (S3 tied or losing to S4 at disk pressure):
  reframe paper around S5 (which already wins ~260× at SF=40) and
  retire the S3 disk-pressure pitch. S3's role becomes "the
  parameter-flexible reference point that proves S5's pre-aggregation
  is what earns the speedup, not the merged-index layout."
- **REMEDIATION** if refuted (S3-fused beats S4 at SF=40 dram=0.1):
  the merged-index pitch lives. Then run A6-confirm below to map the
  envelope.

### H12 — Load-amortized cost is what the paper should report

**Mechanism.** S1's custkey-sorted secondaries cost something to build
at load time; S4 builds equivalent hashmaps per-query. S3 amortizes
the sort across all queries. The right comparison axis is
**cumulative time vs query count**, not steady-state TX/s.

- **PREDICTION**: at low query count, S4 wins (no build cost
  amortized); at high query count, S1 and S3 win; the crossover lets
  the paper answer "how many queries justify a merged index."
- **TEST**: instrument load() to dump build time per secondary;
  compute `total_time(N) = load_time + N / TX_per_sec` for each
  structure; plot the crossover.
- **REMEDIATION**: report the crossover as a primary metric, not a
  footnote.

### A3 — Re-enable physical seek-skip on LeanStore (H2 B-tree branch)

**Mechanism.** `USE_PHYSICAL_SEEK_SKIP=false` was set globally because
RocksDB's prefetch buffer invalidates on physical Seek. LeanStore's
B-tree has no such buffer; physical Seek across rejected custkey
groups should be cheaper than forward-iterating each record there.

- **PREDICTION**: ≥10% lift on LeanStore SF=15 S3-fused over the
  current 23.06 iso baseline. Rejected groups span 10–50 records;
  Seek is `O(log N)` page touches.
- **TEST**: gate the constexpr on `Backend` trait in
  `frontend/tpch/coli_pipeline.tpp`. RocksDB stays on forward iter.
- **REMEDIATION**: if confirmed, ship as default for LeanStore. If
  refuted, document and move on.

### A4 — H7 SSTWrite source attribution (RocksDB only)

Read-only Q3I queries report nonzero SSTWrite/TX. Source unknown.

- **MECHANISM CANDIDATES**: compaction during run; WAL writes for
  read-only txns; commit-marker writes; L0→L_n promotions.
- **TEST**: `--h7_test={nobg,opt,nocommit,sstdelta}` flag isolates
  each candidate.
- **WHERE**: `frontend/shared/RocksDB.hpp` (DB open path);
  `tpch_executable.hpp` (`PauseBackgroundWork` wrap; commit elision).

| Variant     | Implementation                                    | Confirms cause if SSTWrite/TX → 0 |
|-------------|---------------------------------------------------|-----------------------------------|
| `nobg`      | `db->PauseBackgroundWork()` around `helper.run()` | Compaction-driven                 |
| `opt`       | Open as `OptimisticTransactionDB`                 | WAL-driven                        |
| `nocommit`  | Skip `txn->Commit()` for read-only                | WAL-commit-marker                 |
| `sstdelta`  | Per-level SST count diff before/after run         | L0→L_n promotions                 |

### A7 — LeanStore aCOLI inflation: leaf-fill or measurement bug?

LeanStore aCOLI reports 141 MiB at SF=40; cardinality estimate is
~30 MiB.

- **MECHANISM CANDIDATES**: low leaf fill ratio (real); page-count
  reporting bug (mirrors the RocksDB `get_size` bug from `50fd2052`).
- **TEST**: mirror the RocksDB `[content/row]` + `[overhead]` walk
  for the LeanStore harness.
- **WHERE**: `tests/q3i/test_query_q3i_leanstore.cpp` +
  `frontend/shared/adapter-scanner/LeanStoreMergedAdapter.hpp`.
- **OUTCOME**: account for the ~5× inflation (e.g. "70% leaf fill +
  18% internal-node overhead") or surface a second measurement bug.

### Confirmation runs (NOT investigations)

- **A6-confirm — dram sweep with fused_emit**: only runs *after* H11
  refuted. dram ∈ {0.05, 0.1, 0.5, 1.0, full} at SF=40, all paths,
  with fused_emit. Maps the envelope where S3 wins. Doc-keeping only;
  the hypothesis work was H11.
- **A6-mini — RocksDB SF=15 dram=0.025 fused_emit regression**:
  5-minute regression to confirm A2c closes the macOS-OS-cache-
  resident corner from A1 (baseline S3=1.59 vs S1=1.61). No new
  signal expected.

### Refuted candidates (recorded so they're not re-investigated)

- **H9 — per-record-width tax (REFUTED)**. S3 doesn't pay
  wide-union padding on disk; tagged-record disk format is its
  actual payload + 1 trailing tag byte. S1's split indexes also
  repeat the custkey prefix in each row. SF=40 `bytes_read/q` is
  ~114 MiB on both, confirming no width delta.
- **H10 — compression masking locality (REFUTED)**. RocksDB compresses
  redundant custkey prefixes; LeanStore B-tree doesn't. If H10 were
  real, SF=40 disk-bound TX/s should differ across backends; it
  doesn't.
- **A2 follow-ups — fast_decode / template_dispatch (DEPRIORITISED)**.
  H4 closed at SF=15 (A2c, +18-50% iso/shared). Reopen only if any
  test above surfaces a new per-call asymmetry.

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
(RocksDB 123 vs ~0.4 TX/s), parameter-flexible across mktsegment/
threshold/orderdate. With A2c landed, **S3 matches S1 in the
cache-resident regime**; whether S3 beats the *correct* baseline
(S4 hash, not S1 merge — see H11 in §3) at disk pressure is the
remaining open question. Even if H11 refutes a steady-state S3 win,
H12 (load-amortized cost vs query count) is the second-order story
the paper should tell anyway. Full historical discussion: archive
`PERFORMANCE-2026-05-03.md` §5.

---

## §5 — Process

- Active worklist updates land **here**.
- Long-form evidence and refutations go to the archive.
- Each completed A-test promotes the relevant H-row in §2 with a
  one-line evidence pointer (commit SHA or archive section).
- Implementation plans for each A-test get their own follow-up plan
  file in `.claude/plans/` when picked up.
