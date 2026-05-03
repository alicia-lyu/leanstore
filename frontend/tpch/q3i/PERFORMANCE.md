# Q3I S3 Performance Investigation

This document tracks the investigation of an unexpected finding: Q3I S3 (the
COLI MI [`PremergedJoin`] path — the supposed showcase of multi-table merged
indexes) has consistently been the *worst*-performing of the four real paths
under disk pressure (SF=40 / dram=0.1 GiB on RocksDB). It captures the
hypotheses tested, the experiments run, the optimisations tried (some
reverted), and the directions still open.

The narrow customer-scan TX-count effect previously documented in
`CLAUDE.md §Performance Notes` is folded into §2 H6 below.

---

## §1 — The unexpected finding

At **SF=40 / dram=0.1 GiB on RocksDB-LSM**, S3 is the slowest real path.
This is the opposite of the paper's pitch.

| Configuration                       | S1 base_merge | S2 view | S3 mi_coli       | S4 hash |
|-------------------------------------|---------------|---------|------------------|---------|
| baseline (pre-skip; before 1dec2358) | 8.04          | 198.3   | 6.95             | ~8      |
| with physical custkey-Seek skip     | 7.87          | 198.3   | 4.42 (regression)| 8.24    |
| seek-skip disabled (current default)| 7.73          | 198.3   | 6.70 (~15% gap)  | 8.09    |

Numbers in TX/s; see `1dec2358` and `8d10782b` for capture context.

S2's lead is expected — the materialised view absorbs the inside-pipeline
cost at load time. The real comparison axis is **S3 vs S1 / S4**: structures
that all do the equivalent inside-pipeline work at query time, differing
only in physical operator. S3 should win there. It doesn't.

---

## §2 — Hypotheses

Each tagged **CONFIRMED** / **REFUTED** / **OPEN**, with the evidence trail.

### H1 — MI is too large per row → REFUTED

Original suspicion: merged adapter reported 228 bytes/row vs split adapters
~100 bytes/row. Investigation in `3200b2b4`: pure measurement artefact.

- `RocksDBMergedAdapter::size()` returns whole-CF live-data size including
  per-CF SST footer + bloom filter + index blocks (over-reports ~22% at SF=1).
- `RocksDBAdapter::size()` calls `GetApproximateSizes` over a key-prefix
  range in the shared default CF — excludes per-CF metadata, allowed 10%
  approximation slack (under-reports).

Truthful content/row (raw key + value bytes summed via direct iterator
walk): MI=177, splits 150–190. Tagged-key overhead is ~5 bytes per
orders/lineitem record; MI is **7% larger than splits, not 2×**.

The `[content/row]` and `[overhead]` lines in `test_query_q3i_lsm` make
this measurable for any future SF.

### H2 — Iterator overhead on rejected custkey groups → CONFIRMED, fix reverted

`7ae77ff4` introduced a physical Seek-to-next-custkey when `on_customer`
returned false. Customer always sorts first within its group (tag=1 < 2/3/4),
so Seek to `customer_coli_t::Key{custkey+1}` lands at the next group's first
row.

- **In-memory (SF=1)**: 3× speedup. `query_by_merged` 488µs vs 1471µs;
  `mi_records_visited` dropped 10654 → 2841.
- **Disk-bound (SF=40 dram=0.1)**: 5× **regression**. SSTRead/TX jumped
  37k → 177k µs; TX/s dropped 6.95 → 4.42 (`1dec2358`).

Root cause of the disk regression: physical Seek invalidates RocksDB's
iterator prefetch buffer and triggers fresh SST block reads. Forward
iteration through the rejected group is essentially free because all four
record types share the same SST block (the COLI co-location is doing what
it was designed to do — at the block-cache level too).

Fix in `8d10782b`: `USE_PHYSICAL_SEEK_SKIP = false` constexpr default. The
Visitor's `group_active = false` flag still short-circuits per-record
dispatch; the underlying iterator just calls `Next()` through the rejected
group's records. Hook is preserved for cache-resident A/B; flip to `true`
when measuring SF=1 / fully-warm scenarios.

The hypothesis was real; the remedy was wrong on disk.

### H3 — Walker visits the entire MI per query even with skips → CONFIRMED

`1d65d900` plumbed `Q3IStats` totals into the production executable. SF=40
dram=0.1 sample:

```
S3 merged:  cust=6000 ord=11725 lin=46730 inv=23450
            mi_records_visited=425000 (≈ full MI)
            mi_groups_skipped=4841
            per-query: 147 ms
```

S1/S4 reach ~125 ms/q. The walker visits ~425k records per query but only
dispatches Visitor work on the ~20% that pass mktsegment + threshold. The
remaining 80% pay iterator overhead (Slice copy, key-decode, variant tag
check) with no Visitor cost.

For comparison: S1's `BinaryMergeJoin` chain over custkey-sorted splits
reads roughly the same number of base records but in *narrower* per-record
streams that don't share blocks across record types — so each scan walks
fewer SST blocks. S3 trades wider-block-locality for full-MI scan
cardinality.

### H4 — Per-record `std::visit` dispatch overhead → OPEN

The walker calls `std::visit` on every record (~425k/q). Modern compilers
turn this into a small jump table, but at high cardinality even a few ns
per record adds up (425k × 5ns = 2 ms/q — small fraction of the 22 ms gap
to S1/S4 at SF=40).

Not microbenchmarked yet. Worth measuring with `perf record` or replacing
`std::visit` with a templated dispatch behind a constexpr flag for A/B.

### H5 — Storage-engine specific: RocksDB block layout / prefetch → OPEN, ACTIVE

The COLI MI lives in its own RocksDB column family. SST block layout
within that CF interleaves all four record types per custkey, which is the
intended co-location — but it also means **block-cache eviction patterns
differ from S1's narrower split CFs**. Whether this hurts S3 specifically,
or is neutral, is testable.

**Active investigation**: rerun S1–S5 at SF=40 / dram=0.1 GiB on the
**LeanStore B-tree backend** (Linux). Predictions to record before the
results land:

- If S3 wins on B-tree but loses on RocksDB → fault is LSM block layout
  on a 4-record CF. Mitigations: separate CF per record type with shared
  comparator, different SST block size, `BlockBasedTableOptions` tweaks.
- If S3 still loses on B-tree → fault is in the COLI walk pattern itself
  (H3 + H4 — full-MI scan with low filter selectivity). Mitigation: S5
  aCOLI variant or projection pushdown to shrink the per-record cost.
- If S3 wins on both at higher SF → all current measurements are inside
  some cache-effect band that doesn't reflect the asymptotic behaviour.

### H7 — Significant SSTWrite during read-only queries → OPEN

Production runs report SSTWrite(µs)/TX values that are sometimes an
**order of magnitude larger than SSTRead(µs)/TX**, despite Q3I being a
read-only workload at the application level (no inserts, updates, or
deletes during the measurement loop).

`128f6d44` added `RocksDBLogger::capture_baseline()` to subtract the
post-load compaction histogram contribution before the measurement loop
starts. **The baseline-subtract did not move the numbers materially** —
indicating the writes are happening *during* `helper.run()`, not before
it.

Plausible sources, none confirmed:

- **Compaction triggered by read traffic.** Heavy iterator scans can
  promote SST files between levels; the LSM may rewrite during the
  measurement window. Test: dump per-level SST counts before / after
  `helper.run()`; compare against baseline.
- **WAL writes from `txn->Commit()`.** Each query is wrapped in a
  RocksDB transaction (see `RocksDB::TransactionDB`). Even a no-op
  read-only commit may emit a commit marker to the WAL. Test: switch
  to `OptimisticTransactionDB` or skip `Commit()` and re-measure.
- **Iterator-side bloom-filter / block-cache writes.** RocksDB
  occasionally writes back metadata; magnitude should be small.
  Probably noise, not a top suspect.
- **Sample-rate quirk in `SST_WRITE_MICROS` histogram.** The histogram
  is sampled rather than exact; if a few large outlier writes (from
  background compaction) land inside the measurement window after the
  baseline snap, they dominate the total. Test: disable background
  compaction during `helper.run()` (`pause_background_work`) and
  re-measure.

This is a known anomaly affecting all four S1–S4 figures uniformly, but
it does mean the "SSTWrite/TX" column is currently not informative for
distinguishing structures. Until ruled out, **focus on SSTRead/TX and
TX/s** for cross-structure comparison.

### H6 — Filter selectivity makes per-query "useful work" tiny → OPEN

Default Q3I params: c_mktsegment='BUILDING' (~20% of customers),
threshold>0 (~all customers w/ open invoices), o_orderdate < 1995-03-15
(~50% of orders). Final result: 7 rows at SF=1 (LIMIT 10).

The "useful work" per query is therefore proportional to ~20% × 50% ≈ 10%
of base records, but S3 scans 100% of the MI to find them. S5 (aCOLI,
`16e98eb6`) addresses this directly: pre-baking aggregates collapses the
scan to 486 records at SF=1 — a 22× reduction. **Whether S3 can be saved
without baking parameters is the open question that motivates the rest of
this investigation.**

This is also the framing for the customer-scan TX-count effect previously
in `CLAUDE.md §Performance Notes`: at small SF (cache-resident data)
S3's tagged-key overhead per query makes it slower than S1/S4, so it
completes fewer queries in the 15s window, and `customers_scanned`
totals look lower — not because S3 scans fewer per query (it scans
the same number; `populate_merged` inserts every customer
unconditionally), but because the 15s window holds fewer of those
identical scans.

---

## §3 — Investigations done (chronological)

| SHA          | What changed                              | What was measured                                        | Finding |
|--------------|-------------------------------------------|----------------------------------------------------------|---------|
| `7ae77ff4`   | Custkey seek-skip introduced              | SF=1 in-memory                                           | 3× speedup; `mi_records_visited` 10654 → 2841 |
| `1dec2358`   | Extend skip to threshold-fail             | SF=40 dram=0.1                                           | S3 regressed 6.95 → 4.42 TX/s; SSTRead/TX 37k→177k |
| `8d10782b`   | `USE_PHYSICAL_SEEK_SKIP=false`            | SF=40 dram=0.1                                           | S3 = 6.70 TX/s; ~15% gap to S1/S4 remains |
| `6f59e84d`   | Per-stage timers + cardinality counters   | SF=1                                                     | Observability only; all parity [OK] |
| `1d65d900`   | Surface `Q3IStats` in production exec     | SF=40 dram=0.1                                           | Confirmed H3 (full-MI walk per query); S3=147 ms/q vs S1/S4=125 |
| `3200b2b4`   | Truthful content/row + per-CF metadata    | SF=1 (`test_query_q3i_lsm`)                              | Refuted H1; MI=177 vs splits 150–190; per-CF metadata 22% of CF size |
| `d54ad10d`   | Order-group skip + S4 timer collapse      | SF=1                                                     | Orders 1500→375; lineitems 6008→828; stage attribution unified |
| `128f6d44`   | bool `on_xxx` hooks + SSTWrite baseline   | SF=1 / SF=40                                             | Logger captures baseline before `helper.run()`, but **SSTWrite/TX did not drop materially** — writes are happening during the measurement window, not before. See H7. |
| `16e98eb6`   | S5 aCOLI MI                               | SF=1                                                     | 486 records vs S3's 10918 (22× scan reduction); 5-way digest match |

---

## §4 — Future directions (priority order)

### 1. LeanStore-on-Linux run (active)

Rerun S1–S5 at SF=40 / dram=0.1 GiB on the B-tree backend.
Capture `Q3IStats` and the per-stage timing block. Pre-register
expectations under H5 above.

Expected output: a row in §1's table for B-tree at SF=40 dram=0.1.

### 2. Memory-pressure sweep on RocksDB

Vary `dram` across {0.05, 0.1, 0.5, 1.0, full} GiB at SF=40 to chart the
cache-resident → disk-bound transition. The merged-index advantage *should*
appear at the disk-bound end. If it doesn't, H3 (full-MI walk dominates) is
fundamental rather than tunable.

### 3. Microbenchmark variant dispatch (H4)

Replace `std::visit` with a templated branchless dispatch behind a
constexpr flag. Re-measure S3 at SF=40 dram=0.1.

### 4. S3 vs S5 size / scan / parity matrix

S5 collapses scan 22× by baking parameters at load time; S3 keeps
parameters flexible. Quantify the cost of parameter flexibility on a
merged index. This is the reviewer-facing version of "where does the
merged index sit between raw co-location and full materialisation"
(REVIEWS.md §1.1 / §1.2 spectrum).

### 5. Pin down SSTWrite anomaly (H7)

Run the four tests under H7 in order — SST file count delta around
`helper.run()`; switch to `OptimisticTransactionDB` or strip
`txn->Commit()`; pause background compaction during the measurement
window; bypass the histogram with `setperf_level(kEnableTimeAndCPUTimeExceptForMutex)`
and inspect raw IOSTATS counters. The goal is to attribute every reported
SSTWrite µs to a concrete source so the column becomes informative again.

### 6. Bytes-read / SST-block profiling (H3 follow-up)

Count distinct SST blocks read per query for each path. Prediction: S3
reads ~the entire MI's blocks while S1/S4 read sparser slices. Confirming
this would localise the regression to LSM block-cache pressure rather than
walker logic.

---

## §5 — Reviewer relevance

REVIEWS.md §4.2 (R3-W2 / R3-D3-5) explicitly asks for substantive evidence
that merged indexes outperform traditional joins for medium-to-large scans.
Q3I-on-RocksDB at SF=40 does **not** currently provide that evidence —
S3 is ~15% behind S1/S4. The LeanStore run, the memory-pressure sweep,
and the S3-vs-S5 matrix together form the response material. This document
is the place to assemble it.
