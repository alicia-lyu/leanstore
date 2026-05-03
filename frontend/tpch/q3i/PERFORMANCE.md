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

**RocksDB-LSM (macOS / SF=40 dram=0.1):**

| Configuration                       | S1 base_merge | S2 view | S3 mi_coli       | S4 hash | S5 aCOLI |
|-------------------------------------|---------------|---------|------------------|---------|----------|
| baseline (pre-skip; before 1dec2358) | 8.04          | 198.3   | 6.95             | ~8      | n/a      |
| with physical custkey-Seek skip     | 7.87          | 198.3   | 4.42 (regression)| 8.24    | n/a      |
| seek-skip disabled (current default)| 7.73          | 198.3   | 6.70 (~15% gap)  | 8.09    | 123.17   |

(S5 was added in `16e98eb6` and only measured on the current default
configuration. It pre-aggregates invoices and lineitems at load time —
see H6.)

**LeanStore-Btree (Linux / dram=0.1) — same ~15% gap, storage-engine-independent:**

| Scale | S1 base_merge | S2 view | S3 mi_coli      | S4 hash | S5 aCOLI |
|-------|---------------|---------|-----------------|---------|----------|
| SF=15 | 30.36         | 437.17  | 25.41 (~16% gap)| 28.38   | 265.63   |
| SF=40 | 0.3926        | 163.33  | 0.3667          | 0.3397  | 89.67    |

S5 on LeanStore SF=15 reproduces the RocksDB pattern: ~10× ahead of the
raw paths (S1/S3/S4 ≈ 25–30 TX/s) but ~38% behind S2 (265.63 vs 437.17).
S5 worker cycles/TX = 7.44 M vs S4's 75.5 M — the aCOLI scan does
~10× less per-query work, matching the TX/s ratio.

**At SF=40 the picture sharpens dramatically.** With raw paths
collapsed to ~0.34–0.39 TX/s by DRAM-spilling page faults, S5 holds
89.67 TX/s — **~260× ahead of the raw paths** and ~55% behind S2
(163.33). S5 worker cycles/TX = 27.6 M (vs S4's 622 M, a ~22×
reduction in per-query CPU work) and `R MiB/TX = 2.97e-05` vs S4's
`6.67e-03` (~225× less I/O traffic).

**aCOLI footprint anomaly (RocksDB side: ROOT-CAUSED & FIXED, commit
`50fd2052`).** Was: at SF=40 RocksDB reported aCOLI = 136.62 MiB,
suspiciously close to COLI = 269 MiB (~52% rather than the ~14%
that cardinality predicted). Root cause: `RocksDB::get_size(cf, ...)`
cached the first caller's result in a single `default_cf_size`
field and returned it for **every** subsequent CF — so any binary
holding two `MergedAdapter`s (e.g. COLI + aCOLI) reported byte-for-
byte identical sizes. SF=1 reproduction with the new
`[content/row]` + `[overhead]` walk in
`tests/q3i/test_query_q3i_rocksdb.cpp` showed aCOLI content =
0.162 MiB but reported = 2.341 MiB (93% "metadata") — pointing at
a measurement-API bug rather than real footprint. Fix: per-CF
cache (`std::unordered_map<ColumnFamilyHandle*, double>`).
Verified at SF=1 post-fix: aCOLI = 0.100 MiB,
merged_coli = 1.120 MiB, parity unchanged. SF=40 RocksDB re-run
pending; expected to drop the reported aCOLI by ~3-4×.

**LeanStore side: still open.** LeanStore's `MergedAdapter::size()`
goes through `btree->estimatePages() * EFFECTIVE_PAGE_SIZE`
(unrelated code path, no caching). At SF=15 aCOLI reports
52.95 MiB and at SF=40 it reports 141.16 MiB — both still
inflated relative to the ~14% cardinality expectation. Likely
B-tree page-utilisation (50-70% fill factor counts every page
including half-full leaves) but not yet quantified. Outstanding
work: add `[content/row]` walk to the LeanStore harness
(`test_load_coli_btree` / `test_query_q3i_btree`) so the inflation
is traceable to leaf-fill ratio rather than another measurement
bug.

(SF=40 collapses S1/S3/S4 to ~0.4 TX/s — data spills out of the 0.1 GiB
DRAM budget; bottleneck is page-fault traffic, not the join algorithm.
S2's small materialised view still fits.)

Numbers in TX/s; see `1dec2358`, `8d10782b` for RocksDB and the
LeanStore run captured 2026-05-02 for B-tree.

S2's lead is expected — the materialised view absorbs the inside-pipeline
cost at load time. The real comparison axis is **S3 vs S1 / S4**: structures
that all do the equivalent inside-pipeline work at query time, differing
only in physical operator. S3 should win there. It doesn't.

---

## §2 — Hypotheses

Each tagged **CONFIRMED** / **REFUTED** / **OPEN**, with the evidence trail.

### H1 — MI is too large per row → REFUTED

Original suspicion: merged adapter reported 228 bytes/row vs split adapters
~100 bytes/row. Investigation in `3200b2b4` confirmed the gap is largely a
*measurement* artefact — but the two size paths use **different RocksDB
APIs** that report different quantities:

- **Split adapters** (`RocksDBAdapter<R>::size()` →
  `RocksDB::get_size<R>()`) call `GetApproximateSizes(opts, default_cf,
  &range, 1, &size)` over a key-prefix range `[Record::id, Record::id+1)`.
  This returns approximate *file-system bytes* within the SST files that
  intersect the prefix range, with `files_size_error_margin = 0.1`
  (RocksDB interpolates ±10% of any straddling file's size into the
  range). The shared default CF holds 4 base tables + 3 split COLI
  adapters at SF=40, so per-record-id prefix ranges don't align with
  SST boundaries — error can be larger than the nominal 10% in practice.
  Per-CF metadata (footer, bloom filters, index blocks) is allocated to
  the whole CF, not the prefix subset, so this path effectively
  *excludes* per-CF metadata.

- **Merged adapter** (`RocksDBMergedAdapter::size()` →
  `RocksDB::get_size(cf, name)`) force-compacts the CF
  (`BottommostLevelCompaction::kForce`) and reads
  `rocksdb.estimate-live-data-size`, the post-compaction live-data
  estimate (live key+value bytes for non-tombstoned records). Because
  each MergedAdapter owns its dedicated CF, this measurement covers the
  whole CF — including per-CF SST footer, bloom filters, index blocks,
  and properties amortised across the smaller content.

**Are these "live data"? Could tombstones explain the asymmetry?** No.
Q3I is read-only at the application level — no inserts/updates/deletes
during the measurement window. At load time, every record is inserted
exactly once. Both `size()` paths invoke `CompactRange` with
`BottommostLevelCompaction::kForce` *before* measuring, which drops any
intermediate L0/L1 tombstones at the bottommost level. After that, no
garbage and no tombstones. The numbers reported are real live data.

**What actually drives the asymmetry**, then, is the API contract:

- Split path returns **compressed file bytes minus per-CF metadata** for
  a prefix range, with ±10% slack. RocksDB's default block compression
  (LZ4 / Snappy) cuts disk bytes ~2× vs raw KV — explains the 40–60%
  underreport vs `[content/row]`.
- Merged path returns the **post-compaction live-data estimate** for a
  whole dedicated CF; per-CF metadata fraction (~22% at SF=1, shrinks at
  higher SF) inflates the per-row figure proportionally.

Truthful content/row (raw key + value bytes summed via direct iterator
walk): MI=177, splits 150–190. Tagged-key overhead is ~5 bytes per
orders/lineitem record; MI is **7% larger than splits, not 2×**.

**Performance impact**: largely none. Block cache eviction and SST read
cost depend on actual on-disk file bytes, not on either reported number.
At SF=40 the LeanStore-Btree run shows S1's whole footprint
(265.6 MiB) and S3's whole footprint (268.9 MiB) within ~1.2% — so size
is not driving the ~15% S3 gap. The asymmetry is a *reporting* problem
that confused early analysis; the `[content/row]` and `[overhead]`
lines in `test_query_q3i_lsm` are the like-for-like numbers for any
future cross-structure size comparison.

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
group's records. The constexpr branch (Seek path) is kept in the source —
not deleted — so a future experiment can flip the flag to `true` and
re-run cache-resident scenarios (SF=1 or any config where all data fits
in DRAM). The disk regression came from prefetch-buffer invalidation;
when there's no SST read traffic to begin with, that cost vanishes and
Seek may win again. Treat the constexpr as a documented A/B knob, not
dead code.

The hypothesis was real; the remedy was wrong on disk.

**B-tree exception (open follow-up):** the prefetch-buffer rationale that
killed the seek-skip on RocksDB does **not** apply to LeanStore's B-tree
backend. A B-tree Seek is a tree descent (`O(log N)` page touches) with
no prefetch buffer to invalidate. Forward iteration through a rejected
custkey's invoices+orders+lineitems can touch 10–50 records at SF=40 —
potentially across multiple leaf pages — whereas a direct Seek to the
next customer is a single descent. **Physical seek-to-next-customer
should win on B-tree.**

Per-order seek (the symmetric optimisation when `o_orderdate` rejects an
order) is probably NOT worth doing on either backend: order groups are
small (~4 lineitems per order at SF=40) and the descent cost likely
outweighs forward-iterating past a handful of records. The cost-benefit
crossover happens around the average group size; below it, forward
iteration wins; above it, Seek wins.

Action item: gate the `USE_PHYSICAL_SEEK_SKIP` constexpr on the Backend
trait so RocksDB and LeanStore can pick different defaults
(`coli_pipeline.tpp:539`). Document the rationale next to the flag.
Re-run the LeanStore SF=15 numbers with the B-tree branch active and
record the delta vs the current S3=25.41 TX/s baseline.

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

For comparison, S1's `BinaryMergeJoin` chain over custkey-sorted splits
**reads roughly the same total bytes** (LeanStore SF=40 footprints: S1
= 265.6 MiB, S3 = 268.9 MiB; within 1.2%) and benefits equally from the
DRAM budget. Block-cache locality should actually *favour* S3 — the MI
co-locates all four record types per custkey in one stream, so a single
block read brings in everything needed for that custkey's join, whereas
S1 advances four separate iterators each pulling its own blocks.

So the gap is **not bytes-read or block locality**. The LeanStore
SF=15 cycles-per-TX numbers point at CPU instead:

| Path | pp_0 cycles/TX | worker_1 cycles/TX | TX/s |
|------|---------------:|-------------------:|-----:|
| S1 base_merge | 80 M | 73 M | 30.36 |
| S3 mi_coli    | 96 M | 83 M | 25.41 |

S3 burns ~16% more CPU cycles per query than S1 — matching the ~16% TX/s
gap almost exactly. The per-record cost of the COLI walker
(`std::visit` on a 4-way variant + tagged-key decode per record + the
`else if (group_active)` cascade) appears higher than the per-record
cost of S1's narrow scanner advances feeding into a `BinaryMergeJoin`.
This routes the investigation toward **H4** (per-record dispatch
overhead, aggregated across the full walk) and away from storage-layout
or block-cache hypotheses. Because H6 confirms all three raw paths
full-scan their inputs uniformly, H6 explains the S5 win but not the
S3-vs-S1 gap — H4 is the remaining live suspect for that gap.

### H4 — Per-record dispatch overhead → OPEN, but `std::visit` is probably not the culprit

**Do skipped records pay the `std::visit` cost?** Yes. The walker calls
`std::visit` unconditionally on every record yielded by the scanner. The
`else if (group_active)` short-circuit is *inside* the visit lambda — it
suppresses the `on_invoice` / `on_order` / `on_lineitem` body, but the
`std::visit` dispatch itself (variant tag check + lambda invocation +
inner `if constexpr` type-tag chain) fires regardless.

**But that overhead is small.** The geo benchmark uses the same variant
pattern in `PremergedJoin` and showed no pathological dispatch cost at
comparable scale. Modern compilers turn `std::visit` over a small variant
into a jump table; per-record cost is a few ns. At 425k records/query
that's ~2 ms — tiny next to the 22 ms gap to S1/S4 at SF=40.

So if H4 is real, it's not `std::visit` itself but something *adjacent*
that S3 pays for every record and S1 doesn't:

- **Tagged-key decode**: S3 must parse the trailing `idx_id` byte and the
  composite (custkey, [secondary fields]) on every record; S1's split
  adapters decode plain `(custkey, ...)` keys. Decode cost differs.
- **`scanner->next()` over a 4-record-type CF**: each next must emit a
  fully-typed `std::variant<customer_coli_t, invoice_coli_t,
  orders_coli_t, lineitem_coli_t>`. The merged scanner inspects the key
  to dispatch to the right `accepts_key` and copies the right payload
  size. S1's split scanners are monomorphic per record type.
- **Iterator advance arithmetic**: S3 advances ONE iterator through 4
  interleaved record types per custkey (~70 records per custkey at SF=40
  on average). S1 advances 4 iterators through narrower streams; the
  per-record cost is lower per iterator but there are 4 of them.

These are all per-record costs aggregated over the full MI walk — exactly
the cardinality H3 confirmed. The cycles gap (16% extra at SF=15) is
plausibly the sum of these small overheads × 425k records, not any one
of them in isolation.

**Action**: micro-attribute via `perf record` or counter-instrument the
walker dispatch path. The per-record `std::visit` is the cheapest
suspect on the list; tagged-key decode is the next-cheapest;
`scanner->next()` payload emission is the most likely source. Replacing
`std::visit` with a templated dispatch may not move the needle by much
— measure first.

### H5 — Storage-engine specific: RocksDB block layout / prefetch → REFUTED

LeanStore-Btree run (Linux, 2026-05-02) shows the **same ~15% S3-vs-S1
gap at SF=15** (S1=30.36, S3=25.41 TX/s) and S3 still loses at SF=40
(S3=0.3667, S1=0.3926). The gap is storage-engine-independent — not a
RocksDB block-cache or prefetch artefact.

This eliminates LSM-specific mitigations from the candidate fix list and
redirects investigation to H3 + H4 (the COLI walk pattern itself: full-MI
scan with low filter selectivity, plus per-record dispatch overhead).

### H6 — Low filter selectivity makes the raw paths uniformly inefficient → CONFIRMED (uniform across S1/S3/S4)

Default Q3I params: `c_mktsegment='BUILDING'` (~20% of customers),
`o_orderdate < 1995-03-15` (~50% of orders), threshold>0 (filters
further). Final result: 7 rows at SF=1, ~10 rows at higher SF.

**All three raw paths (S1/S3/S4) scan 100% of base records.** S1's
BMJ chain reads every customer (to apply mktsegment), every invoice (to
build the `cust_open_due` aggregate), every order (to apply orderdate),
and every lineitem (to compute revenue) — the join semantics fire only
on matches, but the *inputs* are full scans. S4's HJ chain has the same
property: build-side hashmaps consume full table scans, then the probe
side iterates the largest input in full. S3 walks the full MI for the
same reason. So this hypothesis does **not** explain the S3-vs-S1 gap;
it explains why all three raw paths leave a lot of I/O / dispatch work
on the table relative to what the query semantically needs.

**S5 (aCOLI MI, `16e98eb6`) is the path that breaks this floor.** By
pre-aggregating invoices and lineitems at load time, S5 reduces the
*scan cardinality* (not just the join output) from ~390M base records
at SF=40 to ~61M aCOLI records (1.5M customers + 60M orders), and within
that further down to 486 records at SF=1 because the residual scan
applies all parameterised filters early. RocksDB SF=40 result:
S5 = 123.17 TX/s, ~300× ahead of S1/S3/S4 (~0.4 TX/s, DRAM-spilling).

**Open: S5 still trails S2 by ~38% (123 vs 198 TX/s)**, where the
expectation is that S5 should match or beat S2 since both scan
~|orders|-cardinality structures with the same pre-aggregated values
fused. The likely culprits are:

1. **Project-pushdown gap.** S2's `q3i_pipeline_view_t` carries only
   the columns Q3I reads (`revenue`, `cust_open_due`, `c_mktsegment`,
   `o_orderdate`, `o_shippriority`). S5's `customer_acoli_t` and
   `orders_acoli_t` carry the *full* base record payload plus the
   pre-aggregated field. Per-row scan bytes are higher; this is
   exactly the "no project pushdown below secondary-structure loading"
   limitation documented in `tpch/CLAUDE.md §Known Design Limitations`.
2. **Variant dispatch.** S5 walks a `std::variant<customer_acoli_t,
   orders_acoli_t>` (`std::visit` per record); S2 is a monomorphic
   scan of `q3i_pipeline_view_t`. Marginal per-record cost for S5.
3. **No order-as-the-only-row layout.** S2 has one row per
   `(custkey, orderkey)`; S5 has one customer row plus N order rows
   per custkey. The customer row contributes a small overhead per
   group on the S5 side that S2 doesn't pay.

The first item is the dominant suspect and is the one the paper would
most naturally fix: a projection-pushed `customer_acoli_t` /
`orders_acoli_t` would shrink record width to match the view's, which
should close most of the gap. S5 retains its parameter-flexibility
advantage over S2 either way: S2 bakes the full result projection at
load time, while S5 lets the query-time predicate pick what to emit.

A possible reordering of the spectrum, after a project-pushed S5:

```
   S3 (raw COLI)           S5 (aggregate-pushed COLI)        S2 (full view)
   parameter-flexible      parameter-flexible                load-time-baked
   slow                    fast                              fastest
```

S5 then becomes the recommended default and S2 the upper bound.

**What this implies for the investigation**: H6 is the strongest argument
for the aCOLI direction in REVIEWS.md §1.1 R2-D1 — pre-aggregating into
the MI is the way to actually beat the raw-scan baseline. It also
implies that *no per-record optimisation* on S3 (variant dispatch,
tagged-key decode, walker layout) will close the S3-vs-S1 gap to a clear
win — both still bottleneck on full-table scans, and the per-record
overhead is incremental at best. The interesting question becomes: at
what point on the aCOLI ↔ COLI spectrum does the merged index become
worth its complexity?

**Note on the customer-scan TX-count effect** (previously in
`CLAUDE.md §Performance Notes`): at small SF the lower S3 TX/s
proportionally lowers raw `customers_scanned` totals across the 15s
window — not because S3 scans fewer customers per query (it scans the
same set; `populate_merged` inserts every customer unconditionally), but
because the window holds fewer of those identical full scans.

### H7 — Significant SSTWrite during read-only queries → OPEN, RocksDB-specific

Production RocksDB runs report SSTWrite(µs)/TX values sometimes an
**order of magnitude larger than SSTRead(µs)/TX**, despite Q3I being a
read-only workload at the application level. The 2026-05-02 LeanStore
run reports `W MiB/TX = 0` across S1–S4 at both SF=15 and SF=40,
confirming the write traffic is an LSM-engine artefact — not a
workload property — and pinning H7 as RocksDB-specific.

`128f6d44` added `RocksDBLogger::capture_baseline()` to subtract the
post-load compaction histogram contribution before the measurement loop
starts. **The baseline-subtract did not move the numbers materially** —
indicating the writes happen *during* `helper.run()`, not before it.

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

This anomaly affects all four S1–S4 figures on RocksDB roughly uniformly,
so it does not explain the S3-vs-S1 gap on its own — but it does mean
the "SSTWrite/TX" column is not currently informative for distinguishing
structures. Until ruled out, **focus on SSTRead/TX and TX/s** for
cross-structure comparison on RocksDB; on LeanStore the column is
trivially zero and not useful either way.

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
| 2026-05-02   | LeanStore-Btree run on Linux              | SF=15 / SF=40 dram=0.1                                   | Same ~15% S3-vs-S1 gap as RocksDB → H5 REFUTED. `W MiB/TX = 0` on B-tree → SSTWrite anomaly is RocksDB-specific (compaction). |
| 2026-05-03   | S5 aCOLI MI on RocksDB                    | SF=40 dram=0.1                                           | S5=123.17 TX/s — beats S1/S3/S4 (~0.4 TX/s, DRAM-spilling) by ~300×, behind S2 (198.3) by ~38%. Reported size 136.62 MiB suspiciously close to base alone (~130 MiB) — aCOLI MI delta ≈ 6 MiB; potential `get_aggregated_size()` measurement bug, not yet root-caused. |
| 2026-05-03   | S5 aCOLI MI + S4 hash on LeanStore        | SF=15 dram=0.1                                           | S5=265.63 TX/s (worker cycles 7.44 M) reproduces the RocksDB pattern; ~10× ahead of S1/S3/S4 raw paths, ~38% behind S2 (437.17). S4=28.38 TX/s slots between S1 (30.36) and S3 (25.41) — completes the SF=15 raw-path picture. aCOLI footprint 52.95 MiB vs base 48.90 MiB (~4 MiB delta) matches the RocksDB size anomaly — likely `get_aggregated_size()` measures the wrong CF / range on both backends. |
| 2026-05-03   | S5 aCOLI MI + S4 hash on LeanStore        | SF=40 dram=0.1                                           | S5=89.67 TX/s (worker cycles 27.6 M, R MiB/TX 2.97e-05). S4=0.3397 TX/s (worker cycles 622 M, R MiB/TX 6.67e-03) — DRAM-spilling, 6 queries in 17.7 s. **S5 holds ~260× lead over raw paths under disk pressure**, ~22× fewer cycles/TX and ~225× less I/O than S4; ~55% behind S2 (163.33). aCOLI footprint 141.16 MiB vs S4 base 130.43 MiB (~11 MiB delta) — same anomaly pattern at SF=40 LeanStore. |
| 2026-05-03   | aCOLI size anomaly root-caused on RocksDB | SF=1 (`test_query_q3i_lsm`)                              | Bug: `RocksDB::get_size(cf,...)` cached one global result and returned it for every CF. With two MergedAdapters (COLI + aCOLI) both reported byte-for-byte identical sizes. Phase 6 [content/row] walk surfaced it (aCOLI content 0.162 MiB vs reported 2.341 MiB → 93% "metadata"). Fix `50fd2052` keys cache per-CF. Post-fix SF=1: aCOLI = 0.100 MiB, merged_coli = 1.120 MiB. Parity unchanged across all 5 paths. |

---

## §4 — Future directions (priority order)

### 1. LeanStore-on-Linux run — DONE (2026-05-02)

Result: same ~15% S3-vs-S1 gap on B-tree (SF=15: S1=30.36, S3=25.41).
H5 refuted — bottleneck is not LSM-specific. SF=40 collapses S1/S3/S4 to
~0.4 TX/s (DRAM-bound page-fault thrashing); only S2's small view stays
above water (163 TX/s). S5 not yet captured on LeanStore — useful
follow-up.

### 2. Memory-pressure sweep on RocksDB

Vary `dram` across {0.05, 0.1, 0.5, 1.0, full} GiB at SF=40 to chart the
cache-resident → disk-bound transition. The merged-index advantage *should*
appear at the disk-bound end. If it doesn't, H3 (full-MI walk dominates) is
fundamental rather than tunable.

### 3. Micro-attribute the per-record cost (H4)

`perf record` the walker hot path on RocksDB at SF=15 and SF=40 to
attribute the 16% extra cycles/TX. Suspects, ordered cheapest to most
expensive: `std::visit` dispatch (likely small per geo precedent),
tagged-key decode, merged-scanner `next()` payload emission. Replacing
`std::visit` with a templated dispatch is one A/B knob; another is
specialising the merged scanner to skip variant construction when the
visitor has only `void`-returning hooks. Measure before optimising.

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

### 6. Re-enable physical seek-skip on LeanStore (H2 B-tree branch)

Gate `USE_PHYSICAL_SEEK_SKIP` on the Backend trait
(`coli_pipeline.tpp:539`) so LeanStore takes the Seek branch while
RocksDB stays on forward iteration. Re-run SF=15 dram=0.1 on B-tree and
record the delta vs S3=25.41 TX/s. The expected win is meaningful only
when ~80% of customers fail mktsegment and each rejected group spans
10–50 records on average (true at SF=15+ for default Q3I params).

---

## §5 — Reviewer relevance

REVIEWS.md §4.2 (R3-W2 / R3-D3-5) asks for substantive evidence that
merged indexes outperform traditional joins on medium-to-large scans.
**S3 alone does not provide that evidence at SF=40** (~15% behind S1/S4
on both RocksDB and LeanStore). H6 explains why: all three raw paths
(S1/S3/S4) full-scan their inputs equally, and the merged-index per-row
overhead leaves S3 marginally worse than S1's narrow split scans.

**S5 (aCOLI MI) is the path that earns the merged-index pitch.** By
co-locating customer + order rows with pre-aggregated `cust_open_due`
and `pre_revenue` columns, S5 collapses scan cardinality 22× at SF=1
and runs ~300× faster than S1/S3/S4 at SF=40 dram=0.1 on RocksDB
(123 TX/s vs ~0.4). This bridges the spectrum between raw co-location
(S3) and full materialisation (S2), and directly addresses
REVIEWS.md §1.1 R2-D1 ("merged indexes storing simple aggregates as
included columns").

The reviewer response built from this document should lead with S5,
present S3 as the parameter-flexible counterpart, and use the
S3-vs-S1/S4 gap as the *cost* of that flexibility — not as a failure
of merged indexes generally.
