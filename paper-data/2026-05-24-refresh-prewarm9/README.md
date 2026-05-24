# 2026-05-24-refresh-prewarm9 — refresh_sales RF1/RF2 IN-MEMORY (DRAM=9 GiB), DBToaster comparison

**Status: numbers recorded, paper framing deferred.** This run measures
refresh_sales update maintenance with LeanStore given enough memory to hold the
entire image resident, as an apples-to-apples point against the in-memory
DBToaster IVM baseline (`../2026-05-24-dbtoaster`). The interpretation/figure
framing is intentionally left open (decided 2026-05-24).

## Setup

- btree only (`--prewarm` is LeanStore-specific; RocksDB has no equivalent).
- SF=1550 (~5 GiB secondary, the same working set as DBToaster's SF=0.36).
- `--dram_gib=9` (> the 8.42 GiB btree image) + `--prewarm=true`: the whole DB
  is scanned into the buffer pool *before* the timed loop (untimed), so the
  RF1/RF2 loop runs fully in-memory with no SSD reads.
- `--update_size=1` (eager), `--refresh_seconds=90`, S1–S4, both Q3+Q5 views
  maintained per order under S2.
- Recovered from a per-structure COPY of the existing tpch family image
  (`/mnt/ssd/tpch_btree/1550.image`); canonical read image untouched, no reload.
- commit `23160335`, host `c220g2-011011.wisc.cloudlab.us`.

**In-memory confirmed:** S4 base reached 54,561 pair/s here vs 5,417 pair/s on
the SSD-spilling 5L run (`../2026-05-24-refresh-5L-ssd`) — ~10× faster once SSD
I/O is removed, so the buffer pool genuinely held the footprint.

## Metric

The headline unit is the **size-stable refresh pair** (one pair = insert 1
order via RF1 + delete 1 order via RF2, each order = 1 order + its 1–7
lineitems, both views maintained) — the TPC-H-faithful unit, the same one the
5L runs report (`pair_tps`). LeanStore measures pairs directly (the loop
interleaves RF1+RF2 each iteration); DBToaster measured RF1 and RF2 as separate
phases, so its pair rate is `1/(1/rf1 + 1/rf2)` (its harness already prints this
as "pair orders/s (aggregate)" = 2× this, counting both orders). `pairs_per_s_med`
is the per-iteration median (maintenance-only); `pairs_per_s_agg` =
total/elapsed (includes the harness's per-iteration CSV-write overhead). Phase
rates (`rf1_ops_per_s`, `rf2_ops_per_s`) are kept for attribution.
`summary/pair_vs_dbtoaster.csv` (and the per-structure `summary/refresh_prewarm9_throughput.csv`).

## Results — size-stable pair (pairs/s, in-memory)

| engine / structure | pairs/s (med) | pairs/s (agg) | RF1 ops/s | RF2 ops/s |
|---|--:|--:|--:|--:|
| **DBToaster** (in-mem IVM) | **13,792** | 13,792 | 86,253 | 16,417 |
| **S1 split**  | 20,506 | 19,116 | 34,944 | 49,650 |
| **S2 view**   | 10,395 |  9,773 | 17,802 | 24,978 |
| **S3 merged** | 18,507 | 17,109 | 31,147 | 46,354 |
| **S4 base**   | 27,280 | 24,771 | 43,842 | 73,910 |

## Observations (factual; framing deferred)

1. **On the pair, LeanStore S3/S1 beat DBToaster; only S2 trails it.** Merged
   **S3 (18.5k) and split S1 (20.5k) pairs/s > DBToaster (13.8k)**; view **S2
   (10.4k) < DBToaster**; base S4 (27.3k) is the ceiling. DBToaster's very fast
   inserts (86k) are offset by slow deletes (16k), so on the size-stable pair it
   lands below the merged/split index. (An RF1-insert-only comparison would
   instead favour DBToaster — hence the pair is the honest unit.)
2. **Within LeanStore:** merged **S3 ≈ split S1 ≫ view S2** on the pair, matching
   the SSD 5L ordering (S3 1656 ≳ S1 1382 ≫ S2 802 pair/s). S3 maintains at
   split-index levels; the view pays the most. (In-memory, split edges merged
   slightly — no scattered-access penalty when everything is resident; the two
   are within ~10%.)
3. **Attribution:** DBToaster wins RF1 inserts (86k vs S3 31k — generated delta
   code, no TX/WAL/MVCC/buffer-manager); LeanStore wins RF2 deletes (S3 46k vs
   16k — DBToaster's `on_delete` triggers are costly). They partially cancel on
   the pair, net favouring the merged/split index.
4. **Memory:** DBToaster needs peak RSS 8.25 GiB (≈1.7× the ~4.9 GiB working
   set) and OOMs below its footprint (no spill). LeanStore maintains the same
   Q3+Q5 views at **0.4–1.0 GiB** DRAM by spilling to SSD
   (`../2026-05-24-refresh-5L-ssd`). This run shows the in-memory ceiling; the
   memory-efficiency contrast lives in the 5L comparison.

## Caveats

- `update_size=1` charges one full LeanStore TX (startTX/commitTX, WAL append,
  MVCC version) per order; DBToaster has no transaction unit. A batched-TX
  variant would narrow the RF1 gap but is not measured here.
- DBToaster runs RF1 then RF2 as separate phases; LeanStore interleaves them
  per iteration. The per-phase medians are still directly comparable; only the
  `*_agg` column mixes the two.
- `DECIMAL→double` (DBToaster) vs `Numeric` (LeanStore); dbgen `-U` orderkeys vs
  LeanStore's sparse `orderkey_from_index` grid — both honor the disjoint,
  size-stable-pair invariant (see the DBToaster README caveats).

## LSM addendum (2026-05-24) — prewarm BACKFIRES; LSM has no in-memory speedup

The prewarm9 idea does **not** transfer to RocksDB. Tried on the SSD (sweep
paused), SF=3850, S1–S4 — see `summary/lsm_9gib_refresh.csv`:

| LSM structure | DRAM=9 (no prewarm) | 5L: DRAM=1.0 |
|---|--:|--:|
| S1 split  | 780.8 | 1,144 |
| S2 view   | **472.6** | 922 |
| S3 merged | 606.1 | 1,173 |
| S4 base   | 672.1 | 1,309 |

- **`--prewarm` cripples RocksDB**: S1 measured **59.6 pairs/s WITH prewarm vs
  780 WITHOUT** (~13×). The block cache has `strict_capacity_limit=true` and is
  charged all memory; prewarm fills it (0.8·dram = 7.2 GiB) to the hard limit and
  starves the RF write path. Prewarm is a LeanStore buffer-pool concept — it has
  no useful RocksDB analogue. (Initially mistaken for HDD-bound: HDD and SSD gave
  *identical* 60 pairs/s, proving it was never disk — it was the cache config.)
- **More memory hurts**: even without prewarm, DRAM=9 is ~0.5× the 5L DRAM=1.0
  rate across all structures. LSM refresh is **write/compaction-bound**, so extra
  block cache doesn't help (reads aren't the bottleneck) and the larger
  memtable/cache budget degrades throughput. **LSM has no in-memory speedup for
  refresh** — unlike btree (≈10× faster prewarmed) and unlike DBToaster (33k
  pairs/s in-memory but requires full residency). The headline LSM number stays
  the **5L (DRAM 1.0)** run; only **S2-view-is-slowest** survives cleanly at
  DRAM=9 (the S1/S3/S4 spread is within RocksDB's run-to-run compaction noise).

## Files

- `raw/btree.s{1..4}.csv` — per-iteration `elapsed_s,rf1,rf2,pair` orders/s.
- `raw/btree_s{1..4}.out` — stdout incl. `prewarm(SN): scanned … rows` + totals.
- `summary/refresh_prewarm9_throughput.csv` — one row per structure (median +
  aggregate phase rates); carries a `disk=ssd` column (image medium; loop in-mem).
- `summary/pair_vs_dbtoaster.csv` — the size-stable **pairs/s** comparison
  (4 LeanStore structures + DBToaster), the headline table above.

## Reproduce

```bash
bash build/scratch/run_refresh_prewarm9_ssd.sh        # btree S1-S4, prewarm, DRAM=9
bash build/scratch/summarize_refresh_prewarm9_ssd.sh  # rebuilds raw/ + summary/ (preserves this README)
```
