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

Per-iteration **phase** rate: pure RF1-insert orders/s and pure RF2-delete
orders/s per iteration (one order = 1 order + its 1–7 lineitems, both views
maintained). Reported as the **median** over all iterations — this is exactly
the unit DBToaster reports (RF1/RF2 phase orders/s), so the numbers line up
directly. `rf1_agg_ops` (total/elapsed) is also carried for continuity with the
5L runs. `summary/refresh_prewarm9_throughput.csv`.

## Results (orders/s, in-memory)

| structure | RF1 insert (median) | RF2 delete (median) | pair (median) | RF1 agg |
|---|--:|--:|--:|--:|
| **S1 split**  | 34,944 | 49,650 | 41,011 | 19,116 |
| **S2 view**   | 17,802 | 24,978 | 20,790 |  9,773 |
| **S3 merged** | 31,147 | 46,354 | 37,014 | 17,109 |
| **S4 base**   | 43,842 | 73,910 | 54,561 | 24,771 |
| **DBToaster** (in-memory IVM) | **86,253** | 16,417 | — | — |

## Observations (factual; framing deferred)

1. **Within LeanStore:** the maintenance ordering matches the SSD 5L run —
   merged **S3 (31.1k) ≈ split S1 (34.9k) ≫ view S2 (17.8k)** on RF1; same on
   RF2. S3 maintains at split-index levels and the view pays the most.
2. **vs DBToaster, RF1 inserts:** DBToaster (86,253) is ~2.8× faster than
   LeanStore S3 and ~4.8× faster than S2 — it is generated delta code with no
   transaction layer / WAL / MVCC / buffer manager; LeanStore pays
   general-purpose-engine overhead at `update_size=1` (one TX per order).
3. **vs DBToaster, RF2 deletes:** LeanStore is faster (S3 46.4k, S2 25.0k vs
   DBToaster 16.4k) — DBToaster's `on_delete` triggers are costly.
4. **Memory:** DBToaster needs peak RSS 8.25 GiB (≈1.7× the ~4.9 GiB working
   set) and OOMs below its footprint (no spill). LeanStore maintains the same
   Q3+Q5 views at **0.4–1.0 GiB** DRAM by spilling to SSD
   (`../2026-05-24-refresh-5L-ssd`: btree S2 802, S3 1656 pair/s). This run only
   shows the in-memory ceiling; the memory-efficiency contrast lives in the 5L
   comparison.

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

## Files

- `raw/btree.s{1..4}.csv` — per-iteration `elapsed_s,rf1,rf2,pair` orders/s.
- `raw/btree_s{1..4}.out` — stdout incl. `prewarm(SN): scanned … rows` + totals.
- `summary/refresh_prewarm9_throughput.csv` — one row per structure; carries a
  `disk=ssd` column (image medium; the timed loop is in-memory).

## Reproduce

```bash
bash build/scratch/run_refresh_prewarm9_ssd.sh        # btree S1-S4, prewarm, DRAM=9
bash build/scratch/summarize_refresh_prewarm9_ssd.sh  # rebuilds raw/ + summary/ (preserves this README)
```
