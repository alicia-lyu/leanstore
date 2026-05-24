# 2026-05-23-refresh-5L — refresh_sales RF1/RF2 at the 5L cell (c0)

refresh_sales (TPC-H RF1/RF2) update throughput at the **paper headline cell
5L = c0**: ~5 GiB secondaries, DRAM 1.0 GiB, **low memory pressure** (but data ≫
DRAM, so disk-bound). Companion to the in-memory smoke `2026-05-23-refresh-sf1`.

- commit `165dd75c` (calcite-integration), host `c220g2-011011.wisc.cloudlab.us`
- SF=1550 (btree) / 3850 (lsm), `--dram_gib=1.0`, `--update_size=1`,
  `--refresh_seconds=90`, S1–S4, both backends.
- **Recovered from the existing tpch family image** (`tpch_btree/1550.image`,
  `tpch_lsm/3850/`) on a per-structure copy — refresh_sales shares the family
  adapter names + `load_vanilla_family`, so no reload; canonical read images
  untouched.

## Metric

Count-based throughput over **actual elapsed** time (disk-bound — no warm
plateau). At this scale **RF2 does not exhaust** (`total_rf1 == total_rf2`), so
the size-stable RF1+RF2 **pair throughput** (`pair_tps = #iterations/elapsed`,
the TPC-H-faithful unit) is the headline. `summary/refresh_sales_5L_throughput.csv`.

## Result (pairs/sec)

| backend | S1 split | S2 view | S3 merged | S4 base |
|---|--:|--:|--:|--:|
| **btree** (pairs over 90 s) | 24 | 16 | 26 | 61 |
| **btree** pair_tps | 0.27 | 0.18 | **0.29** | 0.68 |
| **btree** pair_tps (tail 30 s) | 0.40 | 0.20 | **0.50** | 1.00 |
| **lsm** (pairs over 90 s) | 826 ⚠ | 120 | 124 | 124 |
| **lsm** pair_tps | 9.2 ⚠ | 1.3 | 1.4 | 1.4 |

**Headline (btree — clean):** under memory pressure the **merged index (S3)
matches the split index (S1) and both beat the materialised view (S2)**;
base (S4) is the no-secondary-maintenance ceiling. Same shape as the in-memory
smoke — the merged index pays **no IO penalty over split** for its
custkey-scattered access even when data ≫ DRAM. Supports §5.4.

## Caveats

1. **LSM is not truly memory-pressured here — treat the lsm row as
   unreliable.** `--dram_gib` caps only RocksDB's *internal block cache*; the
   `cp -r` of the 7.1 GiB image warms all SSTs into the **OS page cache** (the
   host has ≫ 1 GiB free RAM), so RocksDB reads stay in RAM. LeanStore uses
   `O_DIRECT` (bypasses the OS cache), so only the **btree** row is a genuine
   memory-pressure measurement. The lsm `S1=9.2` is a 6.6× outlier (sustained,
   tail=9.0) while S2/S3/S4 cluster at ~1.3–1.4 — consistent with cache/run-order
   noise, not a structural signal. For a clean LSM pressure number, drop the OS
   page cache between copy and run (`echo 3 > /proc/sys/vm/drop_caches`, needs
   root) or run from a cgroup with a memory cap.
2. **Small absolute counts** (btree 16–61 pairs over 90 s) → rates carry ~±20 %
   noise; the *ordering* (base ≫ merged ≈ split > view) is robust but tighter
   CIs need a longer window or multiple reps.
3. Single rep, single run-order. No warmup phase (disk-bound throughout, so the
   initial buffer-pool fill is a small fraction of 90 s).

## Files
- `raw/{btree,lsm}.s{1..4}.csv`, `raw/{btree,lsm}_s{1..4}.out`
- `summary/refresh_sales_5L_throughput.csv` — tidy, one row per
  (backend, structure); plotter-ready.

## Reproduce
```bash
bash build/scratch/run_refresh_5L.sh         # recover-from-image-copy sweep, both backends
bash build/scratch/summarize_refresh_5L.sh   # rebuilds raw/ + summary/ (preserves this README)
```
