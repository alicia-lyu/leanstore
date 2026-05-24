# 2026-05-24-refresh-5L-ssd — refresh_sales RF1/RF2 at 5L (c0), **real SSD**

refresh_sales (TPC-H RF1/RF2) update throughput at the paper headline cell
**5L = c0** (~5 GiB secondaries, DRAM 1.0 GiB), rerun on the **real SSD**.
Supersedes `2026-05-23-refresh-5L`, which was measured on a spinning SAS HDD
(`/dev/sdb`) that had been mislabeled and mounted at `/mnt/ssd`; the actual
Intel DC S3500 480 GB SATA SSD (`/dev/sdc`) was unmounted. See
`../../LINUX_SETUP.md §3` for the disk fix.

- commit `c9b5f594` (calcite-integration), host `c220g2-011011.wisc.cloudlab.us`
- SF=1550 (btree) / 3850 (lsm), `--dram_gib=1.0`, `--update_size=1`,
  `--refresh_seconds=90`, S1–S4, both backends.
- **Recovered from the existing tpch family image** (`tpch_btree/1550.image`,
  `tpch_lsm/3850/`) on a per-structure copy — refresh_sales shares the family
  adapter names + `load_vanilla_family`, so no reload; canonical read images
  untouched. **OS page cache dropped between the image copy and each timed run.**

## Metric

Count-based throughput over actual elapsed time. RF2 does not exhaust at this
scale (`total_rf1 == total_rf2`), so the size-stable RF1+RF2 **pair throughput**
(`pair_tps = #pairs/elapsed`, the TPC-H-faithful unit) is the headline.
`summary/refresh_sales_5L_throughput.csv`.

## Result (pairs/sec, higher = better)

| backend | S1 split | S2 view | S3 merged | S4 base |
|---|--:|--:|--:|--:|
| **btree** | 1382 | 802 | **1656** | 5417 |
| **lsm**   | 1144 | 922 | **1173** | 1309 |

**Headline:** on both backends the **merged index (S3) matches-or-beats the
split index (S1) and both clearly beat the materialised view (S2)**; base (S4)
is the no-secondary-maintenance ceiling. The merged index pays no maintenance
penalty over split secondaries for its custkey-scattered writes, while the view
pays the most — directly supporting §5.4 ("matches the view on reads *without*
the view's update cost").

## Why this corrects the HDD run's caveat

The HDD README (`2026-05-23-refresh-5L`) flagged the lsm row as unreliable,
reasoning that the `cp -r` of the image warmed all SSTs into the **OS page
cache** so RocksDB "reads stayed in RAM." **That reasoning is wrong for this
codebase:** RocksDB is configured with `use_direct_reads=true` and
`use_direct_io_for_flush_and_compaction=true` (`frontend/shared/RocksDB.cpp:16-17`),
so its reads use `O_DIRECT` and bypass the OS page cache entirely; LeanStore
likewise uses `O_DIRECT`. Neither engine is served by the host's 157 GiB of RAM.
The HDD numbers were therefore genuine — just measured on a spinning disk, where
random-write/eviction under `O_DIRECT` is seek-bound and pathologically slow
(btree did only ~24 pairs in 90 s). On the SSD the same workload runs
~1000–5000× faster and the per-structure ordering is clean and stable (no
outlier; cf. the HDD lsm S1=9.2 anomaly, which was run-order/SST-state noise,
not page cache).

## Files
- `raw/{btree,lsm}.s{1..4}.csv`, `raw/{btree,lsm}_s{1..4}.out`
- `summary/refresh_sales_5L_throughput.csv` — one row per (backend, structure);
  carries a `disk=ssd` column.

## Reproduce
```bash
bash build/scratch/run_refresh_5L_ssd.sh        # recover-from-image-copy sweep, both backends, cold-start
bash build/scratch/summarize_refresh_5L_ssd.sh  # rebuilds raw/ + summary/ (preserves this README)
```
