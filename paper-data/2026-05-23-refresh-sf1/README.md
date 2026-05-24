# 2026-05-23-refresh-sf1 — refresh_sales RF1/RF2 update throughput

First Linux run of the `refresh_sales` (TPC-H RF1/RF2) update experiment on
the vanilla Q3/Q5 COL schema. **SF=1 smoke scale** — establishes the pipeline
and the cross-structure shape; see Caveats for why SF≥15 is the paper-final
follow-up.

- commit `bc5b252f` (calcite-integration), host `c220g2-011011.wisc.cloudlab.us`
- both backends (`refresh_sales_btree`, `refresh_sales_lsm`)
- per structure S1–S4, `--update_size=1`, `--refresh_seconds=30`, `--dram_gib=8`
- each structure run on a **fresh copy** of the pre-loaded vanilla-family image
  (read images untouched), recovered via `--recover`.

## Metric

RF1 inserts/sec, **count-based** (iterations ÷ wall-seconds) — not the CSV's
per-op instantaneous-rate column, which is ratio-biased. Two measures in
`summary/refresh_sales_rf_throughput.csv`:

- `rf1_tps_avg` = `total_rf1 / refresh_seconds` — robust, whole-run average
  (includes cold warmup).
- `rf1_tps_hot` = (#iterations in the mid-tail window `[20,25)s`) / 5 — warm
  steady-state. A fixed mid-tail slice (not the final 5 s) so it stays warm for
  every structure yet excludes any trailing write-stall (see caveat 3).

## Result (RF1 inserts/sec)

| backend | S1 split | S2 view | S3 merged | S4 base |
|---|--:|--:|--:|--:|
| **btree** avg | 20090 | 8402 | 14080 | 30377 |
| **btree** hot | 33588 | 22603 | **35735** | 41940 |
| **lsm** avg | 6791 | 2265 | 3526 | 7476 |
| **lsm** hot | 7689 | 4155 | **10184** | 12290 |

**Headline (supports §5.4):** at warm steady state the **merged index (S3)
matches or beats the split index (S1), and both substantially exceed the
materialised view (S2)** — on both backends (btree hot: S3 35.7k ≳ S1 33.6k ≫
S2 22.6k; lsm hot: S3 10.2k > S1 7.7k ≫ S2 4.2k). The merged index maintains
cheaply; the view pays the most. S4 base is the no-secondary-maintenance ceiling.

**Warmup nuance:** in the whole-run `avg`, S3 merged trails S1 split — the
merged index pays a higher *warmup* cost because RF1/RF2 touch custkey-scattered
pages (large cold working set read from the freshly-copied image), whereas the
split index appends near-sequentially. Once hot, per-op merged ≈/> split.

## Caveats (why SF=1 is a smoke, not the paper number)

1. **RF2 reservoir exhausts early.** RF2 deletes pre-existing orders from the
   bottom of the keyspace; the reservoir is `ORDERS_SCALE×SF = 1500` at SF=1, so
   every structure drains it in well under the 30 s window. The hot tail is
   therefore **RF1-only**, and the size-stable RF1+RF2 *pair* throughput (the
   TPC-H-faithful unit) is not measurable at SF=1. **SF≥15** gives a 22500+
   reservoir → measurable steady-state pair / RF2-delete throughput.
2. **No warmup phase in the binary.** Timing starts at op 1, so `avg` is
   warmup-confounded; `hot` (mid-tail window) is the cleaner steady-state proxy.
   A `--warmup_seconds` pre-loop (the read harness already has the flag) would
   let `avg` report steady-state directly.
3. **lsm S4 tail compaction stall** (`elapsed_total=33.19 s`): the base-only
   path writes the most data (no secondary work to throttle it) and its final
   iteration blocked ~8 s on a RocksDB write-stall past the 30 s deadline. The
   `[20,25)s` hot window excludes that single stalled iteration (an earlier
   "last-5s" window mis-sampled it as 0.2). A real LSM signal worth keeping for
   the SF≥15 run.

## Files

- `raw/{btree,lsm}.s{1..4}.csv` — per-iteration RF CSVs
  (`elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s`).
- `raw/{btree,lsm}_s{1..4}.out` — run logs (final RF1/RF2 totals).
- `summary/refresh_sales_rf_throughput.csv` — tidy, one row per
  (backend, structure); plotter-ready (grouped bar: x=structure, series=backend,
  y=`rf1_tps_hot` or `rf1_tps_avg`).

## Reproduce

```bash
bash build/scratch/run_refresh_hot.sh        # runs both backends, S1–S4, on image copies
bash build/scratch/summarize_refresh_hot.sh  # rebuilds raw/ + summary/ (NOTE: wipes the tag dir except this README)
```
(Both scripts live in `build/scratch/`; they reuse the pre-loaded base images
at `/mnt/ssd/refresh_sales_{btree,lsm_smoke}`.)
