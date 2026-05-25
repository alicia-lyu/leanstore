# refresh_sales — Linux perf-run ledger

Append one entry per `refresh_sales_{lsm,btree}` sweep that produces a
`RefreshTPut.s<N>.csv`. Format: date+time, commit SHA + branch, path to the
CSV (or why it wasn't produced), config (SF / DRAM / structure(s) / host /
`--update_size` / `--run_for_seconds`), and **one sentence** stating whether
the run supports the §5.4 paper claim (merged-index maintenance ≈
traditional-index, view costs more) and how. Correctness-only runs (no perf
numbers) still get an entry — say so explicitly. Don't backfill old runs.

Convention mirrors `frontend/tpch/q3/RUNS.md` /
`frontend/geo/RUNS.md`. Terse: ledger, not writeup.

## Entries

- **2026-05-24 — DBToaster IVM baseline (new `dbtoaster/`)** — branch
  `calcite-integration`, this Linux node. DBToaster 2.3 maintains the **Q3 + Q5
  pipeline views** (unaggregated C×O×L and C×O×L×NATION joins, one row per
  lineitem) under RF1/RF2 from dbgen `-U`, **SF=0.36** (working set ≈ 5 GiB —
  the paper 5-GiB-secondary anchor; SF pinned empirically: ~140 MB@0.01,
  1.41 GiB@0.1). **Unlimited memory, no `ulimit` sweep** (DBToaster is pure
  in-memory — no spill — so a cap below the working set OOMs; the metric is the
  footprint, not a pressure curve). CSV:
  `dbtoaster/results/RefreshTPut.dbtoaster.csv`. Correctness: both views =
  2,160,142 rows = base 2,160,128 + RF1 (2,157) − RF2 (2,143) — size-stable
  delta holds. **Supports §5.x memory-requirement gap**: DBToaster maintains
  both views at RF1 ≈ 86k / RF2 ≈ 16k orders/s but needs **4.9 GiB resident
  (working set) / 8.25 GiB peak** — whereas LeanStore S2 serves a 5 GiB
  secondary from a 0.4–1.0 GiB buffer pool by spilling to SSD. Peak ≈ 1.7× the
  working set here; the precise RAM multiple under sustained RF is a later
  characterization. Setup in `LINUX_SETUP.md §Step 6`.
- **2026-05-23 (later) — 5L cell (c0)** — commit `165dd75c`
  (calcite-integration), host `c220g2-011011.wisc.cloudlab.us`. Both backends,
  S1–S4, SF=1550 (btree) / 3850 (lsm), DRAM=1.0 GiB, `--update_size=1`,
  `--refresh_seconds=90`, **recovered from the existing tpch family image on
  per-structure copies** (shared adapter names; read images untouched; no
  reload). CSVs + summary: `paper-data/2026-05-23-refresh-5L/`. RF2 does not
  exhaust here, so size-stable **pair throughput** is the metric.
  **Supports §5.4 (btree)**: under memory pressure merged ≈ split, both > view,
  base = ceiling (pair/s tail: base 1.0, merged 0.5, split 0.4, view 0.2) — the
  merged index pays no IO penalty over split for scattered access even when data
  ≫ DRAM. **lsm row unreliable**: `--dram_gib` caps only RocksDB's block cache
  while `cp -r` warms all SSTs into the OS page cache (LeanStore uses O_DIRECT,
  so only btree is truly pressured); lsm S1=9.2 pair/s is a cache/run-order
  outlier vs S2/S3/S4 ~1.3–1.4. Clean LSM pressure needs drop_caches (root) or a
  cgroup memory cap.
- **2026-05-23 23:18 CDT** — commit `bc5b252f` (calcite-integration), host
  `c220g2-011011.wisc.cloudlab.us`. First Linux run, both backends, S1–S4,
  SF=1, DRAM=8 GiB, `--update_size=1`, `--refresh_seconds=30`, on fresh copies
  of the loaded vanilla-family image. CSVs +
  summary: `paper-data/2026-05-23-refresh-sf1/` (`summary/refresh_sales_rf_throughput.csv`).
  **Supports §5.4**: at warm steady state merged-index RF1 maintenance
  matches/beats the split index and both ≫ the materialised view (btree hot
  S3 35.7k ≳ S1 33.6k ≫ S2 22.6k; lsm hot S3 10.2k > S1 7.7k ≫ S2 4.2k
  inserts/s) — view pays the most. Caveats: SF=1 smoke; RF2 reservoir (1500)
  exhausts early so pair/RF2 steady-state is not measurable here (needs SF≥15);
  whole-run `avg` is warmup-confounded (merged warms slower via scattered
  access); lsm S4 hit a tail compaction stall. Required a one-line fix first:
  `recover_last_ids()` was crashing on btree (called off-Worker) — now wrapped
  in `scheduleJobSync`.

## 2026-05-24 — 5L (c0) RF1/RF2 on the **real SSD** (tag `2026-05-24-refresh-5L-ssd`)

- commit `c9b5f594` (calcite-integration), host `c220g2-011011`; `build/scratch/run_refresh_5L_ssd.sh` (recover-from-image-copy, OS page cache dropped per run), SF=1550 btree / 3850 lsm, DRAM 1.0, `--refresh_seconds=90`, `--update_size=1`, S1–S4 both backends. Summary: `paper-data/2026-05-24-refresh-5L-ssd/` (`disk=ssd`).
- **Supports §5.4 cleanly**: pair throughput **S3 ≥ S1 > S2** on both backends (btree S3 1656 ≳ S1 1382 ≫ S2 802; lsm S3 1173 ≳ S1 1144 ≫ S2 922 pairs/s), S4 base the no-maintenance ceiling. Supersedes the noisy HDD run `2026-05-23-refresh-5L` (btree ~24 pairs/90s, seek-bound; SSD ~10³× faster, no lsm outlier). **Corrects** that run's "lsm served from OS page cache" caveat — RocksDB sets `use_direct_reads=true` (RocksDB.cpp:16), so reads bypass the page cache and the HDD numbers were genuine, just seek-bound.

## 2026-05-24 — IN-MEMORY (prewarm, DRAM=9) DBToaster comparison (tag `2026-05-24-refresh-prewarm9`)

- commit `23160335` (calcite-integration), host `c220g2-011011`; `build/scratch/run_refresh_prewarm9_ssd.sh`, btree only (`--prewarm` is LeanStore-specific), SF=1550 (~5 GiB secondary = DBToaster SF=0.36), `--dram_gib=9` + `--prewarm=true` (whole 8.42 GiB image scanned into the pool, untimed → loop runs fully in-memory), `--update_size=1`, `--refresh_seconds=90`, S1–S4. Summary: `paper-data/2026-05-24-refresh-prewarm9/` (`disk=ssd`; loop in-memory). **Numbers recorded, paper framing deferred.**
- **Result — size-stable PAIRS/s (TPC-H unit, = 5L `pair_tps`):** S1 20.5k, **S3 18.5k**, S2 10.4k, S4 27.3k (btree, `summary/pair_vs_dbtoaster.csv`). Within LeanStore the §5.4 ordering holds in-memory (**S3 ≈ S1 ≫ S2**; split edges merged slightly with everything resident). Caveat: `update_size=1` = one TX/order.
- **CORRECTION (DBToaster pair, measured interleaved):** the earlier 13.8k DBToaster pair was *derived* from separate bulk phases and *understated* it. Re-ran with `RF_INTERLEAVE=1` (typed `on_insert/on_delete`, 1+1 loop like LeanStore) → **33.2k pairs/s** (2 runs, <1% var; `paper-data/2026-05-24-dbtoaster/summary/refresh_sales_dbtoaster_interleaved.csv`). So **DBToaster (33.2k) actually beats LeanStore in-memory btree** (S1 20.5k/S3 18.5k) — the earlier "LeanStore beats DBToaster on the pair" read is REVERSED. LeanStore's edge is **memory** (runs the same views at 0.4–1.0 GiB by spilling; DBToaster needs ~8 GiB resident, OOMs below working set), not raw in-memory throughput.
- **LSM addendum — prewarm BACKFIRES; no in-memory speedup** (`build/scratch/run_refresh_lsm9_ssd.sh`, SF=3850, SSD, sweep paused; `summary/lsm_9gib_refresh.csv`): `--prewarm` fills RocksDB's strict-capacity block cache and starves the write path → S1 **59.6 pairs/s WITH prewarm vs 780 WITHOUT** (~13×; HDD==SSD identical 60 proved it was the cache config, never disk-bound). Even no-prewarm, DRAM=9 (S1 781/S2 473/S3 606/S4 672 pairs/s) is ~0.5× the 5L DRAM=1.0 rates — LSM refresh is **write/compaction-bound**, so extra block cache hurts. **LSM has no in-memory speedup**; headline LSM number stays the 5L (DRAM 1.0) run. Only S2-view-slowest survives at DRAM=9 (S1/S3/S4 within compaction noise).

## 2026-05-25 — 5L + 5H memory-pressure A/B on the real SSD (tags `2026-05-24-refresh-{5L,5H}-ssd`)

- commit `83240c0a` (calcite-integration), host `c220g2-011011`; `build/scratch/run_refresh_{5L,5H}_ssd.sh` + matching `summarize_*`. Both cells re-run **at the same commit** (5L re-stamped from `c9b5f594`) so the only variable is DRAM: same SF (1550 btree / 3850 lsm, ~5 GiB secondaries), same recover-from-per-structure-copy + drop-caches cold start, S1–S4 both backends, `--refresh_seconds=90`, `--update_size=1`. 5L = DRAM 1.0 (c0); 5H = DRAM 0.4 (c3). Summaries: `paper-data/2026-05-24-refresh-{5L,5H}-ssd/` (`disk=ssd`). Metric `pair_tps` (RF1+RF2 iteration rate; RF2 does not exhaust at this scale).
- **Supports §5.4 + the memory-pressure claim cleanly.** 5L ordering S3 ≥ S1 > S2 holds (btree S3 1674 ≳ S1 1430 ≫ S2 781, S4 5531 ceiling; lsm S3 1150 ≈ S1 1155 ≫ S2 939, S4 1310). 5L→5H: **LSM flat (±2%)**, **btree degrades 21–59%** (S1 0.61×, S3 0.66×, **S4 0.41×**). At 5H LSM overtakes btree on S1/S2/S3 (S1 1156 vs 868, S2 916 vs 614, S3 1157 ≈ 1105) and btree's S4 lead collapses 4.2×→1.8×. Write-optimized SST flush/compaction is buffer-pool-independent; btree pays random page eviction once the working set spills the 0.4 GiB pool. Complements `2026-05-24-refresh-prewarm9` (abundant memory: LSM showed no in-memory speedup) — pressure is where LSM wins.

## 2026-05-25 — 5L + 5H + 5HH at **bg=2** (HTAP contention) on the real SSD (tags `2026-05-25-refresh-{5L,5H,5HH}-bg2-ssd`)

- commit `05e5bf98` (calcite-integration), host `c220g2-011011`; `build/scratch/run_refresh_bg2_ssd.sh <cell>` + `summarize_refresh_bg2_ssd.sh`. **First refresh runs with the bg=2 cohort** (new wiring `d4e06bfb`: concurrent Q3/Q5 + random point-lookup reads on BG_WORKER, foreground RF1/RF2 on MAIN_WORKER), aligning refresh with the paper-wide bg=2 setup. Three cells, same SF (1550 btree / 3850 lsm, ~5 GiB secondaries) + per-structure-copy + drop-caches cold start; only DRAM varies: 5L=1.0 (c0), 5H=0.4 (c3), 5HH=0.1 (c4, new highest pressure). S1–S4 both backends, `--refresh_seconds=90`, `--update_size=1`, single-rep. Summaries: `paper-data/2026-05-25-refresh-{5L,5H,5HH}-bg2-ssd/` (`disk=ssd`, `bg=2`, with a `bg_txs` column). 24/24 runs exit 0. Metric `pair_tps`.
- **Supports §5.4 under contention; adds the LSM-contention-robustness result.** §5.4 ordering S3 ≥ S1 > S2 (S4 ceiling) holds within each backend at bg=2 (5L btree S3 1157 ≳ S1 995 ≫ S2 520; lsm S3 986 ≈ S1 993 ≫ S2 809). vs bg=0: bg=2 costs **lsm only ~0.86×** but **btree ~0.70×**, and **btree S4 collapses 0.31×** (the cohort's hash queries scan the same base tables S4 uses). Memory pressure compounds (5L→5HH): btree ~0.5× (S3 1157→679), **lsm flat ~0.9–1.0×** (S3 986→886). **At 5HH, LSM beats btree on every structure incl. base** (lsm S4 1091 > btree S4 939). Caveat: `bg_txs`=2 at S4 (all cells) + btree S1 (5H/5HH) = two heavy cold scans eat the window (un-rotated, single bg worker, time-balanced) — low count there = heaviest contention, not light; S1–S3 elsewhere rotate to 80k–254k.
