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
