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
