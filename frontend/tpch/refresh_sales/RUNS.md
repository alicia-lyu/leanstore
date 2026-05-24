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
