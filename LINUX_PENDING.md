# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **q10_btree_7 / q10i_btree_7 rerun + size-accounting continuity
  (2026-05-28, post-c9406c44)** — the 5L btree sweep had two aborts
  at sx=7 (Q10/Q10I `get_size()` missing case 7); the case-7 fix
  landed as 9697ab5f. The follow-up size_audit refactor (c9406c44)
  then broadened every Sx's per-adapter sum (S2/S3/S5/S7 now include
  co-resident MIs / preagg) and added a `size_audit` console line
  per (query, sx). Returned value is still the per-adapter sum, but
  for several Sx that sum now differs from the pre-c9406c44 value.
  Re-running q10_btree_7 / q10i_btree_7 now writes new-accounting
  rows into a sweep whose other 26 rows have old-accounting size
  columns. Decision needed: (a) accept the 2-row inconsistency
  (size column is auxiliary), (b) re-run the entire 5L btree sweep
  with the new code for column-wise consistency. Defer until user
  picks. Also pending: q10i_btree_2 `std::out_of_range` — pre-existing
  abort in S2 image path; wants its own diagnostic worktree.

- **5L bg=2 sweep: `--param_seed=1,2` reps pending (2026-05-28)** —
  the first paper-sweep run after the family-image refactor (commits
  354dc7c4 → 1e8bd47f) covers btrees only at the 5L cell with
  `--param_seed=0` (single rep). Reps 1 and 2 (rotated parameters
  per `q3_family/params.hpp` PARAM_TABLE) are deferred until the
  LSM-side recover path is fixed and per-rep parameters can run
  on both backends symmetrically. Until then, single-rep results
  cannot show the rep-to-rep variance the paper's headline cells
  use to derive median + IQR; treat seed=0 as a smoke / signal
  pass, not the publishable figure.

- **LSM recover-from-image not set up properly (2026-05-28)** — the
  16-image per-Sx reload completed successfully on disk
  (`/mnt/ssd/tpch_{lsm,btree}_S{1..4}/$(scale)` and tpchi
  equivalents), but the LSM run path doesn't restore from the
  persisted image the way the btree path does; recovering forces
  a full re-load each run, wasting hours per sweep. Diagnostic +
  fix in a worktree branch; the btree sweep can proceed in the
  meantime since btree recover works.

- **No `latency.csv` from any binary (2026-05-23)** — **DEFERRED to
  next project (2026-05-23)**: btree binaries emit `cpu/bm/cr/dt.csv`
  but no `latency.csv`; LSM emits none of them. Analyzer no longer
  references `latency_p50_ms` / `latency_p99_ms` (columns dropped from
  `DIAG_FIELDS`); the diagnostics-explore plot's P99 panel renders as
  "latency_p99_ms not in CSV" — explicit signal, not silent gap.
  Populating it means threading latency-quantile capture through the
  TPut/`tx_seconds` loop in `frontend/tpch/tpch_workload.hpp` (or
  equivalent) **and re-running the entire experiment matrix** to
  backfill every existing sweep — out of scope for this project;
  revisit next project.

### From last-20-commit sweep (2026-05-27)

Open follow-ups flagged in commit bodies on `calcite-integration`
(2026-05-25 → 2026-05-26). Each cites its source commit SHA. Items
already closed by a later commit are omitted (notably 63c48999's "run
the 5L sweep / regen q10.pdf" — completed by 60702452).

- **S6 shared-view Linux perf sweep, Q3/Q5/Q10 (2026-05-27)** — commits
  aedf3cc1 (S6 `col_shared_view_t`, id=80), 6e216855 (plotter +
  `space_table.py` wiring). Code and analysis wiring landed and parity-
  verified at SF=1 on both backends, but **no S6 perf data exists yet**.
  Pending: run the S6 sweep so `TPut.s6.csv` lands and the
  "Mat-View (shared)" bar panels / space-table line render. The plotter
  already skips structures with no rows, so the sweep is the only missing
  piece. Q3+Q5 share one physical S6 table in the family image.

- **Q10 S6 view → family-image migration (2026-05-27)** — commit
  aedf3cc1. Q3+Q5 share one S6 table in the family image; Q10 currently
  builds a byte-identical copy in its **standalone** image. Migrating
  Q10's S6 into the shared family image is deferred.

- **Param-rotation A/B run (2026-05-27)** — commits 062afe29
  (`--param_seed` flag + q3 parity test), 94750ba9
  (`experiments/run_param_ab.sh` + `paper-data/scripts/param_ab_report.py`).
  Harness and reporter are built and code-validated only ("not exercised
  for this submission — time"). Pending: run the same-image
  default-vs-rotated A/B on Linux to back the "headline rests on a single
  substitution parameter" claim with evidence (currently a caveat in
  `paper-data/PAPER_EDITS.md` Edit 5).

- **Per-query bg=2 cohort topology + un-rotation caveat (2026-05-27)**
  — commits aee6c31a (refresh_sales bg=2 sweep), d4e06bfb
  (refresh_sales cohort wiring), 9483d7c9 (q10/q10i no-cohort
  fallback fix). Under the single time-balanced bg worker, any
  structure whose foreground query is heavy enough to fill the time
  slice on its own un-rotates to ~2 cold scans per 15 s, giving a
  misleadingly low `bg_txs`. Per-query topology and where the
  un-rotation bites:

  - **Q3, Q5** — vanilla family cohort, 2-step (Q3 then Q5 at the
    selected structure) plus heterogeneous base-table point-lookup
    step (`register_vanilla_bg_steps_at` in
    `frontend/tpch/tpch_vanilla_family.hpp:118`). S1–S4 only (no S5,
    per PLAYBOOK soundness rule). Cleanly rotates at 5L; un-rotation
    status at 5H/5HH not yet characterised on Linux.
  - **Q3I, Q5I** — invoice-extended family cohort, 2-step (Q3I +
    Q5I) at S1–S4; the `Structure==5` specialisation drops Q5I so
    the cohort is Q3I-only there (Q5I has no aCOLI variant — see
    `frontend/tpch/tpchi_family.hpp:86–113`). Bg=2 rotation
    behaviour at 5L/5H/5HH on Linux pending.
  - **Q10, Q10I** — no family cohort registered; the no-cohort
    fallback runs a heterogeneous base-table point-lookup stream on
    BG_WORKER and re-runs the foreground query on MAIN_WORKER (1:1
    wall-clock, fix 9483d7c9). At 5L: fast structures (S1/S3/S5)
    see ~40k–64k `bg_txs` from the point-lookup stream; slow
    structures (S2, S4) see `bg_txs` ≈ 2 because one query fills
    the 15 s window. Same shape as refresh_sales S4 / btree S1.
  - **Q12** — same no-cohort fallback path as Q10/Q10I (no
    `register_*_bg_steps` call from
    `frontend/tpch/q12/executable_rocksdb.cpp`). Bg=2 perf not yet
    collected on Linux; expect the same heavy-structure un-rotation
    when any structure runs >7.5 s per call.
  - **refresh_sales** — RF1/RF2 update workload with a concurrent
    read-only worker reusing the vanilla Q3+Q5 cohort
    (`register_vanilla_bg_steps<B>(... q3, q5, structure,
    FLAGS_bg_point_lookups)`, commit d4e06bfb). S4 (all cells) and
    btree S1 (5H/5HH) un-rotate to 2 heavy cold scans — the
    original entry's anchor case.

  Pending: (1) re-run the existing images with a multi-worker bg
  cohort (≥ N=cohort-size workers, no time-balance) to lift
  `bg_txs` at the un-rotation cases; (2) once Linux perf data lands
  for Q3/Q5/Q3I/Q5I at 5H/5HH, fold any new un-rotation observations
  back into this list. Candidate follow-up applies across the whole
  list, not just refresh_sales.

*(All Q5I bring-up items closed 2026-05-17 — see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Q5 first Linux perf sweep
closed 2026-05-14, Q5I LSM and btree first-Linux sweeps closed
2026-05-17. Optional Q5 SF=40 disk-bound replay deferred without
owner.)*
