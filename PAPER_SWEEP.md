# Paper Sweep — Canonical Configuration

This file is the **canonical, git-tracked specification** of the
experiment matrix and data-gathering format for the paper-ready
performance sweep. Anything not pinned here is not part of the
paper-reported results.

Update this file *before* running a sweep that changes the matrix,
schema, or output layout. The runner script in
[`experiments/run_paper_sweep.sh`](experiments/run_paper_sweep.sh)
reads
[`experiments/sweep.yaml`](experiments/sweep.yaml) which is derived
from the matrix below — keep both in sync.

## Scope

Two benchmark families plus one decoupled microbenchmark:

- **Vanilla COL family** — `{Q3, Q5}`. Both use the
  3-table COL merged index (`MergedAdapter<customer_coli_t,
  orders_coli_t, lineitem_col_t>`). Shared load image at
  `$(data_disk)/tpch_{lsm,btree}/`.
- **Invoice-extended COLI family** — `{Q3I, Q5I}`. Both use the
  4-table COLI merged index
  (`MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t,
  invoice_coli_t>`). Shared load image at
  `$(data_disk)/tpchi_{lsm,btree}/`.
- **Geo microbenchmark** — `geo` (5-table hierarchical join).
  Decoupled from TPC-H per Tweak 2 (commit `f35b9b16`). Own scale
  axis via `--geo_scale_factor`.

**Excluded from the paper sweep**:
- **Q12** — uses the OL pipeline (orders × lineitem only) which is
  not directly comparable with the COL family. Self-contained build
  retained for proof-of-concept / regression catching, not for
  paper figures.
- **Q9, Q10I** — skeleton / design-doc only.
- **Q3I S5 (aCOLI MI)** — implemented and parity-verified but
  deferred from paper sweep per [`PLAYBOOK §S5`](frontend/tpch/PLAYBOOK.md).

## The matrix

For every (binary × cell) combination listed below, run all four
storage structures S1–S4 *and* both `--bg_query_thread` settings
(false → isolated baseline, true → contention measurement). That
gives 8 runs per cell per binary.

### TPC-H binaries (per family)

| Family | Binaries | Image dir |
|--------|----------|-----------|
| Vanilla | `q3_lsm`, `q3_btree`, `q5_lsm`, `q5_btree` | `$(data_disk)/tpch_{lsm,btree}/` |
| TPCHi   | `q3i_lsm`, `q3i_btree`, `q5i_lsm`, `q5i_btree` | `$(data_disk)/tpchi_{lsm,btree}/` |

A single load per (family × backend × SF) serves all binaries in
that family. Total persisted images per family: one per (backend ×
SF) combination.

### Geo binaries

| Backend | Binary | Image dir |
|---------|--------|-----------|
| RocksDB    | `geo_lsm`   | `$(data_disk)/geo_lsm/`   |
| LeanStore  | `geo_btree` | `$(data_disk)/geo_btree/` |

### DRAM × secondary-size cells (per binary)

The paper focuses on **beyond-memory** workloads, so the matrix is
parameterised by `DRAM` and the **target main-secondary size**
(LSM/BTree SF is then derived from the Target-SF table in
[`frontend/tpch/RUNS.md`](frontend/tpch/RUNS.md)). The "1:5 rule"
says DRAM ≈ secondary / 5 — at that ratio, the working set is
~5× DRAM, comfortably beyond memory.

| Cell tag | DRAM (GiB) | Main secondary | LSM SF | BTree SF | Geo LSM | Geo BTree | Regime |
|----------|----:|---------------:|-------:|---------:|--------:|----------:|--------|
| `c0` | 1.0  | 5 GiB  | 3850 | 1550 | 515 | 194 | User-requested anchor; main secondary 5× DRAM. |
| `c1` | 0.4  | 2 GiB  | 1500 | 620  | 206 | 78  | Prior q5i_btree paper baseline (commit `a8513db5`); 1:5 ratio. |
| `c2` | 0.1  | 0.5 GiB | 380  | 150  | 52  | 19  | Smaller end of beyond-memory; faster reload for development. |
| `c3` | 0.4  | 5 GiB  | 3850 | 1550 | 515 | 194 | Same SF as c0 but 0.4 GiB DRAM — isolates DRAM pressure at fixed secondary size. |

SFs come from the calibration in
[`frontend/tpch/RUNS.md §Target-SF table`](frontend/tpch/RUNS.md):
- TPC-H LSM: ~1.3 MiB/SF main per-query secondary.
- TPC-H BTree: ~3.3 MiB/SF.
- Geo LSM: 9.7 MiB/SF (empirical, geo merged S3 measured 2026-05-18).
- Geo BTree: 25.8 MiB/SF (empirical, same measurement).

BTree is denser than LSM in both benchmarks (~2.5–2.7×) because
LeanStore's secondaries pack tighter than RocksDB SST-encoded LSM
ones. Geo is denser than TPC-H (~7× LSM, ~8× BTree) because its
`customer2_t` records carry the full TPC-H customer payload while
the per-query views/MIs cover the full 5-table hierarchy.

The cross gives two natural paper plots:
- **TX/s vs DRAM at fixed secondary size = 5 GiB** —
  cells `c3 → c0` (DRAM: 0.4 → 1.0 GiB, same SF).
- **TX/s vs secondary size at fixed 1:5 DRAM ratio** —
  cells `c2 → c1 → c0` (secondary: 0.5 → 2 → 5 GiB,
  DRAM: 0.1 → 0.4 → 1.0 GiB).

### Background-contention axis

For every (binary, cell, structure) combination, run two variants:

- `bg0` — `--bg_query_thread=false` (TPC-H) / `--geo_bg_thread=false` (geo).
  Foreground query only; baseline.
- `bg1` — `--bg_query_thread=true` (TPC-H) / `--geo_bg_thread=true` (geo).
  Background worker cycles family queries at the same
  `--storage_structure` as the foreground (TPC-H) or runs geo-only
  insert/erase on `customer2` (geo).

### Repetitions

Each (binary, cell, structure, bg) tuple is run **3 times**. Per-cell
summary statistics (median TX/s, IQR) are computed in
`analyze_sweep.py`. Three is the minimum for a non-degenerate IQR;
go higher only if a cell shows high variance in the first pass.

### Run-time per cell

Each foreground TX loop runs for `--tx_seconds=15` (current default
in `tpch_flags.hpp`). Per-cell TX time is fixed; the dominant cost
is **load time per family × SF**, which grows roughly linearly with
SF.

- TPC-H foreground TX: 4 binaries × 4 cells × 4 structures × 2 bg
  × 3 reps × 15s ≈ ~96 min per backend.
- Geo foreground TX: 2 binaries × 4 cells × 4 structures × 2 bg
  × 3 reps × 9 queries × 15s ≈ ~3.6 hours per backend.
- Load time: c0 / c3 share SF (both 3850 LSM / 1550 BTree), so
  one load per family per SF tier. The Target-SF table loads at
  ≈ 2–5 hours per family per SF tier on the CloudLab node; c0/c3
  is the dominant load cost.

Plan to run c2 and c1 first (faster reload), then c0/c3 overnight.
Plan for a 24–36 hour total sweep budget on a clean Linux box if
running both backends from scratch.

## Output format

Two-tier output: raw per-run CSVs stay in `build/` (gitignored,
large), summary CSVs land in `paper-data/` (git-tracked, small).

```
build/                                       # gitignored; raw CSVs per make-target
  <binary>/<sweep-tag>/c<N>-bg<0|1>-r<R>/   # one dir per run
    TPut.csv                                 # high-level throughput
    Elapsed.csv                              # elapsed-time variant
    size.csv                                 # per-structure size
    <tx>/<method>/bm.csv                     # per-phase detail CSVs
    <tx>/<method>/cpu.csv                    # ditto
    <tx>/<method>/latency.csv                # ditto
    <tx>/<method>/cr.csv                     # ditto
    <tx>/<method>/dt.csv                     # ditto

paper-data/<sweep-tag>/                      # git-tracked; one per sweep run
  manifest.yaml                              # cell list + commit SHA + host info
  summary/
    headline.csv                             # one row per (binary,cell,struct,bg,rep) — paper-direct
    diagnostics.csv                          # one row per (binary,cell,struct,bg,rep) — attribution columns
    inversions.csv                           # S3-vs-S2 inversions flagged by analyzer (empty if none)
    stats.csv                                # median + IQR across reps, one row per (binary,cell,struct,bg)
  figures/                                   # emitted by scripts/plot_paper_sweep.py
    headline_{tpch,geo}_vs_{dram,secondary}.{pdf,png}
    contention_dropoff.{pdf,png}
    inversions.{pdf,png}
    s3_vs_s2_speedup.{pdf,png}
    duration_baseline.{pdf,png}
    diagnostics_attribution.{pdf,png}
```

See [`scripts/PLOTTING.md`](scripts/PLOTTING.md) for the figure
catalog and the design of `plot_paper_sweep.py`. The runner script
invokes the plotter automatically after the analyzer succeeds.

`<sweep-tag>` is the day plus a letter suffix (e.g. `2026-05-18-a`)
so successive sweeps don't collide. The runner pipes `build/.../*.csv`
through `scripts/analyze_sweep.py` to produce the summary CSVs.

The repo growth budget is ~tens of KB per sweep: 4 summary CSVs ×
(8 binaries × 3 cells × 4 structures × 2 bg × 3 reps ≈ 576 rows) +
manifest. Multi-sweep history fits comfortably under 10 MB even
after a dozen iterations of the matrix.

### CSV column schema

#### `summary/headline.csv` — for paper plots

One row per (binary, cell, structure, bg, rep). This is the file the
paper figures consume.

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `binary` | runner | str  | e.g. `q3_lsm` |
| `family` | runner | str  | `vanilla` / `tpchi` / `geo` |
| `backend` | runner | str  | `lsm` / `btree` |
| `query` | runner | str  | `q3` / `q5` / `q3i` / `q5i` / `geo` |
| `cell` | runner | str  | `c0` / `c1` / `c2` / `c3` |
| `sf` | runner | int  | scale factor |
| `dram_gib` | runner | float | `--dram_gib` value |
| `structure` | runner | int  | 1 / 2 / 3 / 4 |
| `method` | TPut.csv | str  | e.g. `mi_col_walk` (verbose name) |
| `tx` | TPut.csv | str  | always `query` for TPC-H; per-tx for geo |
| `bg` | runner | int  | 0 / 1 |
| `rep` | runner | int  | 1 / 2 / 3 |
| `tx_per_s` | TPut.csv `TPut (TX/s)` | float | the headline |
| `tx_count` | runner | int | TXs completed in the window |
| `bg_tx_count` | binary stderr | int | bg thread TXs in the same window |
| `ms_per_tx` | derived | float | 1000 / tx_per_s |
| `size_mib` | TPut.csv `size (MiB)` | float | structure footprint |
| `commit_sha` | runner | str  | git HEAD at sweep time |

#### `summary/diagnostics.csv` — for attribution

Same row shape as `headline.csv` plus the BM/CPU/CR/DT/Latency columns
the analyzer surfaces. Schema deliberately wide so we can run an
"explain this outlier" pass with a stable column set.

| Column group | Source | Purpose |
|--------------|--------|---------|
| `cpu_cycles_per_tx`, `cpu_instr_per_tx`, `cpu_l1_miss_per_tx`, `cpu_llc_miss_per_tx`, `cpu_branch_miss_per_tx`, `cpu_util_pct` | `cpu.csv` | CPU-bound vs cache-bound attribution |
| `bm_free_pct`, `bm_consumed_pages`, `bm_p1_pct`, `bm_p2_pct`, `bm_p3_pct`, `bm_evicted_mib`, `bm_async_write_mib`, `bm_rounds` | `bm.csv` | buffer-pool pressure (LeanStore only) |
| `cr_committed`, `cr_restarts`, `cr_gct_committed`, `cr_wal_write_gib`, `cr_pct_gc` | `cr.csv` | abort / GC overhead (LeanStore only) |
| `dt_struct_split`, `dt_misses_counter`, `dt_page_reads`, `dt_page_writes` | `dt.csv` | data-structure operation counts |
| `latency_p50_ms`, `latency_p99_ms` | `latency.csv` | latency distribution |
| `lsm_block_read_count`, `lsm_block_cache_hit_pct`, `lsm_iter_next_cpu_ms` | per-query stderr | RocksDB-specific perf-counter totals (only for `_lsm` binaries) |

Rocks-only counters are blank for `_btree` rows; LeanStore-only BM/CR
columns are blank for `_lsm` rows. The runner records which are
expected to be blank in `manifest.yaml`.

#### `summary/inversions.csv` — S3-vs-S2 flag report

One row per (binary, cell, bg, rep) where S3 throughput is more than
**5%** below S2. Columns: the diagnostics columns for both S2 and S3
side-by-side, plus a one-line `attribution_hint` derived from the
largest delta column. Empty file if no inversions — that's the
expected outcome.

#### `summary/stats.csv` — median + IQR across reps

One row per (binary, cell, structure, bg). Columns: `tx_per_s_median`,
`tx_per_s_iqr`, `bg_tx_count_median`, plus the diagnostics medians.
This is the file paper figures consume when they need a single number
per cell (no error bars).

## Runner contract

[`experiments/sweep.yaml`](experiments/sweep.yaml) defines the matrix
in machine-readable form. The runner script
[`experiments/run_paper_sweep.sh`](experiments/run_paper_sweep.sh)
iterates the cells, invokes the right `make q*_iso_N` (or geo)
target per cell, copies CSVs into `paper-data/<sweep-tag>/raw/`, and
runs `scripts/analyze_sweep.py` to populate `summary/`.

The yaml is the single source of truth for the matrix; this file
documents the schema and the *why*.

## Updating this document

When the matrix changes:
1. Edit this file + `experiments/sweep.yaml` in the same commit.
2. Add a one-line dated note in
   [`frontend/tpch/HISTORY.md`](frontend/tpch/HISTORY.md) (or
   `frontend/geo/RUNS.md` for geo-only changes) so the reason is
   discoverable.
3. The next sweep automatically picks up the new matrix.

If a cell is dropped or added mid-sweep, do not edit
`paper-data/<sweep-tag>/manifest.yaml` after the fact — start a new
tagged sweep instead.
