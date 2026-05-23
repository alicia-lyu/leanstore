# Paper-data plotter

`scripts/plot_paper_sweep.py` reads the four summary CSVs emitted by
`scripts/analyze_paper_sweep.py` and writes a directory of paper-ready
figures (PDF + PNG sibling) under `paper-data/<tag>/figures/`.

## Run

```bash
# Default mode: sweep-diagnostic figures (the 9 builders below)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-a

# Paper-figures mode: 3 typeset-ready figures under figures/paper/
# (1×4 TPC-H btree row, 1×4 TPC-H lsm row, 2×3 condensed geo grid)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --mode paper-figures

# Diagnostics-explore mode: 6-panel metric grids + diagnostics_paper.csv
# (scratch outputs under figures/diagnostics/; pick 1-2 panels for the paper)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --mode diagnostics-explore

# Subset (overrides --mode)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-a \
    --figures headline_tpch_vs_secondary,inversions

# Alternate format
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-a --format svg

# Different root (rarely needed)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-a \
    --root /tmp/some-other-paper-data/2026-05-18-a
```

## Dependencies

`matplotlib`, `pandas`, `pyyaml`, `numpy`. On Ubuntu/Debian:
```bash
sudo apt-get install python3-matplotlib python3-pandas python3-yaml python3-numpy
```
Versions tested on the CloudLab node: matplotlib 3.5.1, pandas 1.3.5,
numpy 1.21.5.

## Figure catalog

| Filename | Reads | What it shows |
|---|---|---|
| `headline_tpch_vs_dram.pdf` | headline.csv | **ms/query** (log scale) vs DRAM at fixed secondary=5 GiB (cells c3→c0). 2×4 grid, one panel per TPC-H binary. Lines = S1..S4, bg=0 solid / bg=1 dashed. Twin axis on right = TX/s. Lower is better. |
| `headline_tpch_vs_secondary.pdf` | headline.csv | ms/query vs secondary at fixed 1:5 ratio (c2→c1→c0). Same shape. |
| `headline_geo_vs_dram.pdf` | headline.csv | Faceted grid: rows = geo binary, cols = tx type (10 types). ms/query, log scale. |
| `headline_geo_vs_secondary.pdf` | headline.csv | Same shape, secondary axis. |
| `contention_dropoff.pdf` | headline.csv | Per (binary, cell, structure): ms_bg=1 / ms_bg=0 slowdown. Bar chart; values > 1 = worse under contention. |
| `inversions.pdf` | inversions.csv | Bar chart of S3-vs-S2 inversions flagged by the analyzer, presented as ms_s3 / ms_s2 (>1 = S3 slower = inversion). Empty file → renders a "no inversions flagged" note. |
| `s3_vs_s2_speedup.pdf` | headline.csv | Heatmap binary × cell, value = S3 speedup over S2 in ms terms (S2 ms / S3 ms; bg=0). Cells > 1.00× favor S3. |
| `duration_baseline.pdf` | stats.csv | Reference bars: raw ms/query at anchor cell c0, bg=0, per (binary, structure). Anchors intuition for the headline plot axes. |
| `diagnostics_attribution.pdf` | diagnostics.csv | 2×2 grid: LLC misses/TX, BM free %, BM evicted MiB, P99 latency. X=cell, lines=structure, median across binaries. LSM panels blank for LeanStore-only counters and vice versa. |
| **`paper/paper_tpch_btree_headline.pdf`** | headline.csv | **Paper-mode.** 1×4 single-column row (q3, q5, q3i, q5i btree). bg=2 only, structures S1–S4 only. X-axis = data size (GiB) with H/L memory-pressure suffix: `2` (sec 2 GiB), `5H` (sec 5 GiB, DRAM 0.4), `5L` (sec 5 GiB, DRAM 1.0). Y = ms/query (log). Shared legend on top. Stacks vertically with the lsm sibling for backend comparison. |
| **`paper/paper_tpch_lsm_headline.pdf`** | headline.csv | **Paper-mode.** Same shape and geometry as the btree sibling above, for the lsm backend. Legend omitted (parent doc uses the btree figure's legend). |
| **`paper/paper_geo_condensed.pdf`** | headline.csv | **Paper-mode.** 2×3 grid: rows = geo backend (btree, lsm), cols = tx pattern (join-nsc, mixed-nsc, distinct-nsc) — all at depth nsc. Same data-size x-axis and colour map as the TPC-H figures. |
| **`diagnostics/diag_explore_all.pdf`** | diagnostics.csv | **Diagnostics-explore mode.** 6-panel metric grid: LLC misses/TX, cycles/TX, BM eviction rounds, BM evicted MiB, TX restarts, P99 latency. Median across all TPC-H/TPCHI binaries at bg=2. Use to scan for which 1–2 metrics earn a paper spot. |
| **`diagnostics/diag_explore_q3i_lsm.pdf`** | diagnostics.csv | **Diagnostics-explore mode.** Same 6 metrics for q3i_lsm only — the binary with the flagged S3-vs-S2 inversion at large DRAM. |
| **`summary/diagnostics_paper.csv`** | diagnostics.csv | **Diagnostics-explore mode side effect.** One row per (binary, cell, structure) with the six explored metrics, so the writer can table-ify any of them without re-running the analyzer. |

**Y-axis convention:** primary axis is **ms/query** (lower is better, the
intuitive lens for "how slow is each query"). TX/s lives on the right twin
axis so reviewers can read throughput at a glance. ms is plotted on a log
scale because durations span 2-3 orders of magnitude across structures and
backends (e.g. q3_lsm S3 ≈ 600 ms vs q3_btree S4 ≈ 140 s at c2). All
ms-aggregates (median + IQR) are computed per-rep in ms-space — they are
NOT derived from stats.csv's TX/s aggregates, since 1000/x is non-linear
and would distort the IQR. The plotter aggregates from headline.csv
directly via `aggregate_ms_per_query()` for this reason.

Every figure carries a footer line with `tag`, `commit`, `host`, and
the cell list from `manifest.yaml`.

## When a sweep is incomplete

The plotter doesn't error on missing cells / missing binaries. A
sub-panel with no data renders as a centered "no data" note; lines
across cells render with a gap where data is absent (no fabricated
zero).

## Known data gap (as of 2026-05-23)

`diagnostics.csv` for `2026-05-18-b` has all attribution columns
(`cpu_*`, `bm_*`, `cr_*`, `dt_*`, `latency_*`, `lsm_*`) **empty**:
the per-rep detail counters under `<binary>/<cell>-bgN-rR/<tx>/<method>/`
exist on disk but the analyzer isn't joining them into the
diagnostics CSV. `diag_explore_*` renders as "no data" panels until
that's fixed. Plotter is correct; analyzer needs a follow-up to
populate these columns. Until then, treat `diagnostics-explore` mode
as scaffolding that's ready for data, not a live signal.

## Adding a new figure

Three steps:

1. Write a builder `fig_<name>(data: SweepData) -> Optional[Path]`
   that composes the existing plot primitives (`line_with_iqr`,
   `grouped_bar`, `heatmap`) and returns the output path (or `None`
   if there's nothing to plot).
2. Register it in the `FIGURE_BUILDERS` dict at the bottom of
   `plot_paper_sweep.py`.
3. Add a row to the catalog table above.

All styling lives in the `STYLE` dict at the top of the module —
don't hardcode colors/markers in the builder.

## Pre-baked into the runner

`experiments/run_paper_sweep.sh` invokes the plotter after the
analyzer finishes. A clean sweep run produces both
`paper-data/<tag>/summary/` and `paper-data/<tag>/figures/`
without a second command.
