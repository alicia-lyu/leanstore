# Paper-data plotter

`scripts/plot_paper_sweep.py` reads the four summary CSVs emitted by
`scripts/analyze_paper_sweep.py` and writes a directory of paper-ready
figures (PDF + PNG sibling) under `paper-data/<tag>/figures/`.

## Run

```bash
# Default: produce all 9 figures, PDF + PNG, for the named tag
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-a

# Subset
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
| `headline_tpch_vs_dram.pdf` | stats.csv | TX/s vs DRAM at fixed secondary=5 GiB (cells c3→c0). 2×4 grid, one panel per TPC-H binary. Lines = S1..S4, bg=0 solid / bg=1 dashed. Twin axis on right = ms/query. |
| `headline_tpch_vs_secondary.pdf` | stats.csv | TX/s vs secondary at fixed 1:5 ratio (c2→c1→c0). Same shape. |
| `headline_geo_vs_dram.pdf` | stats.csv | Faceted grid: rows = geo binary, cols = tx type (10 types). Same line conventions. |
| `headline_geo_vs_secondary.pdf` | stats.csv | Same shape, secondary axis. |
| `contention_dropoff.pdf` | stats.csv | Per (binary, cell, structure): TX/s ratio bg=1 / bg=0. Bar chart; values < 1 = degradation under contention. |
| `inversions.pdf` | inversions.csv | Bar chart of any S3-vs-S2 inversions flagged by the analyzer. Empty → renders a "no inversions flagged" note. |
| `s3_vs_s2_speedup.pdf` | stats.csv | Heatmap binary × cell, value = S3/S2 median TX/s ratio (bg=0). Cells > 1.00× favor S3. |
| `duration_baseline.pdf` | stats.csv | Reference bars: raw ms/query at anchor cell c0, bg=0, per (binary, structure). Anchors intuition for the TX/s axes in the headline plots. |
| `diagnostics_attribution.pdf` | diagnostics.csv | 2×2 grid: LLC misses/TX, BM free %, BM evicted MiB, P99 latency. X=cell, lines=structure, median across binaries. LSM panels blank for LeanStore-only counters and vice versa. |

Every figure carries a footer line with `tag`, `commit`, `host`, and
the cell list from `manifest.yaml`.

## When a sweep is incomplete

The plotter doesn't error on missing cells / missing binaries. A
sub-panel with no data renders as a centered "no data" note; lines
across cells render with a gap where data is absent (no fabricated
zero).

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
