# Archived paper-data plotter code

`legacy_figures.py` holds the nine figure builders that used to make
up the default-mode output of `scripts/plot_paper_sweep.py`. They
were retired when the paper revision settled on the paper-figures
mode (1×4 TPC-H rows with H/L pressure annotation + condensed geo)
plus the diagnostics-explore mode.

## When to revive something from here

- **`fig_contention_dropoff`, `fig_inversions`** — sweep-internal QA.
  Useful when a new sweep mixes bg=0 and bg=1 runs and you want to
  spot pathological contention slowdowns or S3-vs-S2 ranking flips
  before deciding which numbers to take to the paper.
- **`fig_s3_vs_s2_speedup`** (heatmap) — quick visual when you want
  one summary chart of "is merged faster than view, and by how
  much, across every (binary, cell)?". Not paper-grade by itself
  but a handy debugging aid.
- **`fig_duration_baseline`** — absolute-latency reference bars at
  the anchor cell. Useful to remind yourself of the ms order of
  magnitude per binary before reading the log-scale paper plots.
- **`fig_headline_tpch` (2×4)** and **`fig_headline_geo`** (faceted
  by tx type) — earlier draft figures. If the paper ever needs to
  cover bg=0 and bg=1 side by side, or all tx patterns at once,
  these are the starting point.
- **`fig_diagnostics`** (2×2 LLC / BM / latency) — the cleaned-up
  version of this exact 2×2 layout is what we'd want as the
  attribution figure in §5.5, but only once the raw counters in
  `diagnostics.csv` are actually populated (see LINUX_PENDING).

## How to use

```python
import sys
sys.path.insert(0, "paper-data/scripts")          # so the import resolves
from plot_paper_sweep import load_sweep
from archive.legacy_figures import fig_contention_dropoff

data = load_sweep("2026-05-18-b", Path("paper-data/2026-05-18-b"))
fig_contention_dropoff(data)
```

Or to wire one back into the CLI temporarily, import it inside
`plot_paper_sweep.py` and register it in `FIGURE_BUILDERS`.

## What this archive depends on

`plot_paper_sweep.py` (the live module) for:

- `STYLE`, `STRUCTURE_LABELS`, `SweepData`
- `aggregate_ms_per_query`, `line_with_iqr`, `_structure_style`
- `_save`, `_all_or_empty`

Constants and primitives that only the legacy code used
(`DRAM_AXIS_CELLS`, `SECONDARY_AXIS_CELLS`, `ANCHOR_CELL`,
`TPCH_BINARIES`, `GEO_BINARIES`, `DIAG_PANELS`, `grouped_bar`,
`heatmap`, `_add_twin_axis` & friends) are re-declared at the top
of `legacy_figures.py`, so the archive stays self-contained — no
need to keep zombie symbols in the live module.

## Why archive rather than delete

The paper sweep matrix evolves. Today bg=2 is the only mode that
matters; the next reviewer round may ask for bg=0/bg=1 sensitivity
or a wider cell grid, and these builders are the cheapest path
back to that style of output.
