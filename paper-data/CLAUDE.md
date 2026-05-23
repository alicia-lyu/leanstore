# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This directory holds **paper-figure data**: raw per-rep sweep snapshots,
analyzer-produced summary CSVs, and matplotlib figures. The execution
engine that produces these snapshots lives in the parent repo; see
[`../CLAUDE.md`](../CLAUDE.md) for build/run of the binaries themselves.

## Layout

```
paper-data/
├── SWEEP_LOG.md         # rolling status doc — read this first to see what's running
├── scripts/             # analyzer + plotter (Python 3, stdlib + matplotlib/pandas/numpy/pyyaml)
│   ├── analyze_paper_sweep.py
│   ├── plot_paper_sweep.py
│   ├── analyze_sweep.py    # older, pre-paper analyzer; kept for ad-hoc use
│   └── PLOTTING.md         # figure catalog + add-a-figure recipe
└── <tag>/               # one directory per sweep run, tag = YYYY-MM-DD-{a,b,...}
    ├── manifest.yaml    # tag, commit, host, cells, families, reps, runs_ok/err
    ├── raw/             # gitignored, large — per-rep snapshots
    │   └── <binary>/<cell>-bg<bg>-r<rep>/
    │       ├── TPut.csv, TPut.s<N>.csv      # throughput, per-structure slices
    │       ├── size.csv,  size.s<N>.csv
    │       ├── sstables.csv                  # LSM only
    │       ├── structure<N>.log, structure<N>_stderr.txt
    │       └── <tx>/<method>/{bm,cpu,cr,dt,latency}.csv  # detail counters
    ├── summary/         # git-tracked — analyzer output
    │   ├── headline.csv      # 1 row per (binary,cell,structure,bg,rep,method,tx)
    │   ├── diagnostics.csv   # same key + attribution columns (LLC, BM, P99, rocksdb counters)
    │   ├── inversions.csv    # rows where S3 ms > S2 ms by > threshold
    │   └── stats.csv         # 1 row per (binary,cell,structure,bg) — median + IQR
    └── figures/         # gitignored, regenerable — PDF + PNG siblings
```

**Note**: `paper-data/scripts/` was recently moved here from
`../scripts/`. The directory shows as untracked (`?? scripts/`) in
`git status` until staged. `experiments/run_paper_sweep.sh` in the
parent repo still references the old path — search before relocating
permanently.

## Regenerating summaries and figures

```bash
# Analyzer: walks <tag>/raw/ → writes <tag>/summary/*.csv
python3 scripts/analyze_paper_sweep.py --tag 2026-05-18-b --root 2026-05-18-b

# Plotter: reads <tag>/summary/*.csv → writes <tag>/figures/*.{pdf,png}
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --figures headline_tpch_vs_secondary,inversions
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --format png
```

The plotter and analyzer never error on missing data — incomplete cells
render as "no data" panels or gaps in lines. Safe to run mid-sweep.

`experiments/run_paper_sweep.sh` (parent repo) invokes the analyzer
*and* plotter automatically after a sweep finishes, so a clean sweep
produces both `summary/` and `figures/` without a second command.

## Sweep-matrix conventions

Hardcoded in `analyze_paper_sweep.py` and used everywhere downstream:

- **Cells** (DRAM × SF combinations): `c0` (1.0 GiB, biggest SF), `c1` (0.4), `c2` (0.1, small-SF baseline — dropped from `-b`), `c3` (0.4, same DRAM as c1 but bigger SF).
- **TPC-H SFs**: lsm `{c0:3850, c1:1500, c2:380, c3:3850}`; btree `{c0:1550, c1:620, c2:150, c3:1550}`. (Btree SFs are smaller because btree storage is denser.)
- **Geo SFs**: `geo_lsm {c0:515, c1:206, c2:52, c3:515}`; `geo_btree {c0:194, c1:78, c2:19, c3:194}`.
- **Background-tx modes** (`bg`): `0`=isolated, `1`=single bg query, `2`=Q3+Q5+random-pointlookup cohort (the paper-headline mode).
- **Structures** (1–5): `S1` base merge-join (red), `S2` materialised view (blue), `S3` merged-index — the headline (**green**), `S4` hash join (orange), `S5` aCOLI MI (purple, **deferred from paper**). Per-row "best of S2/S3" is the paper's main inversion claim.
- **Binaries**: 8 TPC-H (`q3,q5,q3i,q5i` × `lsm,btree`) + 2 geo (`geo_lsm,geo_btree`). Family map (`FAMILY_OF` in the analyzer): `vanilla` (q3,q5), `tpchi` (q3i,q5i — invoice-extended), `geo`.

If you add a new binary or cell, update `FAMILY_OF` / `CELL_DRAM` /
`SF_TPCH` / `SF_GEO` in `analyze_paper_sweep.py` — the plotter is
schema-driven from the resulting CSVs and needs no per-binary edits.

## Plotting conventions

Detail in `scripts/PLOTTING.md`. The non-obvious bits:

- **Primary axis is ms/query**, log scale, lower-is-better. TX/s lives on the right twin axis. Aggregation is done in ms-space (median + IQR) directly from `headline.csv` via `aggregate_ms_per_query()` — *not* by inverting stats.csv's `tx_per_s` aggregates (1000/x is non-linear and would distort IQR).
- All colors/markers/labels live in the `STYLE` dict at the top of `plot_paper_sweep.py`. Don't hardcode in builders.
- Adding a figure = write `fig_<name>(data: SweepData) -> Optional[Path]`, register in `FIGURE_BUILDERS`, add a row to `PLOTTING.md`'s catalog table. Return `None` when there's nothing to plot (renders as "no data" rather than erroring).
- Every figure carries a footer with `tag`, `commit`, `host`, and the cell list (read from `manifest.yaml`).

## What's tracked vs gitignored

- **Tracked**: `summary/*.csv`, `manifest.yaml`, `SWEEP_LOG.md`, `scripts/`.
- **Gitignored** (large / regenerable): `<tag>/raw/`, `<tag>/figures/`.

When pulling on a fresh machine, summaries + manifest are present but
`raw/` and `figures/` are not. Regenerate figures with the plotter from
the checked-in summaries; raw cannot be reconstructed without re-running
the sweep.

## SWEEP_LOG.md

Hand-off log between hourly background checks. Newest entries on top.
The "Active" section tracks the in-flight tag; sections flip to "Frozen"
when a tag halts. Update the "Latest status" bullet (don't append a
new dated section for every check — overwrite the bullet, keep history
in the bullet list below).
