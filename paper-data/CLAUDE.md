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
├── diagrams.yaml        # one entry per paper diagram → source tag(s) + builder name
├── diagrams/            # gitignored, regenerable — shared output dir for all paper figures
│   ├── paper_tpch_btree_headline.{pdf,png}
│   ├── paper_tpch_lsm_headline.{pdf,png}
│   └── ...              # one file per diagrams.yaml entry
├── scripts/             # analyzer + plotter (Python 3, stdlib + matplotlib/pandas/numpy/pyyaml)
│   ├── analyze_paper_sweep.py
│   ├── plot_paper_sweep.py
│   ├── plot_refresh_lsm_vs_btree.py
│   ├── plot_refresh_sales.py
│   ├── _diagram_metadata.py   # shared YAML helper (load_metadata, sources_for, output_path)
│   ├── analyze_sweep.py       # older, pre-paper analyzer; kept for ad-hoc use
│   └── PLOTTING.md            # figure catalog + add-a-figure recipe
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
    └── figures/         # gitignored, regenerable — per-sweep diagnostic PDFs
```

## Regenerating summaries and figures

```bash
# Analyzer: walks <tag>/raw/ → writes <tag>/summary/*.csv
python3 scripts/analyze_paper_sweep.py --tag 2026-05-18-b --root 2026-05-18-b

# Paper plotter: reads diagrams.yaml → writes paper-data/diagrams/*.{pdf,png}
python3 scripts/plot_paper_sweep.py --all
python3 scripts/plot_paper_sweep.py --diagram paper_tpch_btree_headline
python3 scripts/plot_paper_sweep.py --diagram paper_tpch_btree_headline,paper_q10

# Diagnostics plotter (per-sweep): writes <tag>/figures/diagnostics/
python3 scripts/plot_paper_sweep.py --diag-tag 2026-05-24-a-ssd

# Refresh plotters
python3 scripts/plot_refresh_lsm_vs_btree.py --diagram refresh_lsm_vs_btree
python3 scripts/plot_refresh_sales.py --diagram refresh_sales
```

The plotter and analyzer never error on missing data — incomplete cells
render as "no data" panels or gaps in lines. Safe to run mid-sweep.

Paper diagrams are never rebuilt automatically from a sweep — they are a
deliberate, post-hoc step. A sweep creates `<tag>/summary/` CSVs and
its own per-tag diagnostics; paper figures are rebuilt by editing
`diagrams.yaml` to point at the new tag and running the plotter:

```bash
$EDITOR paper-data/diagrams.yaml   # update sources: entry
python3 scripts/plot_paper_sweep.py --diagram <name>
```

`experiments/run_paper_sweep.sh` (parent repo) invokes the analyzer and
the diagnostics plotter (`--diag-tag`) after a sweep; it never rebuilds
paper diagrams automatically — that decoupling is intentional.

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
- Adding a figure: write a builder, register it in `BUILDERS`, add a YAML entry in `diagrams.yaml`, add a catalog row in `PLOTTING.md`. See `scripts/PLOTTING.md §Adding a new figure` for the full recipe.
- The footer joins all source tags with `+`; the primary tag's manifest supplies commit/host. Footer is omitted from paper-mode figures (typesetter doesn't want it).

## What's tracked vs gitignored

- **Tracked**: `summary/*.csv`, `manifest.yaml`, `SWEEP_LOG.md`, `scripts/`, `diagrams.yaml`.
- **Gitignored** (large / regenerable): `<tag>/raw/`, `<tag>/figures/`, `diagrams/`.

When pulling on a fresh machine, summaries + manifest are present but
`raw/`, `figures/`, and `diagrams/` are not. Regenerate paper figures
with `python3 scripts/plot_paper_sweep.py --all`; raw cannot be
reconstructed without re-running the sweep.

## Linked figures in the paper repo

The paper source tree (`../../merged_index_interesting_orderings/figures/`)
references plots via **relative symlinks** into `paper-data/`. All paper
figures now live under the stable `paper-data/diagrams/` directory — the
output stem is the diagram name from `diagrams.yaml`. Re-pointing a
diagram to a new tag only requires editing `diagrams.yaml` and rerunning
the plotter; no symlink changes are needed.

Current links (paper-side → paper-data source):

- `tpch_btree_headline.pdf` → `diagrams/paper_tpch_btree_headline.pdf`
- `tpch_lsm_headline.pdf` → `diagrams/paper_tpch_lsm_headline.pdf`
- `q10.pdf` → `diagrams/paper_q10.pdf`
- `refresh_5L_pair_latency.pdf` → `diagrams/refresh_5L_pair_latency.pdf`
- `refresh_lsm_vs_btree_5L_5H.pdf` → `diagrams/refresh_lsm_vs_btree.pdf`

The following figures are *referenced by `experiments_revised.tex` but
not currently linked* in the paper `figures/` directory — add symlinks
after first regeneration with `--all`:

- `tpch_lsm_headline_hdd.pdf` → `diagrams/paper_tpch_lsm_headline_hdd.pdf`
- `paper_tpch_vanilla.pdf` → `diagrams/paper_tpch_vanilla.pdf`
- `diag_ssd_lsm_sst_path.pdf` → regenerate via `--diag-tag 2026-05-24-a-ssd`, then link from `2026-05-24-a-ssd/figures/diagnostics/diag_ssd_lsm_sst_path_ssd.pdf`

When creating or updating a symlink, use a relative path so the paper
repo stays portable. Verify with `ls -la` on the paper side. The
`diagrams/` dir is gitignored here but the symlinks in the paper repo
are tracked there.

## SWEEP_LOG.md

Hand-off log between hourly background checks. Newest entries on top.
The "Active" section tracks the in-flight tag; sections flip to "Frozen"
when a tag halts. Update the "Latest status" bullet (don't append a
new dated section for every check — overwrite the bullet, keep history
in the bullet list below).
