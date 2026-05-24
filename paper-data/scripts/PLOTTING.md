# Paper-data plotter

`scripts/plot_paper_sweep.py` reads the four summary CSVs emitted by
`scripts/analyze_paper_sweep.py` and writes a directory of paper-ready
figures (PDF + PNG sibling) under `paper-data/<tag>/figures/`.

## Run

```bash
# Default mode = paper-figures: 3 typeset-ready figures under
# figures/paper/ (1×4 TPC-H btree row, 1×4 TPC-H lsm row, 2×3
# condensed geo grid). This is what the experiments/run_paper_sweep.sh
# runner invokes; the no-flag form is fine.
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --mode paper-figures

# Diagnostics-explore mode: 6-panel metric grids + diagnostics_paper.csv
# (scratch outputs under figures/diagnostics/; pick 1-2 panels for the paper)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --mode diagnostics-explore

# Subset (overrides --mode)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b \
    --figures paper_tpch_btree,paper_tpch_lsm

# Alternate format
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b --format svg

# Different root (rarely needed)
python3 scripts/plot_paper_sweep.py --tag 2026-05-18-b \
    --root /tmp/some-other-paper-data/2026-05-18-b
```

## Legacy builders (archived)

The pre-paper-revision sweep-diagnostic builders (2×4 TPC-H headline,
faceted geo, contention drop-off, inversion bar chart, S3-vs-S2
heatmap, duration-baseline bars, 2×2 diagnostics grid) have moved to
`scripts/archive/legacy_figures.py`. See `scripts/archive/README.md`
for when to revive them and how to wire one back into the CLI.

## Dependencies

`matplotlib`, `pandas`, `pyyaml`, `numpy`. On Ubuntu/Debian:
```bash
sudo apt-get install python3-matplotlib python3-pandas python3-yaml python3-numpy
```
Versions tested on the CloudLab node: matplotlib 3.5.1, pandas 1.3.5,
numpy 1.21.5.

## Disk media

Every paper-figure basename gets a `_ssd` or `_hdd` suffix when the
source tag's summary CSVs carry a `disk` column (added by
`scripts/mark_disk_media.py`). Tags without the column keep their
bare names. **All pre-2026-05-24 tags are HDD-measured** — the
`/mnt/ssd` mount on the experiment node was actually a SAS spindle
until then; `mark_disk_media.py` retroactively stamps that fact on the
CSVs so HDD baselines and SSD reruns can't be confused.

## Figure catalog

| Filename | Reads | What it shows |
|---|---|---|
| `paper/paper_tpch_btree_headline.pdf` | headline.csv | 1×4 row (q3, q5, q3i, q5i btree). bg=2 only, structures S1–S4. **5L cell only** (largest data, low memory pressure) — each panel is a single group of 4 coloured bars (S4/S1/S2/S3 in legend order). Y = seconds/query (log). Shared legend above. Designed to stack vertically with the lsm sibling. |
| `paper/paper_tpch_lsm_headline.pdf` | headline.csv | Same shape and geometry as the btree sibling, for the lsm backend. Legend omitted — parent doc uses the btree figure's legend. |
| `paper/paper_tpch_btree_memory_pressure.pdf` | headline.csv | 1×2 grouped-bar figure (q5, q5i btree). bg=2, S1–S4. X = 3 cells (`2`, `5L`, `5H`) showing both scale-up (2 → 5L) and pressure (5L → 5H). Y = seconds/query (log). Companion to the headline figure when both axes need to be shown. **Politely skipped** on c0-only tags (e.g. the SSD rerun `2026-05-24-a-ssd`). |
| `paper/paper_tpch_lsm_memory_pressure.pdf` | headline.csv | Same as the btree sibling, for the lsm backend. Legend omitted. Same c0-only skip rule. |
| `paper/paper_geo_condensed.pdf` | headline.csv | 2×3 grid: rows = geo backend (btree, lsm), cols = tx pattern (join-nsc, mixed-nsc, distinct-nsc) at depth nsc. Same data-size axis and colour map as the TPC-H figures. **Politely skipped** on tags without `geo_btree`/`geo_lsm` binaries. |
| `diagnostics/diag_btree_llc_miss.pdf` | diagnostics.csv | 1×4 row (q3, q5, q3i, q5i btree). Y = `cpu_llc_miss_per_tx` (log). Strongest btree attribution: cleanly ranks `merged_idx` ≈ `mat_view` < `mj` < `hj`, supporting the headline `merged ≈ view` claim by underlying cache behaviour. **Main-text candidate**. |
| `diagnostics/diag_btree_bm_rounds.pdf` | diagnostics.csv | Same shape, Y = `bm_rounds`. Noisy at n=3 — lines cross. Useful for spot-check, not main-text. |
| `diagnostics/diag_btree_dt_split.pdf` | diagnostics.csv | Same shape, Y = `dt_struct_split`. Mostly zero on read-only queries (write path not exercised) — kept for future write-workload runs. |
| `diagnostics/diag_lsm_sst_read.pdf` | diagnostics.csv | 1×4 row (lsm). Y = `sst_read_us_per_tx` (log). Hash-join 10-100× above the rest; merged sits slightly above view on q3i/q5i — **attributes the Q3I lsm headline anomaly** (§5.5 candidate). |
| `diagnostics/diag_lsm_sst_compaction.pdf` | diagnostics.csv | Same shape, Y = `sst_compaction_us`. Mostly flat across structures; non-distinguishing. |
| `diagnostics/diag_lsm_cpu_cycles.pdf` | diagnostics.csv | Same shape, Y = `cpu_cycles_per_tx`. The only fully-populated CPU column on lsm rows. |
| `summary/diagnostics_paper.csv` | diagnostics.csv | Side effect of diagnostics-explore mode. One row per (binary, cell, structure) with every metric we plot. Backend-specific metrics are NaN for the other backend's rows — that's the signal. |

Backend-specific note: `diagnostics.csv` carries disjoint signal sets per backend (see `paper-data/CLAUDE.md`). btree rows have the full LeanStore counter family (`cpu_*`, `bm_*`, `cr_*`, `dt_*`); lsm rows have CPU cycles + utilisation + RocksDB SST timing surrogates (`sst_read_us_per_tx`, `sst_write_us_per_tx`, `sst_compaction_us`). The per-query diagnostic figures above pick the columns that actually populate per backend.

**Y-axis convention:** primary axis is **seconds/query** (lower is
better) on a log scale; TPC-H queries here run from ~5 s up to hours
at large SF/contention. Aggregates (median + IQR) are computed per-rep
in ms-space and converted to seconds at plot time — they are NOT
derived from `stats.csv`'s `tx_per_s` aggregates, since 1000/x is
non-linear and would distort the IQR. The plotter aggregates from
`headline.csv` directly via `aggregate_ms_per_query()` for this reason.

Diagnostics-mode figures keep a footer line with `tag`, `commit`,
`host`, and the cell list from `manifest.yaml`. Paper-mode figures
omit the footer (typesetter doesn't want it).

## Standalone scripts (not in the sweep harness)

| Script | Reads | What it shows |
|---|---|---|
| `plot_refresh_sales.py --tag <tag>` | `<tag>/summary/refresh_sales_rf_throughput.csv` | 1×2 grouped bar (x=backend, series=structure) of **µs / RF1 insert** (log, lower-is-better). Left = hot window (20–25 s), right = whole-run avg. Picks up an optional `dbtoaster_rf_throughput.csv` sibling and adds a 5th series when present. Writes `figures/paper/refresh_sales_rf1_latency.{pdf,png}`. |

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
