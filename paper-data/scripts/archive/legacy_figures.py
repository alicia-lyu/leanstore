#!/usr/bin/env python3
"""Archived figure builders from earlier paper-data plotter iterations.

These nine builders were the default-mode output of
``scripts/plot_paper_sweep.py`` before the paper-figures and
diagnostics-explore modes superseded them. They are preserved here
(rather than deleted) for two reasons:

1. The sweep matrix evolves over time — if the paper sweep goes back
   to bg=0 / bg=1 / multi-axis exploration, these builders are useful
   starting points (especially ``fig_contention_dropoff`` and
   ``fig_inversions`` which target sweep-internal QA, not the paper).
2. ``s3_vs_s2_speedup`` heatmap and ``duration_baseline`` bar chart
   are nice ad-hoc tools for spot-checking a new sweep before
   building a paper figure off it.

To use, import the live shared symbols from the parent module and
register any builder in its FIGURE_BUILDERS dict (or call directly):

    from plot_paper_sweep import load_sweep
    from archive.legacy_figures import fig_contention_dropoff
    data = load_sweep("2026-05-18-b", Path("paper-data/2026-05-18-b"))
    fig_contention_dropoff(data)

Note: the live ``plot_paper_sweep`` no longer exports the
``_add_twin_axis`` / ``_add_twin_txps_axis`` helpers nor the
``grouped_bar`` / ``heatmap`` primitives nor the
``DRAM_AXIS_CELLS`` / ``SECONDARY_AXIS_CELLS`` / ``ANCHOR_CELL`` /
``TPCH_BINARIES`` / ``GEO_BINARIES`` constants needed by these
builders. They are re-declared at the top of this file so the
archive stays self-contained.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from plot_paper_sweep import (
    STYLE, STRUCTURE_LABELS, SweepData,
    aggregate_ms_per_query, line_with_iqr, _structure_style,
    _save, _all_or_empty,
)


# Constants formerly in plot_paper_sweep, retired when paper-mode
# replaced the c2/c1/c3/c0 sweep axes with explicit data-size labels.
DRAM_AXIS_CELLS = ["c3", "c0"]              # secondary pinned to 5 GiB
SECONDARY_AXIS_CELLS = ["c2", "c1", "c0"]   # DRAM:sec ratio pinned at 1:5
ANCHOR_CELL = "c0"

TPCH_BINARIES = ["q3_lsm", "q3_btree", "q5_lsm", "q5_btree",
                 "q3i_lsm", "q3i_btree", "q5i_lsm", "q5i_btree"]
GEO_BINARIES = ["geo_lsm", "geo_btree"]

DIAG_PANELS: List[Tuple[str, str, bool]] = [
    ("cpu_llc_miss_per_tx", "LLC misses / TX", True),
    ("bm_free_pct",         "BM free %",      False),
    ("bm_evicted_mib",      "BM evicted MiB", True),
    ("latency_p99_ms",      "P99 latency (ms)", True),
]


# Plot primitives only used by archived builders.

def grouped_bar(ax, df: pd.DataFrame, x_col: str, y_col: str,
                hue_col: str, x_order: Sequence,
                hue_labels: Dict[int, str]) -> None:
    """Grouped bars: one cluster per x, one bar per hue."""
    hues = sorted(df[hue_col].unique())
    n_hue = len(hues)
    width = 0.8 / max(n_hue, 1)
    x_idx = np.arange(len(x_order))
    for i, hue in enumerate(hues):
        sub = df[df[hue_col] == hue].set_index(x_col).reindex(x_order)
        y = sub[y_col].astype(float).values
        offset = (i - (n_hue - 1) / 2) * width
        ax.bar(x_idx + offset, y, width=width,
               color=STYLE["structure_colors"].get(int(hue), "#777"),
               edgecolor="white", linewidth=0.4,
               label=hue_labels.get(int(hue), str(hue)))
    ax.set_xticks(x_idx)
    ax.set_xticklabels(x_order, rotation=20, ha="right")


def heatmap(ax, df: pd.DataFrame, row_col: str, col_col: str, value_col: str,
            row_order: Optional[Sequence] = None,
            col_order: Optional[Sequence] = None,
            cmap: str = "RdBu_r", center: Optional[float] = None,
            fmt: str = "{:.2f}"):
    """Render df as a row × col heatmap with cell annotations."""
    pivot = df.pivot_table(index=row_col, columns=col_col, values=value_col,
                           aggfunc="median")
    if row_order is not None:
        pivot = pivot.reindex(index=[r for r in row_order if r in pivot.index])
    if col_order is not None:
        pivot = pivot.reindex(columns=[c for c in col_order if c in pivot.columns])
    arr = pivot.values.astype(float)
    if center is not None:
        finite = arr[np.isfinite(arr)]
        if finite.size:
            span = max(abs(finite.min() - center), abs(finite.max() - center), 1e-6)
            vmin, vmax = center - span, center + span
        else:
            vmin, vmax = center - 1, center + 1
        im = ax.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax, aspect="auto")
    else:
        im = ax.imshow(arr, cmap=cmap, aspect="auto")
    ax.set_xticks(np.arange(pivot.shape[1]))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right",
                       fontsize=STYLE["tick_label_fontsize"])
    ax.set_yticks(np.arange(pivot.shape[0]))
    ax.set_yticklabels(pivot.index, fontsize=STYLE["tick_label_fontsize"])
    for i in range(arr.shape[0]):
        for j in range(arr.shape[1]):
            v = arr[i, j]
            if np.isnan(v):
                ax.text(j, i, "—", ha="center", va="center",
                        color="#888", fontsize=7)
            else:
                ax.text(j, i, fmt.format(v), ha="center", va="center",
                        color="black", fontsize=7)
    return im


# Twin-axis helpers (1000/y reflection) used to overlay TX/s on a
# ms/query primary axis. The paper figures use seconds and a single
# y-axis, so these are archive-only.

def _add_twin_axis(ax, label: str) -> None:
    ylim = ax.get_ylim()
    if not all(np.isfinite(ylim)) or ylim[0] >= ylim[1]:
        return

    def _safe_invert(y):
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(y > 0, 1000.0 / y, np.nan)
    try:
        secax = ax.secondary_yaxis(
            "right", functions=(_safe_invert, _safe_invert))
        secax.set_ylabel(label, fontsize=STYLE["axis_label_fontsize"])
        secax.tick_params(labelsize=STYLE["tick_label_fontsize"])
    except (ValueError, FloatingPointError):
        pass


def _add_twin_txps_axis(ax) -> None:
    _add_twin_axis(ax, "TX/s")


# ---------------------------------------------------------------------------
# Archived: headline TPC-H (2×4 grid, bg=0/bg=1 lines, TX/s twin axis)
# ---------------------------------------------------------------------------

def _headline_tpch_panel(ax, ms_df: pd.DataFrame, binary: str,
                         cells: Sequence[str], xlabel: str) -> bool:
    sub = ms_df[(ms_df["binary"] == binary) & (ms_df["cell"].isin(cells))]
    if sub.empty:
        _all_or_empty(plt.gcf(), ax, f"{binary}: no data")
        ax.set_title(binary, fontsize=STYLE["title_fontsize"])
        return False
    drew_any = False
    for bg in sorted(sub["bg"].unique()):
        sub_bg = sub[sub["bg"] == bg]
        if sub_bg.empty:
            continue
        line_with_iqr(
            ax, sub_bg, x_col="cell", y_col="ms_median",
            iqr_col="ms_iqr", hue_col="structure",
            x_order=cells, hue_labels=STRUCTURE_LABELS,
            linestyle=STYLE["bg_linestyles"].get(int(bg), "-"),
        )
        drew_any = True
    ax.set_title(binary, fontsize=STYLE["title_fontsize"])
    ax.set_xlabel(xlabel, fontsize=STYLE["axis_label_fontsize"])
    ax.set_ylabel("ms / query", fontsize=STYLE["axis_label_fontsize"])
    ax.set_yscale("log")
    ax.tick_params(labelsize=STYLE["tick_label_fontsize"])
    ax.grid(True, axis="y", alpha=0.25, which="both")
    return drew_any


def fig_headline_tpch(data: SweepData, axis: str) -> Optional[Path]:
    if axis == "dram":
        cells = DRAM_AXIS_CELLS
        xlabel = "cell (secondary pinned ≈ 5 GiB; DRAM varies 0.4 → 1.0 GiB)"
        name = "headline_tpch_vs_dram"
    elif axis == "secondary":
        cells = SECONDARY_AXIS_CELLS
        xlabel = "cell (DRAM:sec pinned 1:5; sec 0.5 → 2 → 5 GiB)"
        name = "headline_tpch_vs_secondary"
    else:
        raise ValueError(axis)
    head = data.headline[data.headline["family"].isin(["vanilla", "tpchi"])
                          & (data.headline["tx"] == "query")]
    ms_df = aggregate_ms_per_query(head,
                                   group_cols=["binary", "cell", "structure", "bg"])
    fig, axes = plt.subplots(2, 4, figsize=STYLE["figsize_grid"])
    panel_has_data = []
    for ax, binary in zip(axes.flat, TPCH_BINARIES):
        panel_has_data.append(_headline_tpch_panel(ax, ms_df, binary, cells, xlabel))
    for ax, has_data in zip(axes.flat, panel_has_data):
        if not has_data:
            continue
        lo, hi = ax.get_ylim()
        if not (np.isfinite(lo) and np.isfinite(hi) and lo > 0 and hi > lo):
            continue
        _add_twin_txps_axis(ax)
    handles, labels = [], []
    seen = set()
    for ax in axes.flat:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in seen:
                handles.append(h); labels.append(l); seen.add(l)
    handles.extend([
        plt.Line2D([], [], color="#444", linestyle="-", label="bg=0 isolated"),
        plt.Line2D([], [], color="#444", linestyle="--", label="bg=1 contention"),
    ])
    labels.extend(["bg=0 isolated", "bg=1 contention"])
    fig.legend(handles, labels, loc="upper center", ncol=min(len(labels), 7),
               fontsize=STYLE["legend_fontsize"],
               bbox_to_anchor=(0.5, 1.02), frameon=False)
    fig.suptitle(f"TPC-H query duration vs {axis} (median ± IQR over 3 reps; lower is better)",
                 fontsize=STYLE["title_fontsize"] + 1, y=1.07)
    return _save(fig, data.figures_root / name, data.footer)[0]


# ---------------------------------------------------------------------------
# Archived: geo faceted by tx type
# ---------------------------------------------------------------------------

def fig_headline_geo(data: SweepData, axis: str) -> Optional[Path]:
    if axis == "dram":
        cells = DRAM_AXIS_CELLS
        xlabel = "cell (secondary pinned, DRAM varies)"
        name = "headline_geo_vs_dram"
    elif axis == "secondary":
        cells = SECONDARY_AXIS_CELLS
        xlabel = "cell (1:5 ratio, secondary varies)"
        name = "headline_geo_vs_secondary"
    else:
        raise ValueError(axis)
    head = data.headline[data.headline["family"] == "geo"]
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head,
        group_cols=["binary", "cell", "structure", "bg", "tx"],
    )
    tx_types = sorted(ms_df["tx"].unique())
    n_rows = len(GEO_BINARIES)
    n_cols = len(tx_types)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(2.2 * n_cols, 2.6 * n_rows),
                             sharex="col")
    if n_rows == 1:
        axes = np.array([axes])
    for i, binary in enumerate(GEO_BINARIES):
        for j, tx in enumerate(tx_types):
            ax = axes[i, j]
            sub = ms_df[(ms_df["binary"] == binary) & (ms_df["tx"] == tx)
                        & (ms_df["cell"].isin(cells))]
            if sub.empty:
                _all_or_empty(fig, ax, "—")
                continue
            for bg in sorted(sub["bg"].unique()):
                sub_bg = sub[sub["bg"] == bg]
                line_with_iqr(ax, sub_bg, x_col="cell",
                              y_col="ms_median",
                              iqr_col="ms_iqr",
                              hue_col="structure",
                              x_order=cells,
                              hue_labels=STRUCTURE_LABELS,
                              linestyle=STYLE["bg_linestyles"].get(int(bg), "-"))
            ax.set_yscale("log")
            if i == 0:
                ax.set_title(tx, fontsize=8)
            if j == 0:
                ax.set_ylabel(f"{binary}\nms / query",
                              fontsize=STYLE["axis_label_fontsize"])
            ax.tick_params(labelsize=6)
            ax.grid(True, axis="y", alpha=0.2, which="both")
    fig.suptitle(f"Geo query duration vs {axis} (per tx type; lower is better)",
                 fontsize=STYLE["title_fontsize"] + 1, y=0.995)
    fig.supxlabel(xlabel, fontsize=STYLE["axis_label_fontsize"])
    handles = [plt.Line2D([], [], color=STYLE["structure_colors"][s],
                          marker=STYLE["structure_markers"][s],
                          label=STRUCTURE_LABELS[s], linewidth=1.4)
               for s in [1, 2, 3, 4]]
    handles.extend([
        plt.Line2D([], [], color="#444", linestyle="-", label="bg=0"),
        plt.Line2D([], [], color="#444", linestyle="--", label="bg=1"),
    ])
    fig.legend(handles=handles, loc="upper center", ncol=6,
               fontsize=STYLE["legend_fontsize"],
               bbox_to_anchor=(0.5, 1.04), frameon=False)
    return _save(fig, data.figures_root / name, data.footer)[0]


# ---------------------------------------------------------------------------
# Archived: contention drop-off, inversions, S3-vs-S2 heatmap, duration baseline
# ---------------------------------------------------------------------------

def fig_contention_dropoff(data: SweepData) -> Optional[Path]:
    head = data.headline[data.headline["family"].isin(["vanilla", "tpchi"])
                         & (data.headline["tx"] == "query")]
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])
    pivot = ms_df.pivot_table(index=["binary", "cell", "structure"],
                              columns="bg", values="ms_median",
                              aggfunc="median")
    if 0 not in pivot.columns or 1 not in pivot.columns:
        return None
    pivot["slowdown"] = pivot[1] / pivot[0]
    pivot = pivot.reset_index()
    binaries = [b for b in TPCH_BINARIES if b in pivot["binary"].unique()]
    cells = [c for c in ["c2", "c1", "c3", "c0"] if c in pivot["cell"].unique()]
    if not binaries or not cells:
        return None
    fig, axes = plt.subplots(2, 4, figsize=STYLE["figsize_grid"], sharey=True)
    for ax, binary in zip(axes.flat, TPCH_BINARIES):
        sub = pivot[pivot["binary"] == binary]
        if sub.empty:
            _all_or_empty(fig, ax, f"{binary}: no data")
            ax.set_title(binary, fontsize=STYLE["title_fontsize"])
            continue
        grouped_bar(ax, sub[["cell", "structure", "slowdown"]], x_col="cell",
                    y_col="slowdown", hue_col="structure", x_order=cells,
                    hue_labels=STRUCTURE_LABELS)
        ax.axhline(1.0, color="#444", linewidth=0.6, linestyle=":",
                   label="bg=0 baseline")
        ax.set_title(binary, fontsize=STYLE["title_fontsize"])
        ax.set_ylabel("slowdown (bg=1 ms / bg=0 ms)",
                      fontsize=STYLE["axis_label_fontsize"])
        ax.set_xlabel("cell", fontsize=STYLE["axis_label_fontsize"])
        ax.tick_params(labelsize=STYLE["tick_label_fontsize"])
        ax.grid(True, axis="y", alpha=0.25)
        ax.set_ylim(0, max(2.0, ax.get_ylim()[1]))
    fig.suptitle("Contention slowdown: query duration under bg=1 relative to bg=0 (larger = worse)",
                 fontsize=STYLE["title_fontsize"] + 1, y=1.02)
    handles = [plt.Rectangle((0, 0), 1, 1,
                             color=STYLE["structure_colors"][s])
               for s in [1, 2, 3, 4]]
    fig.legend(handles=handles, labels=[STRUCTURE_LABELS[s] for s in [1, 2, 3, 4]],
               loc="upper center", ncol=4,
               fontsize=STYLE["legend_fontsize"],
               bbox_to_anchor=(0.5, 1.06), frameon=False)
    return _save(fig, data.figures_root / "contention_dropoff", data.footer)[0]


def fig_inversions(data: SweepData) -> Optional[Path]:
    inv = data.inversions
    fig, ax = plt.subplots(figsize=STYLE["figsize_single"])
    if inv.empty:
        _all_or_empty(fig, ax, "no S3-vs-S2 inversions flagged\n"
                               "(S3 ≥ 0.95 × S2 in every rep)")
        ax.set_title("S3 vs S2 inversion report",
                     fontsize=STYLE["title_fontsize"])
        return _save(fig, data.figures_root / "inversions", data.footer)[0]
    inv = inv.copy()
    inv["label"] = inv["binary"].astype(str) + "/" + inv["cell"].astype(str) \
                   + "/bg" + inv["bg"].astype(str) + "/r" + inv["rep"].astype(str) \
                   + "/" + inv["tx"].astype(str)
    inv["ms_slowdown"] = inv["s2_tx_per_s"].astype(float) / inv["s3_tx_per_s"].astype(float)
    inv = inv.sort_values("ms_slowdown", ascending=False)
    colors = ["#c0392b" if r > 1.25 else ("#e67e22" if r > 1.10 else "#f1c40f")
              for r in inv["ms_slowdown"]]
    ax.barh(inv["label"], inv["ms_slowdown"], color=colors, edgecolor="white")
    ax.axvline(1.0, color="#27ae60", linewidth=0.8, linestyle="--",
               label="S3 == S2 ms")
    ax.axvline(1 / 0.95, color="#888", linewidth=0.5, linestyle=":",
               label="threshold (S3 ≥ 5% slower)")
    ax.set_xlabel("S3 ms / S2 ms  (>1 = S3 slower = inversion)",
                  fontsize=STYLE["axis_label_fontsize"])
    ax.set_title(f"S3 vs S2 inversions ({len(inv)} flagged)",
                 fontsize=STYLE["title_fontsize"])
    ax.tick_params(labelsize=6)
    ax.legend(fontsize=STYLE["legend_fontsize"], loc="lower right")
    ax.grid(True, axis="x", alpha=0.25)
    return _save(fig, data.figures_root / "inversions", data.footer)[0]


def fig_s3_vs_s2_speedup(data: SweepData) -> Optional[Path]:
    head = data.headline[(data.headline["family"].isin(["vanilla", "tpchi"]))
                         & (data.headline["tx"] == "query")
                         & (data.headline["bg"] == 0)
                         & (data.headline["structure"].isin([2, 3]))]
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure"])
    pivot = ms_df.pivot_table(index="binary", columns=["cell", "structure"],
                              values="ms_median", aggfunc="median")
    rows = []
    for binary in pivot.index:
        for cell in sorted({c for c, _ in pivot.columns}):
            try:
                s2 = pivot.loc[binary, (cell, 2)]
                s3 = pivot.loc[binary, (cell, 3)]
            except KeyError:
                continue
            if pd.isna(s2) or pd.isna(s3) or s3 <= 0:
                continue
            rows.append({"binary": binary, "cell": cell, "speedup": s2 / s3})
    if not rows:
        return None
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(7.5, 5.0))
    binaries = [b for b in TPCH_BINARIES if b in df["binary"].unique()]
    cells = [c for c in ["c2", "c1", "c3", "c0"] if c in df["cell"].unique()]
    heatmap(ax, df, row_col="binary", col_col="cell", value_col="speedup",
            row_order=binaries, col_order=cells,
            cmap="RdBu_r", center=1.0, fmt="{:.2f}×")
    ax.set_title("S3 speedup over S2 in ms (bg=0; > 1.00× favors S3)",
                 fontsize=STYLE["title_fontsize"])
    return _save(fig, data.figures_root / "s3_vs_s2_speedup", data.footer)[0]


def fig_duration_baseline(data: SweepData) -> Optional[Path]:
    stats = data.stats[(data.stats["family"].isin(["vanilla", "tpchi"]))
                       & (data.stats["cell"] == ANCHOR_CELL)
                       & (data.stats["bg"] == 0)
                       & (data.stats["tx"] == "query")]
    if stats.empty:
        msg = f"no rows for cell {ANCHOR_CELL} bg=0 yet"
        fig, ax = plt.subplots(figsize=STYLE["figsize_single"])
        _all_or_empty(fig, ax, msg)
        ax.set_title(f"Duration baseline at {ANCHOR_CELL}",
                     fontsize=STYLE["title_fontsize"])
        return _save(fig, data.figures_root / "duration_baseline", data.footer)[0]
    stats = stats.copy()
    stats["ms_per_query"] = 1000.0 / stats["tx_per_s_median"].replace(0, np.nan)
    binaries = [b for b in TPCH_BINARIES if b in stats["binary"].unique()]
    fig, ax = plt.subplots(figsize=(9.0, 4.5))
    grouped_bar(ax, stats, x_col="binary", y_col="ms_per_query",
                hue_col="structure", x_order=binaries,
                hue_labels=STRUCTURE_LABELS)
    ax.set_yscale("log")
    ax.set_ylabel("ms / query (log scale)",
                  fontsize=STYLE["axis_label_fontsize"])
    ax.set_xlabel(f"binary (anchor cell {ANCHOR_CELL}, bg=0)",
                  fontsize=STYLE["axis_label_fontsize"])
    ax.set_title("Reference: absolute query duration at anchor cell",
                 fontsize=STYLE["title_fontsize"])
    ax.tick_params(labelsize=STYLE["tick_label_fontsize"])
    ax.legend(fontsize=STYLE["legend_fontsize"], loc="upper left")
    ax.grid(True, axis="y", alpha=0.25, which="both")
    return _save(fig, data.figures_root / "duration_baseline", data.footer)[0]


def fig_diagnostics(data: SweepData) -> Optional[Path]:
    diag = data.diagnostics
    if diag.empty:
        return None
    diag = diag[diag["family"].isin(["vanilla", "tpchi"])
                & (diag["bg"] == 0) & (diag["tx"] == "query")]
    if diag.empty:
        return None
    agg = (diag.groupby(["binary", "cell", "structure"])
               .median(numeric_only=True).reset_index())
    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.0))
    cells_present = sorted(agg["cell"].unique(),
                           key=lambda c: ["c2", "c1", "c3", "c0"].index(c)
                           if c in ["c2", "c1", "c3", "c0"] else 99)
    for ax, (col, ylabel, logy) in zip(axes.flat, DIAG_PANELS):
        if col not in agg.columns:
            _all_or_empty(fig, ax, f"{col} not in diagnostics")
            continue
        sub = agg[["binary", "cell", "structure", col]].copy()
        sub = sub.dropna(subset=[col])
        if sub.empty:
            _all_or_empty(fig, ax, f"{col}: no data")
            ax.set_title(ylabel, fontsize=STYLE["title_fontsize"])
            continue
        med = (sub.groupby(["cell", "structure"])
                   .median(numeric_only=True).reset_index())
        for struct in sorted(med["structure"].unique()):
            s = med[med["structure"] == struct].set_index("cell").reindex(cells_present)
            ax.plot(np.arange(len(cells_present)), s[col].values,
                    label=STRUCTURE_LABELS.get(int(struct), str(struct)),
                    **_structure_style(int(struct)))
        ax.set_xticks(np.arange(len(cells_present)))
        ax.set_xticklabels(cells_present)
        ax.set_xlabel("cell", fontsize=STYLE["axis_label_fontsize"])
        ax.set_ylabel(ylabel, fontsize=STYLE["axis_label_fontsize"])
        ax.tick_params(labelsize=STYLE["tick_label_fontsize"])
        if logy:
            ax.set_yscale("log")
        ax.grid(True, alpha=0.25, which="both")
        ax.set_title(ylabel, fontsize=STYLE["title_fontsize"])
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=min(len(labels), 5),
               fontsize=STYLE["legend_fontsize"],
               bbox_to_anchor=(0.5, 1.03), frameon=False)
    fig.suptitle("Diagnostics attribution (median across binaries, bg=0)",
                 fontsize=STYLE["title_fontsize"] + 1, y=1.0)
    return _save(fig, data.figures_root / "diagnostics_attribution",
                 data.footer)[0]
