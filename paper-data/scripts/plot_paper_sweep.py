#!/usr/bin/env python3
"""Paper-data plotter for the LeanStore sweep.

Consumes the four summary CSVs emitted by ``scripts/analyze_paper_sweep.py``
under ``paper-data/<tag>/summary/`` and produces a directory of paper-ready
figures (PDF + PNG sibling) under ``paper-data/<tag>/figures/``. See
``scripts/PLOTTING.md`` for the figure catalog and design notes.

Schema-driven: every series, color, and label is derived from the CSV
column values, not hardcoded against a particular sweep matrix. Adding
a new figure is a single function added to ``FIGURE_BUILDERS``.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import matplotlib
matplotlib.use("Agg")  # no display required on the experiment host
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# STYLE — one source of truth for color/marker/label conventions.
# Storage-structure color choices keep S3 (the merged-index headline) in
# green so it pops; S1 (baseline) in muted red; S2 (view) in blue; S4 in
# orange; S5 reserved purple. Backend distinguished by linestyle, not color.
# ---------------------------------------------------------------------------

STYLE = {
    "structure_colors": {
        1: "#c0392b",  # S1 base merge join — muted red
        2: "#2980b9",  # S2 materialized view — blue
        3: "#27ae60",  # S3 merged index — green (headline)
        4: "#e67e22",  # S4 hash join — orange
        5: "#8e44ad",  # S5 aCOLI — purple (deferred from paper sweep)
    },
    "structure_markers": {1: "o", 2: "s", 3: "D", 4: "^", 5: "v"},
    "backend_linestyles": {"lsm": "-", "btree": "--"},
    "bg_linestyles": {0: "-", 1: "--"},
    "figsize_single": (6.5, 4.0),
    "figsize_grid": (12.0, 7.0),
    "footer_fontsize": 7,
    "title_fontsize": 11,
    "axis_label_fontsize": 9,
    "tick_label_fontsize": 8,
    "legend_fontsize": 7,
}

STRUCTURE_LABELS = {
    1: "S1 base merge-join",
    2: "S2 pipeline view",
    3: "S3 merged index",
    4: "S4 base hash-join",
    5: "S5 aCOLI MI",
}

CELL_LABELS = {
    "c0": "c0 (DRAM=1.0 / sec=5 GiB)",
    "c1": "c1 (DRAM=0.4 / sec=2 GiB)",
    "c2": "c2 (DRAM=0.1 / sec=0.5 GiB)",
    "c3": "c3 (DRAM=0.4 / sec=5 GiB)",
}

CELL_DRAM_GIB = {"c0": 1.0, "c1": 0.4, "c2": 0.1, "c3": 0.4}
CELL_SECONDARY_GIB = {"c0": 5.0, "c1": 2.0, "c2": 0.5, "c3": 5.0}

# The two paper-axis subsets and the order they appear on the x-axis.
DRAM_AXIS_CELLS = ["c3", "c0"]              # secondary pinned to 5 GiB
SECONDARY_AXIS_CELLS = ["c2", "c1", "c0"]   # DRAM:sec ratio pinned at 1:5

ANCHOR_CELL = "c0"  # used by duration_baseline

# Headline plots present **query duration (ms/query)** as the primary y-axis
# since "how long does each query take?" is the more intuitive lens for
# database benchmarks. TX/s lives on the right twin axis so reviewers can
# still read throughput at a glance. Set to False to swap them back.
TIME_AXIS_DEFAULT = True


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@dataclass
class SweepData:
    tag: str
    summary_root: Path
    figures_root: Path
    headline: pd.DataFrame
    stats: pd.DataFrame
    inversions: pd.DataFrame
    diagnostics: pd.DataFrame
    manifest: Dict[str, str]

    @property
    def footer(self) -> str:
        m = self.manifest
        parts = [f"tag {self.tag}"]
        for k in ("commit_sha", "host", "cells", "families"):
            v = m.get(k)
            if v:
                parts.append(f"{k.split('_')[0]} {v}")
        return "  |  ".join(parts)


def _quantile(vals: pd.Series, p: float) -> float:
    vals = vals.dropna()
    if vals.empty:
        return float("nan")
    return float(vals.quantile(p))


def aggregate_ms_per_query(headline: pd.DataFrame,
                           group_cols: Sequence[str]) -> pd.DataFrame:
    """Aggregate per-rep ms/query (= 1000/tx_per_s) over ``group_cols``.

    Returns ``group_cols + ['ms_median', 'ms_iqr', 'n']``. Median + IQR
    are computed in ms space (not derived from TX/s aggregates) so the
    error bars are correct under the non-linear transform.
    """
    if headline.empty:
        return pd.DataFrame(columns=list(group_cols) + ["ms_median", "ms_iqr", "n"])
    df = headline.copy()
    # ms_per_tx is already in headline.csv (see analyzer), but recompute
    # from tx_per_s defensively in case rows have it blank.
    if "ms_per_tx" not in df.columns or df["ms_per_tx"].isna().any():
        df["ms_per_tx"] = 1000.0 / df["tx_per_s"].replace(0, np.nan)
    grouped = df.groupby(list(group_cols), dropna=False)["ms_per_tx"]
    out = grouped.agg(
        ms_median="median",
        n="count",
    ).reset_index()
    iqr = grouped.apply(lambda s: _quantile(s, 0.75) - _quantile(s, 0.25)).rename("ms_iqr")
    out = out.merge(iqr.reset_index(), on=list(group_cols), how="left")
    return out


def load_sweep(tag: str, root: Path) -> SweepData:
    summary = root / "summary"
    figures = root / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    def _read(name: str) -> pd.DataFrame:
        p = summary / name
        if not p.exists() or p.stat().st_size == 0:
            print(f"[plotter] WARN: {p} missing or empty", file=sys.stderr)
            return pd.DataFrame()
        return pd.read_csv(p)

    headline = _read("headline.csv")
    stats = _read("stats.csv")
    inversions = _read("inversions.csv")
    diagnostics = _read("diagnostics.csv")

    manifest: Dict[str, str] = {}
    mfile = root / "manifest.yaml"
    if mfile.exists():
        try:
            manifest = yaml.safe_load(mfile.read_text()) or {}
        except yaml.YAMLError as e:
            print(f"[plotter] WARN: couldn't parse manifest: {e}", file=sys.stderr)

    return SweepData(tag=tag, summary_root=summary, figures_root=figures,
                     headline=headline, stats=stats, inversions=inversions,
                     diagnostics=diagnostics, manifest=manifest)


# ---------------------------------------------------------------------------
# Plot primitives
# ---------------------------------------------------------------------------

def _structure_style(struct: int) -> Dict[str, object]:
    return {
        "color": STYLE["structure_colors"].get(struct, "#777777"),
        "marker": STYLE["structure_markers"].get(struct, "x"),
        "markersize": 5,
        "linewidth": 1.4,
    }


def line_with_iqr(ax, df: pd.DataFrame, x_col: str, y_col: str,
                  iqr_col: str, hue_col: str, x_order: Sequence,
                  hue_labels: Dict[int, str], linestyle: str = "-") -> None:
    """Plot one line per ``hue_col`` value, with shaded IQR band.

    df is in long form with columns x_col, y_col, iqr_col, hue_col.
    x_order pins the categorical x-axis order; missing values render as
    a gap rather than a fabricated zero.
    """
    for hue in sorted(df[hue_col].unique()):
        sub = df[df[hue_col] == hue].set_index(x_col).reindex(x_order)
        style = _structure_style(int(hue))
        style["linestyle"] = linestyle
        y = sub[y_col].astype(float).values
        iqr = sub[iqr_col].astype(float).values
        x = np.arange(len(x_order))
        ax.plot(x, y, label=hue_labels.get(int(hue), str(hue)), **style)
        # IQR band — only where we have both y and iqr.
        mask = ~np.isnan(y) & ~np.isnan(iqr)
        if mask.any():
            lower = y - iqr / 2
            upper = y + iqr / 2
            ax.fill_between(x[mask], lower[mask], upper[mask],
                            color=style["color"], alpha=0.12, linewidth=0)
    ax.set_xticks(np.arange(len(x_order)))
    ax.set_xticklabels(x_order)


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
            fmt: str = "{:.2f}") -> None:
    """Render df as a row × col heatmap with cell annotations."""
    pivot = df.pivot_table(index=row_col, columns=col_col, values=value_col,
                           aggfunc="median")
    if row_order is not None:
        pivot = pivot.reindex(index=[r for r in row_order if r in pivot.index])
    if col_order is not None:
        pivot = pivot.reindex(columns=[c for c in col_order if c in pivot.columns])
    arr = pivot.values.astype(float)
    if center is not None:
        # Center the colormap on `center` (e.g. 1.0 for ratios).
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
    # Cell annotations
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


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _add_twin_axis(ax, label: str) -> None:
    """Add a right-hand y-axis derived by 1000/y from the primary axis.

    Used to overlay TX/s on a ms/query primary axis (or vice versa,
    since 1000/x is its own inverse). Silently skips when the primary
    axis has no finite range — matplotlib's secondary_yaxis can't
    compute its forward/inverse on NaN bounds.
    """
    ylim = ax.get_ylim()
    if not all(np.isfinite(ylim)) or ylim[0] >= ylim[1]:
        return
    def _safe_invert(y):
        with np.errstate(divide="ignore", invalid="ignore"):
            return np.where(y > 0, 1000.0 / y, np.nan)
    try:
        secax = ax.secondary_yaxis(
            "right",
            functions=(_safe_invert, _safe_invert),
        )
        secax.set_ylabel(label, fontsize=STYLE["axis_label_fontsize"])
        secax.tick_params(labelsize=STYLE["tick_label_fontsize"])
    except (ValueError, FloatingPointError):
        # Axis transformation rejected — silently drop the twin axis.
        pass


# Back-compat alias (kept so callers that imported the old name keep
# working; new code should use _add_twin_axis).
def _twin_ms_axis(ax) -> None:
    _add_twin_axis(ax, "ms/query")


def _add_twin_txps_axis(ax) -> None:
    _add_twin_axis(ax, "TX/s")


def _save(fig: plt.Figure, dest: Path, footer: str, fmt: str = "pdf",
          include_footer: bool = True) -> List[Path]:
    """Save fig with a discreet footer line. Always emits PDF + PNG sibling
    when fmt='pdf'. Otherwise emits only the requested format.

    Set ``include_footer=False`` for paper-mode output where the footer
    would just be noise in a typeset figure.
    """
    if include_footer:
        fig.text(0.5, 0.005, footer, ha="center", va="bottom",
                 fontsize=STYLE["footer_fontsize"], color="#666")
        fig.tight_layout(rect=[0, 0.025, 1, 1])
    else:
        fig.tight_layout()
    dest.parent.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []
    primary = dest.with_suffix(f".{fmt}")
    fig.savefig(primary, format=fmt, bbox_inches="tight")
    written.append(primary)
    if fmt == "pdf":
        png = dest.with_suffix(".png")
        fig.savefig(png, format="png", dpi=180, bbox_inches="tight")
        written.append(png)
    plt.close(fig)
    return written


def _all_or_empty(fig: plt.Figure, ax, msg: str) -> None:
    """Render `msg` as a centered note when there's nothing to plot."""
    ax.text(0.5, 0.5, msg, ha="center", va="center",
            transform=ax.transAxes, fontsize=10, color="#666")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_visible(False)


# ---------------------------------------------------------------------------
# Figure builders — TPC-H headline plots
# ---------------------------------------------------------------------------

TPCH_BINARIES = ["q3_lsm", "q3_btree", "q5_lsm", "q5_btree",
                 "q3i_lsm", "q3i_btree", "q5i_lsm", "q5i_btree"]
GEO_BINARIES = ["geo_lsm", "geo_btree"]


def _headline_tpch_panel(ax, ms_df: pd.DataFrame, binary: str,
                         cells: Sequence[str], xlabel: str) -> bool:
    """Plot one panel of the headline TPC-H grid. Returns True if any data.

    Primary axis = ms/query (lower is better). Twin axis (added later
    by the figure builder once layout is finalized) is TX/s.
    """
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
    ax.set_yscale("log")  # ms spans 2-3 orders across structures + backends
    ax.tick_params(labelsize=STYLE["tick_label_fontsize"])
    ax.grid(True, axis="y", alpha=0.25, which="both")
    # Twin TX/s axis is added later in fig_headline_tpch, after all
    # panels' y-limits are finalized — needed because matplotlib's
    # secondary_yaxis evaluates limits at layout time and chokes on NaN
    # bounds inherited from empty sibling panels.
    return drew_any


def fig_headline_tpch(data: SweepData, axis: str) -> Optional[Path]:
    """axis: 'dram' or 'secondary'."""
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
    # Aggregate ms_per_tx from headline.csv (per-rep) so the IQR is
    # correct in ms-space (not derived from tx_per_s IQR via a non-linear
    # transform).
    head = data.headline[data.headline["family"].isin(["vanilla", "tpchi"])
                          & (data.headline["tx"] == "query")]
    ms_df = aggregate_ms_per_query(head,
                                   group_cols=["binary", "cell", "structure", "bg"])
    # No sharey: a sibling panel with no data produces NaN y-limits that
    # poison the row-shared axis (and any secondary_yaxis on the row).
    fig, axes = plt.subplots(2, 4, figsize=STYLE["figsize_grid"])
    panel_has_data = []
    for ax, binary in zip(axes.flat, TPCH_BINARIES):
        panel_has_data.append(_headline_tpch_panel(ax, ms_df, binary, cells, xlabel))
    # Add the right-hand TX/s twin axis on each populated panel (primary
    # is ms/query, secondary is its 1000/y reflection). Skip when the
    # primary y-range collapses to non-positive bounds.
    for ax, has_data in zip(axes.flat, panel_has_data):
        if not has_data:
            continue
        lo, hi = ax.get_ylim()
        if not (np.isfinite(lo) and np.isfinite(hi) and lo > 0 and hi > lo):
            continue
        _add_twin_txps_axis(ax)
    # one shared legend at the top
    handles, labels = [], []
    seen = set()
    for ax in axes.flat:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in seen:
                handles.append(h); labels.append(l); seen.add(l)
    # add bg style legend explicitly
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
# Figure builders — Geo headline plots (faceted by tx type)
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
    # Grid: rows = binary (lsm/btree), cols = tx type
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
# Figure builders — contention drop-off, inversions, diagnostics, headline
# heatmap, duration baseline
# ---------------------------------------------------------------------------

def fig_contention_dropoff(data: SweepData) -> Optional[Path]:
    """Per (binary, cell, structure): bg=1 ms / bg=0 ms slowdown.
    Values > 1 = each query takes longer under contention; larger = worse."""
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
    fig.legend(handles=handles, labels=[STRUCTURE_LABELS[s] for s in [1,2,3,4]],
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
    # Present as ms slowdown: ms_s3 / ms_s2 = tx_per_s_s2 / tx_per_s_s3 = 1/ratio.
    # Values > 1 = S3 SLOWER than S2 (which is the inversion).
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
    """Heatmap of S2 / S3 ms ratio (= S3 speedup over S2 in ms terms).
    Values > 1.00× mean S3 is faster (shorter ms/query) than S2."""
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
    # S3 speedup over S2 in ms terms = S2_ms / S3_ms. > 1.00× → S3 faster.
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


# ---------------------------------------------------------------------------
# Figure builders — diagnostics attribution
# ---------------------------------------------------------------------------

DIAG_PANELS: List[Tuple[str, str, bool]] = [
    # (column, ylabel, log scale)
    ("cpu_llc_miss_per_tx", "LLC misses / TX", True),
    ("bm_free_pct",         "BM free %",      False),
    ("bm_evicted_mib",      "BM evicted MiB", True),
    ("latency_p99_ms",      "P99 latency (ms)", True),
]


def fig_diagnostics(data: SweepData) -> Optional[Path]:
    diag = data.diagnostics
    if diag.empty:
        return None
    diag = diag[diag["family"].isin(["vanilla", "tpchi"])
                & (diag["bg"] == 0) & (diag["tx"] == "query")]
    if diag.empty:
        return None
    # Aggregate to median per (binary, cell, structure)
    agg = (diag.groupby(["binary", "cell", "structure"])
               .median(numeric_only=True).reset_index())
    fig, axes = plt.subplots(2, 2, figsize=(11.0, 7.0))
    cells_present = sorted(agg["cell"].unique(),
                           key=lambda c: ["c2","c1","c3","c0"].index(c)
                           if c in ["c2","c1","c3","c0"] else 99)
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
        # Plot one line per structure, x = cell, faceted across binaries
        # by averaging structures over binaries (median again). Cleaner
        # than 8 facets in one panel.
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


# ---------------------------------------------------------------------------
# Paper-mode builders — typeset-ready figures for the revision
# ---------------------------------------------------------------------------
#
# Paper-mode design choices (differ from default builders above):
# - bg=2 only (the paper's headline contention cohort; bg=0/1 are
#   sweep-internal calibration knobs and don't belong in a typeset figure)
# - structures 1-4 only (S5 deferred from paper per PLAYBOOK)
# - secondary-axis cells (c2 → c1 → c0) — scale-up axis is the headline
# - no footer (typesetter doesn't want it)
# - compact figure size for single-column placement, sharey across panels
# - written under figures/paper/ so they don't mix with the default sweep
#   diagnostic outputs

PAPER_TPCH_QUERIES = ["q3", "q5", "q3i", "q5i"]   # left-to-right panel order
PAPER_HEADLINE_BG = 2                              # paper's contention cohort
PAPER_STRUCTURES = [1, 2, 3, 4]                    # S5 deferred
# Legend order requested by the writer: worst → best baseline → winner.
# Hash-join (S4) first because it's the slowest and most visually
# obvious in the plots; merged index (S3) last because it's the
# headline. Decouples legend ordering from internal numeric order.
PAPER_LEGEND_ORDER = [4, 1, 2, 3]
# Sweep -b runs c1, c3, c0 (c2 was dropped). Axis order is
# 2 → 5L → 5H: scale data first under steady (low) memory pressure,
# then crank pressure up at fixed data size. Lets the reader read
# the "scale-up" effect first and the "pressure" effect second.
PAPER_CELLS = ["c1", "c0", "c3"]
# Tick labels show secondary index size in GiB, with H/L suffix on
# the two 5 GiB cells to distinguish low vs high memory pressure.
# Full label "data size (GiB), H/L = memory pressure" is left to the
# LaTeX caption per user request (keeps the figure box visually clean).
PAPER_CELL_TICK = {
    "c1": "2",      # sec 2 GiB, DRAM 0.4 — baseline (low pressure)
    "c0": "5L",     # sec 5 GiB, DRAM 1.0 — low memory pressure
    "c3": "5H",     # sec 5 GiB, DRAM 0.4 — high memory pressure
}


def _apply_paper_overlap_style(ax) -> None:
    """Patch lines in-place: same marker for all structures, alpha < 1 so
    overlapping lines stay visible. The colour map (red / blue / green /
    orange) already distinguishes structures; the per-structure marker
    shape duplicates that signal and adds visual clutter when lines
    coincide. One marker + transparency makes overlap explicit."""
    for line in ax.get_lines():
        line.set_marker("o")
        line.set_markersize(4)
        line.set_alpha(0.7)
    # IQR bands rendered via fill_between also get alpha-blended.
    for coll in ax.collections:
        coll.set_alpha(0.18)


def _paper_panel(ax, ms_df: pd.DataFrame, binary: str,
                 cells: Sequence[str], show_ylabel: bool) -> bool:
    """One compact panel of the paper TPC-H row. bg=2 only, S1-S4 only."""
    sub = ms_df[(ms_df["binary"] == binary) & (ms_df["cell"].isin(cells))
                & (ms_df["bg"] == PAPER_HEADLINE_BG)
                & (ms_df["structure"].isin(PAPER_STRUCTURES))]
    if sub.empty:
        _all_or_empty(plt.gcf(), ax, "—")
        ax.set_title(binary.replace("_lsm", "").replace("_btree", ""),
                     fontsize=8)
        return False
    line_with_iqr(
        ax, sub, x_col="cell", y_col="ms_median", iqr_col="ms_iqr",
        hue_col="structure", x_order=cells,
        hue_labels=STRUCTURE_LABELS, linestyle="-",
    )
    ax.set_title(binary.replace("_lsm", "").replace("_btree", ""),
                 fontsize=8)
    if show_ylabel:
        ax.set_ylabel("ms / query", fontsize=7)
    # No per-panel xlabel — set once via fig.supxlabel after layout.
    ax.set_xticks(np.arange(len(cells)))
    ax.set_xticklabels([PAPER_CELL_TICK.get(c, c) for c in cells],
                       fontsize=7)
    ax.set_yscale("log")
    ax.tick_params(axis="y", labelsize=6)
    ax.grid(False)  # no log-scale gridlines per user request
    if ax.get_legend():
        ax.get_legend().remove()
    _apply_paper_overlap_style(ax)
    return True


def fig_paper_tpch_row(data: SweepData, backend: str,
                       include_legend: bool) -> Optional[Path]:
    """1×4 row of (q3, q5, q3i, q5i) for one backend, single-column-wide.

    The btree and lsm figures are designed to be the same width and
    panel geometry so the writer can stack them vertically in LaTeX and
    have axes line up panel-by-panel. ``include_legend=True`` attaches a
    single compact legend above the figure; the sibling figure should
    set it False so vertical space isn't duplicated.
    """
    assert backend in ("btree", "lsm")
    head = data.headline[data.headline["family"].isin(["vanilla", "tpchi"])
                         & (data.headline["backend"] == backend)
                         & (data.headline["tx"] == "query")]
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])
    binaries = [f"{q}_{backend}" for q in PAPER_TPCH_QUERIES]
    # Single column ≈ 3.4"; 4 panels at ~0.85" each + slight margin.
    # Height ~1.7" gives readable panels without dwarfing surrounding text.
    fig, axes = plt.subplots(1, 4, figsize=(3.5, 1.75), sharey=True)
    has_any = False
    for j, binary in enumerate(binaries):
        drew = _paper_panel(axes[j], ms_df, binary, PAPER_CELLS,
                            show_ylabel=(j == 0))
        has_any = has_any or drew
    if not has_any:
        plt.close(fig)
        return None
    if include_legend:
        handles = [plt.Line2D([], [], color=STYLE["structure_colors"][s],
                              marker="o", markersize=4, linewidth=1.2,
                              alpha=0.7,
                              label=STRUCTURE_LABELS[s].split(" ", 1)[1])
                   for s in PAPER_LEGEND_ORDER]
        fig.legend(handles=handles, loc="upper center", ncol=4,
                   fontsize=6, bbox_to_anchor=(0.5, 1.08),
                   frameon=False, columnspacing=1.2, handletextpad=0.4)
    name = f"paper_tpch_{backend}_headline"
    dest = data.figures_root / "paper" / name
    return _save(fig, dest, data.footer, include_footer=False)[0]


PAPER_GEO_TX = ["join-nsc", "mixed-nsc", "distinct-nsc"]


def fig_paper_geo_condensed(data: SweepData) -> Optional[Path]:
    """Condensed geo: join-nsc, mixed-nsc, distinct-nsc — nsc depth only.

    2×3 grid:
        rows  = backend (geo_btree on top, geo_lsm below)
        cols  = tx pattern (join, mixed, distinct) — all at depth nsc
    Same secondary-axis cells (c2, c1, c0), same colour map as the
    TPC-H row figures so the visual vocabulary carries between sections.
    The user's preference: keep all three patterns (so the join /
    aggregation / distinct-aggregation progression is visible) but stay
    at nsc depth to keep the figure compact and on-message.
    """
    head = data.headline[(data.headline["family"] == "geo")
                         & (data.headline["tx"].isin(PAPER_GEO_TX))
                         & (data.headline["bg"] == PAPER_HEADLINE_BG)
                         & (data.headline["structure"].isin(PAPER_STRUCTURES))]
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg", "tx"])
    fig, axes = plt.subplots(2, 3, figsize=(5.2, 3.0), sharey="row",
                             sharex="col")
    tx_order = PAPER_GEO_TX
    backend_order = [("geo_btree", "btree"), ("geo_lsm", "lsm")]
    drew_any = False
    for i, (binary, backend) in enumerate(backend_order):
        for j, tx in enumerate(tx_order):
            ax = axes[i, j]
            sub = ms_df[(ms_df["binary"] == binary) & (ms_df["tx"] == tx)
                        & (ms_df["cell"].isin(PAPER_CELLS))]
            if sub.empty:
                _all_or_empty(fig, ax, "—")
                continue
            line_with_iqr(
                ax, sub, x_col="cell", y_col="ms_median",
                iqr_col="ms_iqr", hue_col="structure",
                x_order=PAPER_CELLS, hue_labels=STRUCTURE_LABELS,
                linestyle="-",
            )
            ax.set_yscale("log")
            ax.grid(False)
            if i == 0:
                ax.set_title(tx, fontsize=8)
            if j == 0:
                ax.set_ylabel(f"{backend}\nms / query", fontsize=7)
            if i == len(backend_order) - 1:
                ax.set_xticks(np.arange(len(PAPER_CELLS)))
                ax.set_xticklabels(
                    [PAPER_CELL_TICK.get(c, c) for c in PAPER_CELLS],
                    fontsize=7)
            ax.tick_params(axis="y", labelsize=6)
            if ax.get_legend():
                ax.get_legend().remove()
            _apply_paper_overlap_style(ax)
            drew_any = True
    if not drew_any:
        plt.close(fig)
        return None
    handles = [plt.Line2D([], [], color=STYLE["structure_colors"][s],
                          marker=STYLE["structure_markers"][s],
                          markersize=3, linewidth=1.2,
                          label=STRUCTURE_LABELS[s].split(" ", 1)[1])
               for s in PAPER_STRUCTURES]
    fig.legend(handles=handles, loc="upper center", ncol=4,
               fontsize=6, bbox_to_anchor=(0.5, 1.04),
               frameon=False, columnspacing=1.2, handletextpad=0.4)
    dest = data.figures_root / "paper" / "paper_geo_condensed"
    return _save(fig, dest, data.footer, include_footer=False)[0]


# ---------------------------------------------------------------------------
# Diagnostics exploration mode
# ---------------------------------------------------------------------------
#
# Plot the candidate metric families from §Diagnostics in the plan. Goal:
# scan visually after the run, pick 1-2 panels that earn paper-mode
# treatment. Outputs live under figures/diagnostics/.

DIAG_EXPLORE_FAMILIES: List[Tuple[str, str, str, bool]] = [
    # (panel title, column, ylabel, log)
    ("CPU: LLC misses / TX",   "cpu_llc_miss_per_tx",  "LLC miss / TX",   True),
    ("CPU: cycles / TX",       "cpu_cycles_per_tx",    "cycles / TX",     True),
    ("BM: eviction rounds",    "bm_rounds",            "rounds",          True),
    ("BM: evicted MiB",        "bm_evicted_mib",       "MiB",             True),
    ("TX: restarts",           "cr_restarts",          "restarts",        False),
    ("Tail: P99 latency",      "latency_p99_ms",       "P99 ms",          True),
]


def _diag_filter(diag: pd.DataFrame, binary_filter: Optional[str] = None
                 ) -> pd.DataFrame:
    df = diag[(diag["family"].isin(["vanilla", "tpchi"]))
              & (diag["bg"] == PAPER_HEADLINE_BG)
              & (diag["tx"] == "query")
              & (diag["structure"].isin(PAPER_STRUCTURES))]
    if binary_filter:
        df = df[df["binary"] == binary_filter]
    return df


def _draw_metric_panel(ax, agg: pd.DataFrame, col: str, ylabel: str,
                       logy: bool, cells: Sequence[str]) -> bool:
    if col not in agg.columns:
        _all_or_empty(plt.gcf(), ax, f"{col} not in CSV")
        return False
    sub = agg[["cell", "structure", col]].dropna(subset=[col])
    if sub.empty:
        _all_or_empty(plt.gcf(), ax, f"{col}: no data")
        return False
    for struct in PAPER_STRUCTURES:
        s = sub[sub["structure"] == struct].set_index("cell").reindex(cells)
        y = s[col].astype(float).values
        if np.all(np.isnan(y)):
            continue
        ax.plot(np.arange(len(cells)), y,
                label=STRUCTURE_LABELS[struct].split(" ", 1)[1],
                **_structure_style(struct))
    ax.set_xticks(np.arange(len(cells)))
    ax.set_xticklabels(cells, fontsize=7)
    ax.set_ylabel(ylabel, fontsize=8)
    if logy:
        ax.set_yscale("log")
    ax.tick_params(labelsize=7)
    ax.grid(True, alpha=0.2, which="both")
    return True


def _emit_diag_grid(data: SweepData, diag: pd.DataFrame, name: str,
                    title: str) -> Optional[Path]:
    if diag.empty:
        return None
    # Median across binaries per (cell, structure)
    agg = (diag.groupby(["cell", "structure"])
                .median(numeric_only=True).reset_index())
    cells = [c for c in ["c2", "c1", "c3", "c0"] if c in agg["cell"].unique()]
    if not cells:
        return None
    fig, axes = plt.subplots(2, 3, figsize=(11.0, 6.5))
    for ax, (title_p, col, ylabel, logy) in zip(axes.flat, DIAG_EXPLORE_FAMILIES):
        ax.set_title(title_p, fontsize=9)
        _draw_metric_panel(ax, agg, col, ylabel, logy, cells)
    handles, labels = [], []
    for ax in axes.flat:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in labels:
                handles.append(h); labels.append(l)
    if handles:
        fig.legend(handles, labels, loc="upper center",
                   ncol=len(labels), fontsize=8,
                   bbox_to_anchor=(0.5, 1.02), frameon=False)
    fig.suptitle(title, fontsize=11, y=1.05)
    dest = data.figures_root / "diagnostics" / name
    return _save(fig, dest, data.footer, include_footer=True)[0]


def fig_diag_explore_all(data: SweepData) -> Optional[Path]:
    """6-panel metric grid, median across all TPC-H/TPCHI binaries at bg=2."""
    diag = _diag_filter(data.diagnostics)
    return _emit_diag_grid(
        data, diag, "diag_explore_all",
        "Diagnostics — median across q3/q5/q3i/q5i × {lsm, btree} (bg=2)")


def fig_diag_explore_q3i_lsm(data: SweepData) -> Optional[Path]:
    """Per-binary diagnostics for q3i_lsm — the one flagged anomaly."""
    diag = _diag_filter(data.diagnostics, binary_filter="q3i_lsm")
    return _emit_diag_grid(
        data, diag, "diag_explore_q3i_lsm",
        "Diagnostics — q3i_lsm only (anomaly attribution)")


def emit_diag_summary_csv(data: SweepData) -> Optional[Path]:
    """One-row-per-(binary, cell, structure) summary of the metrics we plot.
    Lets the writer table-ify any of them without re-running the analyzer."""
    diag = _diag_filter(data.diagnostics)
    if diag.empty:
        return None
    cols = [c for _, c, _, _ in DIAG_EXPLORE_FAMILIES if c in diag.columns]
    keep = ["binary", "cell", "structure"] + cols
    sub = diag[keep].copy()
    agg = (sub.groupby(["binary", "cell", "structure"])
               .median(numeric_only=True).reset_index())
    dest = data.summary_root / "diagnostics_paper.csv"
    agg.to_csv(dest, index=False)
    return dest


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

FIGURE_BUILDERS: Dict[str, Callable[[SweepData], Optional[Path]]] = {
    "headline_tpch_vs_dram":      lambda d: fig_headline_tpch(d, "dram"),
    "headline_tpch_vs_secondary": lambda d: fig_headline_tpch(d, "secondary"),
    "headline_geo_vs_dram":       lambda d: fig_headline_geo(d, "dram"),
    "headline_geo_vs_secondary":  lambda d: fig_headline_geo(d, "secondary"),
    "contention_dropoff":         fig_contention_dropoff,
    "inversions":                 fig_inversions,
    "s3_vs_s2_speedup":           fig_s3_vs_s2_speedup,
    "duration_baseline":          fig_duration_baseline,
    "diagnostics_attribution":    fig_diagnostics,
    # Paper-mode builders (typeset-ready, bg=2 only, S1-S4 only).
    "paper_tpch_btree":           lambda d: fig_paper_tpch_row(d, "btree", include_legend=True),
    "paper_tpch_lsm":             lambda d: fig_paper_tpch_row(d, "lsm",   include_legend=False),
    "paper_geo_condensed":        fig_paper_geo_condensed,
    # Diagnostics exploration (multi-metric scan, scratch outputs).
    "diag_explore_all":           fig_diag_explore_all,
    "diag_explore_q3i_lsm":       fig_diag_explore_q3i_lsm,
}

# Curated subsets selectable via --mode.
MODE_FIGURES: Dict[str, List[str]] = {
    "default": [k for k in FIGURE_BUILDERS
                if not k.startswith(("paper_", "diag_explore_"))],
    "paper-figures": ["paper_tpch_btree", "paper_tpch_lsm",
                      "paper_geo_condensed"],
    "diagnostics-explore": ["diag_explore_all", "diag_explore_q3i_lsm"],
    "all": list(FIGURE_BUILDERS.keys()),
}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tag", required=True, help="sweep tag, e.g. 2026-05-18-a")
    p.add_argument("--root", type=Path, default=None,
                   help="paper-data/<tag>/ root (default: paper-data/<tag> relative to cwd)")
    p.add_argument("--mode", default="default",
                   choices=list(MODE_FIGURES.keys()),
                   help="curated figure subset: default (sweep diagnostics), "
                        "paper-figures (typeset-ready), diagnostics-explore "
                        "(6-panel metric grids + diagnostics_paper.csv), all")
    p.add_argument("--figures", default=None,
                   help="comma list of figure names to emit; overrides --mode")
    p.add_argument("--format", default="pdf", choices=["pdf", "png", "svg", "pgf"],
                   help="primary output format; pdf also emits a PNG sibling")
    args = p.parse_args()

    root = args.root or (Path.cwd() / "paper-data" / args.tag)
    if not root.exists():
        print(f"[plotter] error: {root} does not exist", file=sys.stderr)
        return 1
    if args.format == "pgf":
        matplotlib.rcParams.update({
            "pgf.texsystem": "pdflatex",
            "font.family": "serif",
            "text.usetex": False,
            "pgf.rcfonts": False,
        })

    data = load_sweep(args.tag, root)
    if args.figures:
        names = [n.strip() for n in args.figures.split(",") if n.strip()]
        unknown = [n for n in names if n not in FIGURE_BUILDERS]
        if unknown:
            print(f"[plotter] unknown figure(s): {unknown}", file=sys.stderr)
            return 1
    else:
        names = MODE_FIGURES[args.mode]

    # Side effect: diagnostics-explore mode also writes the per-cell
    # summary CSV so the writer can table-ify any metric without
    # rerunning the analyzer.
    if args.mode == "diagnostics-explore" and not args.figures:
        csv_path = emit_diag_summary_csv(data)
        if csv_path:
            print(f"[plotter] wrote {csv_path}", file=sys.stderr)

    emitted: List[Path] = []
    for name in names:
        try:
            out = FIGURE_BUILDERS[name](data)
        except Exception as e:
            print(f"[plotter] ERROR while building {name}: {e}", file=sys.stderr)
            continue
        if out is None:
            print(f"[plotter] {name}: skipped (no data)", file=sys.stderr)
        else:
            emitted.append(out)
            print(f"[plotter] wrote {out}", file=sys.stderr)
    print(f"[plotter] done: {len(emitted)} figures in {data.figures_root}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
