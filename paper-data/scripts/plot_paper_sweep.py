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
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import matplotlib
matplotlib.use("Agg")  # no display required on the experiment host
# usetex so \textsc{...} in legends / labels renders as proper small
# caps, matching tab:exp-baselines in the paper. Requires a working
# LaTeX install (TeX Live's pdflatex on the experiment host).
matplotlib.rcParams.update({
    "text.usetex": True,
    "font.family": "serif",
    "text.latex.preamble": r"\usepackage{lmodern}",
})
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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
        2: "#2980b9",  # S2 materialized view (naive) — blue
        # Q10-only partial-aggregate Mat-View variant. Same blue
        # family, lightened, so the legend reads as a Mat-View variant
        # rather than a new approach. The Merged-Idx partial-agg
        # variant (to be added) will follow the same convention on the
        # green family.
        22: "#7fb3d5",
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

# Labels matching tab:exp-baselines in the paper (monospace).
# Used for all paper-mode and diagnostics figures so the legend
# vocabulary stays in sync with the typeset table.
# Labels matching tab:exp-baselines in the paper. The paper uses
# \textsc{...} (small caps with hyphens); plain matplotlib doesn't
# render small caps so we mirror the exact label text and let LaTeX
# pickup happen at typeset time.
PAPER_STRUCTURE_LABELS = {
    1: r"\textsc{Base-Merge}",
    2: r"\textsc{Mat-View}",
    # Q10-only partial-aggregate Mat-View variant.
    22: r"\textsc{Mat-View} (partial agg)",
    3: r"\textsc{Merged-Idx}",
    4: r"\textsc{Base-Hash}",
    5: r"\textsc{aCOLI}",
}

def _query_title(binary: str) -> str:
    """Strip backend suffix and uppercase the leading Q so panel titles
    read as Q3 / Q5 / Q3i / Q5i — matching the paper's prose."""
    q = binary.replace("_lsm", "").replace("_btree", "")
    if q.startswith("q"):
        q = "Q" + q[1:]
    return q


# Cell → (DRAM GiB, secondary GiB). Kept for reference and used by
# anything that wants to render the cell key in a caption / legend.
CELL_DRAM_GIB = {"c0": 1.0, "c1": 0.4, "c2": 0.1, "c3": 0.4}
CELL_SECONDARY_GIB = {"c0": 5.0, "c1": 2.0, "c2": 0.5, "c3": 5.0}


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
    disk: Optional[str] = None  # 'hdd' / 'ssd' / None (legacy unmarked)

    @property
    def footer(self) -> str:
        m = self.manifest
        parts = [f"tag {self.tag}"]
        for k in ("commit_sha", "host", "cells", "families"):
            v = m.get(k)
            if v:
                parts.append(f"{k.split('_')[0]} {v}")
        if self.disk:
            parts.append(f"disk {self.disk}")
        return "  |  ".join(parts)

    def paper_name(self, base: str) -> str:
        """Suffix a paper-figure basename with disk media when known, so
        SSD reruns don't silently overwrite the HDD baselines."""
        return f"{base}_{self.disk}" if self.disk else base


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

    # Disk-media tag (see scripts/mark_disk_media.py). Mode of the
    # `disk` column across all summary CSVs; warn on mixed values.
    disk: Optional[str] = None
    seen = set()
    for df in (headline, stats, inversions, diagnostics):
        if not df.empty and "disk" in df.columns:
            seen.update(str(v) for v in df["disk"].dropna().unique())
    if seen:
        if len(seen) > 1:
            print(f"[plotter] WARN: mixed disk media in {tag}: {sorted(seen)}",
                  file=sys.stderr)
        disk = sorted(seen)[0]

    return SweepData(tag=tag, summary_root=summary, figures_root=figures,
                     headline=headline, stats=stats, inversions=inversions,
                     diagnostics=diagnostics, manifest=manifest, disk=disk)


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
                hue_order: Sequence[int],
                hue_labels: Dict[int, str],
                bar_width: float = 0.18) -> None:
    """One bar per (x, hue). df is long-form, one row per (x, hue).

    Bars are placed at ``i*1 + (k - (n-1)/2) * bar_width`` so groups
    stay centred on integer x. Missing (x, hue) combinations are
    silently skipped (no zero-height bar fabricated).
    """
    n = len(hue_order)
    x_idx = {x: i for i, x in enumerate(x_order)}
    for k, hue in enumerate(hue_order):
        color = STYLE["structure_colors"].get(int(hue), "#777777")
        label = hue_labels.get(int(hue), str(hue))
        sub = df[df[hue_col] == hue]
        xs, ys = [], []
        for _, row in sub.iterrows():
            if row[x_col] not in x_idx:
                continue
            v = row[y_col]
            if pd.isna(v):
                continue
            xs.append(x_idx[row[x_col]] + (k - (n - 1) / 2) * bar_width)
            ys.append(float(v))
        if xs:
            ax.bar(xs, ys, width=bar_width, color=color, label=label,
                   linewidth=0)
    ax.set_xticks(np.arange(len(x_order)))
    ax.set_xticklabels(x_order)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _save(fig: plt.Figure, dest: Path, footer: str, fmt: str = "pdf",
          include_footer: bool = True) -> List[Path]:
    """Save fig with a discreet footer line. Always emits PDF + PNG sibling
    when fmt='pdf'. Otherwise emits only the requested format.

    Set ``include_footer=False`` for paper-mode output where the footer
    would just be noise in a typeset figure.
    """
    # Skip tight_layout entirely when constrained_layout is already
    # in charge — calling both leads to a warning and stray whitespace.
    using_cl = bool(fig.get_constrained_layout())
    if include_footer:
        fig.text(0.5, 0.005, footer, ha="center", va="bottom",
                 fontsize=STYLE["footer_fontsize"], color="#666")
        if not using_cl:
            fig.tight_layout(rect=[0, 0.025, 1, 1])
    elif not using_cl:
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
# Q10 lives in its own figure (paper_q10) so its 5-bar layout doesn't
# disrupt the 4-bar headline row geometry.
Q10_QUERY = "q10"

# Q10 lives in its own ad-hoc sibling tag (not in the standard sweep
# matrix yet — see 2026-05-25-q10/manifest.yaml). When rendering the
# headline row we merge Q10 rows in from this tag and keep only the
# canonical method per structure (the per-order pre-aggregated view
# matches the §5 narrative; the COL walker is the standard MI path).
Q10_SIBLING_TAG = "2026-05-25-q10"
# Method → synthetic structure id. The naive per-lineitem view keeps
# the canonical S2 (Mat-View) slot; the partial-aggregate variant goes
# to id 22 so it draws as its own bar next to the naive Mat-View. The
# merged-index physskip variant is currently dropped (the standard
# COL walker is the canonical S3 in the paper text).
Q10_METHOD_TO_STRUCT = {
    "base_merge_join":      1,
    "pipeline_view":        2,
    "pipeline_view_preagg": 22,
    "mi_col_walk":          3,
    "base_hash_join":       4,
}
# Per-query bar order for the headline panels. Defaults to
# PAPER_LEGEND_ORDER (4 bars) when the query is not listed.
# Q10 layout: place the partial-agg variant immediately after the
# naive S2 so the two Mat-View bars sit together.
PAPER_PANEL_STRUCTURES = {
    "q10": [4, 1, 2, 22, 3],
}
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
# Headline figure focuses on a single cell (5L = biggest data, low memory
# pressure) so each panel is one group of 4 coloured bars, not a crowded
# scale-up + pressure overlay. Memory pressure gets its own figure.
PAPER_HEADLINE_CELL = "c0"
# Queries shown in the memory-pressure / scale figure (drop q3/q3i to keep
# the figure focused — q5 is the canonical headline; q5i covers tpchi).
MEMORY_PRESSURE_QUERIES = ["q5", "q5i"]
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
    coincide. One marker + transparency makes overlap explicit.

    Also tightens y-axis tick presentation: label only the major
    powers of 10 (suppress 2×10³, 6×10², etc.) and use a small font.
    Otherwise narrow-range panels (e.g. q3 btree 300-2000 s) emit
    five intermediate labels and eat the horizontal space of the
    next panel."""
    # Merged-index (S3, green) gets a distinct diamond marker so the
    # headline series stays visually traceable when lines overlap;
    # everyone else uses the same circle.
    s3_color = STYLE["structure_colors"][3]
    for line in ax.get_lines():
        is_s3 = line.get_color() == s3_color
        line.set_marker("D" if is_s3 else "o")
        line.set_markersize(5 if is_s3 else 4)
        line.set_alpha(0.85 if is_s3 else 0.7)
    # Suppress the IQR fill_between bands — at 3 reps the IQR is
    # noisy and the "shadow" around lines (most visible behind
    # pipeline view) reads as a visual artefact rather than an
    # uncertainty signal.
    for coll in list(ax.collections):
        coll.set_visible(False)
    # Linear y-axis for headline plots so magnitude differences read
    # directly. Cap at 0 to keep the bars/lines anchored to a real baseline.
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5))
    ax.tick_params(axis="y", which="major", labelsize=7)


def _paper_bar_panel(ax, ms_df: pd.DataFrame, binary: str,
                     cell: str, show_ylabel: bool,
                     structures: Optional[Sequence[int]] = None,
                     title: Optional[str] = None) -> bool:
    """One compact bar panel of the paper TPC-H headline row. bg=2,
    one cell, one bar per structure in ``structures`` (defaults to
    ``PAPER_LEGEND_ORDER``). ms → seconds; linear y with a per-panel
    20× cap so a single outlier doesn't squash the rest. The panel
    label is rendered along the bottom (via ``set_xlabel``) so the
    top edge stays clear for cut-off-bar value annotations.
    """
    panel_structs = list(structures or PAPER_LEGEND_ORDER)
    panel_title = title if title is not None else _query_title(binary)
    sub = ms_df[(ms_df["binary"] == binary) & (ms_df["cell"] == cell)
                & (ms_df["bg"] == PAPER_HEADLINE_BG)
                & (ms_df["structure"].isin(panel_structs))]
    if sub.empty:
        _all_or_empty(plt.gcf(), ax, "—")
        ax.set_xlabel(panel_title, fontsize=14)
        return False
    sub = sub.copy()
    sub["s_median"] = sub["ms_median"] / 1000.0
    # One bar per structure, centred on x=0. Q10 has 5 bars (extra
    # Mat-View variant), so the per-bar width scales with the panel's
    # structure count to keep the cluster the same total width.
    n = len(panel_structs)
    bar_w = 0.72 / max(n, 1)
    drew = False
    plotted: List[Tuple[float, float, int]] = []  # (x, height, struct)
    for k, struct in enumerate(panel_structs):
        row = sub[sub["structure"] == struct]
        if row.empty or pd.isna(row["s_median"].iloc[0]):
            continue
        x = (k - (n - 1) / 2) * bar_w
        h = float(row["s_median"].iloc[0])
        plotted.append((x, h, struct))
    # Pick the panel's y-cap from the second-tallest bar (with a small
    # headroom factor) rather than as a multiple of the shortest. This
    # makes the non-outlier bars occupy most of the panel even when a
    # single hash-join bar is two orders of magnitude taller; the over-
    # cap bar gets the value annotation above the top border.
    if plotted:
        finite = sorted((h for _, h, _ in plotted if h > 0), reverse=True)
        if len(finite) >= 2:
            cap = finite[1] * 1.15
        elif finite:
            cap = finite[0] * 1.15
        else:
            cap = None
    else:
        cap = None
    ylim_top = cap * 1.08 if cap is not None else None
    for x, h, struct in plotted:
        # Over-cap bars extend all the way to the top border; the
        # printed value sits just above the border so the reader can
        # still read the true number.
        draw_h = min(h, ylim_top) if ylim_top is not None else h
        ax.bar([x], [draw_h], width=bar_w,
               color=STYLE["structure_colors"][struct],
               linewidth=0, clip_on=False)
        if ylim_top is not None and h > ylim_top:
            ax.annotate(f"{h:.0f}", xy=(x, 1.0),
                        xycoords=("data", "axes fraction"),
                        xytext=(0, 2), textcoords="offset points",
                        ha="center", va="bottom", fontsize=10,
                        color=STYLE["structure_colors"][struct],
                        annotation_clip=False)
        drew = True
    ax.set_xlabel(panel_title, fontsize=14)
    if show_ylabel:
        ax.set_ylabel("seconds / query", fontsize=12)
    ax.set_xticks([])
    # Linear y-axis for headline bars — log scale would compress the
    # S3 vs S2/S4 gap and undersell the win.
    if ylim_top is not None:
        ax.set_ylim(0, ylim_top)
    else:
        ax.set_ylim(bottom=0)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5))
    ax.tick_params(axis="y", which="major", labelsize=11)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    return drew


def _add_two_row_legend(fig, legend_structs: Sequence[int],
                        *, fontsize: int,
                        bbox_main: Tuple[float, float],
                        bbox_variants: Tuple[float, float]) -> None:
    """Render the headline legend across two rows: canonical S1..S4 on
    top, partial-agg variants (synthetic ids ≥ 22) on a separate row
    below. matplotlib's column-major fill orders entries by column so a
    single ``fig.legend`` with ``ncol=4`` would scramble row 1; using
    two legends keeps the two groups aligned independently.
    """
    main = [s for s in legend_structs if s in PAPER_LEGEND_ORDER]
    variants = [s for s in legend_structs if s not in PAPER_LEGEND_ORDER]
    main_handles = [plt.Rectangle((0, 0), 1, 1,
                                  color=STYLE["structure_colors"][s],
                                  label=PAPER_STRUCTURE_LABELS[s])
                    for s in main]
    fig.legend(handles=main_handles, loc="upper center",
               ncol=len(main_handles) or 1,
               fontsize=fontsize, bbox_to_anchor=bbox_main,
               frameon=False, columnspacing=1.5, handletextpad=0.4)
    if not variants:
        return
    variant_handles = [plt.Rectangle((0, 0), 1, 1,
                                     color=STYLE["structure_colors"][s],
                                     label=PAPER_STRUCTURE_LABELS[s])
                       for s in variants]
    # Second legend attaches directly to the figure (fig.legend would
    # overwrite the first), so use add_artist with a manually-built
    # Legend bound to the figure's transform.
    from matplotlib.legend import Legend
    second = Legend(fig, variant_handles,
                    [h.get_label() for h in variant_handles],
                    loc="upper center", ncol=len(variant_handles),
                    fontsize=fontsize, bbox_to_anchor=bbox_variants,
                    bbox_transform=fig.transFigure,
                    frameon=False, columnspacing=1.5, handletextpad=0.4)
    fig.add_artist(second)


def _augment_with_q10(head: pd.DataFrame, data: SweepData,
                      backend: str) -> pd.DataFrame:
    """Splice Q10 rows from the sibling tag into a filtered headline
    frame. The sibling carries two methods per S2/S3 (the A/B variants
    from a perf investigation); we keep only the canonical method per
    structure so each panel has one bar per structure. No-op when the
    sibling isn't reachable from this tag's root.

    Q10 only has SSD data, so we only splice it in when the parent
    tag is SSD-tagged (otherwise the panel just stays empty).
    """
    if data.disk and data.disk != "ssd":
        return head
    sibling = data.summary_root.parent.parent / Q10_SIBLING_TAG \
        / "summary" / "headline.csv"
    if not sibling.exists():
        return head
    q10 = pd.read_csv(sibling)
    q10 = q10[(q10["backend"] == backend) & (q10["tx"] == "query")
              & (q10["query"] == "q10")
              & (q10["method"].isin(Q10_METHOD_TO_STRUCT.keys()))]
    if q10.empty:
        return head
    # Remap method → synthetic structure id so the naive Mat-View
    # variant draws as its own bar (id 22) alongside the pre-agg S2.
    q10 = q10.copy()
    q10["structure"] = q10["method"].map(Q10_METHOD_TO_STRUCT).astype(int)
    # Sibling sweep ran one rep at bg=0 (isolated), parent sweeps run
    # at bg=2 (contention cohort) — relabel to the headline bg so the
    # row is picked up by _paper_bar_panel's filter.
    q10["bg"] = PAPER_HEADLINE_BG
    q10["family"] = "vanilla"
    common = [c for c in head.columns if c in q10.columns]
    return pd.concat([head, q10[common]], ignore_index=True)


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
    n_panels = len(binaries)
    # Author the figure at full-page width (~6.5") so each panel has
    # room for tick labels + line markers; LaTeX scales it down to
    # column width at \includegraphics time. constrained_layout
    # handles the external legend bbox without leaving stray
    # whitespace that tight_layout sometimes does with sharey=False.
    fig, axes = plt.subplots(1, n_panels, figsize=(2.1 * n_panels, 2.6),
                             sharey=False, constrained_layout=True)
    has_any = False
    legend_structs: List[int] = list(PAPER_LEGEND_ORDER)
    for j, (binary, q) in enumerate(zip(binaries, PAPER_TPCH_QUERIES)):
        panel_structs = PAPER_PANEL_STRUCTURES.get(q, PAPER_LEGEND_ORDER)
        for s in panel_structs:
            if s not in legend_structs:
                legend_structs.append(s)
        drew = _paper_bar_panel(axes[j], ms_df, binary, PAPER_HEADLINE_CELL,
                                show_ylabel=(j == 0),
                                structures=panel_structs)
        has_any = has_any or drew
    if not has_any:
        plt.close(fig)
        return None
    if include_legend:
        _add_two_row_legend(fig, legend_structs, fontsize=13,
                            bbox_main=(0.5, 1.16),
                            bbox_variants=(0.5, 1.06))
    name = data.paper_name(f"paper_tpch_{backend}_headline")
    dest = data.figures_root / "paper" / name
    return _save(fig, dest, data.footer, include_footer=False)[0]


def fig_paper_q10(data: SweepData) -> Optional[Path]:
    """1×2 dedicated Q10 figure: B-tree | LSM. Q10 carries an extra
    Mat-View bar (naive per-lineitem vs pre-agg per-order) which
    makes a 5-bar panel; keeping it out of the four-query headline
    row preserves that row's regular 4-bar geometry.
    """
    # Start from an empty frame and pull Q10 rows from the sibling tag
    # for both backends; the standard sweep tag doesn't carry Q10 yet.
    empty = pd.DataFrame(columns=data.headline.columns)
    head_parts = [p for p in (_augment_with_q10(empty, data, b)
                              for b in ("btree", "lsm"))
                  if not p.empty]
    if not head_parts:
        return None
    head = pd.concat(head_parts, ignore_index=True)
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])

    panel_structs = PAPER_PANEL_STRUCTURES.get(Q10_QUERY, PAPER_LEGEND_ORDER)
    fig, axes = plt.subplots(1, 2, figsize=(5.4, 2.8), sharey=False,
                             constrained_layout=True)
    titles = {"btree": "Q10 (B-tree)", "lsm": "Q10 (LSM-tree)"}
    drew_any = False
    for j, backend in enumerate(("btree", "lsm")):
        binary = f"{Q10_QUERY}_{backend}"
        drew = _paper_bar_panel(axes[j], ms_df, binary, PAPER_HEADLINE_CELL,
                                show_ylabel=(j == 0),
                                structures=panel_structs,
                                title=titles[backend])
        drew_any = drew_any or drew
    if not drew_any:
        plt.close(fig)
        return None

    legend_structs: List[int] = list(PAPER_LEGEND_ORDER)
    for s in panel_structs:
        if s not in legend_structs:
            legend_structs.append(s)
    handles = [plt.Rectangle((0, 0), 1, 1,
                             color=STYLE["structure_colors"][s],
                             label=PAPER_STRUCTURE_LABELS[s])
               for s in legend_structs]
    _add_two_row_legend(fig, legend_structs, fontsize=13,
                        bbox_main=(0.5, 1.18),
                        bbox_variants=(0.5, 1.07))

    name = data.paper_name("paper_q10")
    dest = data.figures_root / "paper" / name
    return _save(fig, dest, data.footer, include_footer=False)[0]


def _memory_pressure_panel(ax, ms_df: pd.DataFrame, binary: str,
                           cells: Sequence[str], show_ylabel: bool) -> bool:
    """One panel of the memory-pressure / scale figure. Line plot across
    three cells (2, 5L, 5H) so the scale-up and pressure trends read
    left-to-right. Visual conventions match the prior headline panel."""
    sub = ms_df[(ms_df["binary"] == binary) & (ms_df["cell"].isin(cells))
                & (ms_df["bg"] == PAPER_HEADLINE_BG)
                & (ms_df["structure"].isin(PAPER_STRUCTURES))]
    if sub.empty:
        _all_or_empty(plt.gcf(), ax, "—")
        ax.set_title(_query_title(binary),
                     fontsize=10)
        return False
    sub = sub.copy()
    sub["s_median"] = sub["ms_median"] / 1000.0
    sub["s_iqr"] = sub["ms_iqr"] / 1000.0
    line_with_iqr(
        ax, sub, x_col="cell", y_col="s_median", iqr_col="s_iqr",
        hue_col="structure", x_order=cells,
        hue_labels=STRUCTURE_LABELS, linestyle="-",
    )
    ax.set_title(_query_title(binary),
                 fontsize=10)
    if show_ylabel:
        ax.set_ylabel("seconds / query", fontsize=8)
    ax.set_xticks(np.arange(len(cells)))
    ax.set_xticklabels([PAPER_CELL_TICK.get(c, c) for c in cells],
                       fontsize=8)
    ax.tick_params(axis="y", labelsize=6)
    ax.grid(False)
    if ax.get_legend():
        ax.get_legend().remove()
    _apply_paper_overlap_style(ax)
    return True


def fig_paper_memory_pressure(data: SweepData, backend: str,
                              include_legend: bool) -> Optional[Path]:
    """1×2 grouped-bar figure: q5 and q5i, three cells (2/5L/5H)."""
    assert backend in ("btree", "lsm")
    head = data.headline[data.headline["family"].isin(["vanilla", "tpchi"])
                         & (data.headline["backend"] == backend)
                         & (data.headline["tx"] == "query")]
    if head.empty:
        return None
    have_cells = set(head["cell"].dropna().unique())
    missing = [c for c in PAPER_CELLS if c not in have_cells]
    if missing:
        print(f"[plotter] paper_tpch_{backend}_memory: skipped — "
              f"memory_pressure needs {PAPER_CELLS}, tag has "
              f"{sorted(have_cells)}", file=sys.stderr)
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])
    binaries = [f"{q}_{backend}" for q in MEMORY_PRESSURE_QUERIES]
    fig, axes = plt.subplots(1, 2, figsize=(4.5, 2.2), sharey=False,
                             constrained_layout=True)
    has_any = False
    for j, binary in enumerate(binaries):
        drew = _memory_pressure_panel(axes[j], ms_df, binary, PAPER_CELLS,
                                      show_ylabel=(j == 0))
        has_any = has_any or drew
    if not has_any:
        plt.close(fig)
        return None
    if include_legend:
        handles = [plt.Line2D([], [], color=STYLE["structure_colors"][s],
                              marker=("D" if s == 3 else "o"),
                              markersize=(5 if s == 3 else 4),
                              linewidth=1.2,
                              alpha=(0.85 if s == 3 else 0.7),
                              label=PAPER_STRUCTURE_LABELS[s])
                   for s in PAPER_LEGEND_ORDER]
        fig.legend(handles=handles, loc="upper center", ncol=4,
                   fontsize=8, bbox_to_anchor=(0.5, 1.10),
                   frameon=False, columnspacing=1.5, handletextpad=0.4)
    name = data.paper_name(f"paper_tpch_{backend}_memory_pressure")
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
        print("[plotter] paper_geo_condensed: skipped — needs geo binaries, "
              "tag has none", file=sys.stderr)
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg", "tx"])
    fig, axes = plt.subplots(2, 3, figsize=(6.5, 3.0), sharey="row",
                             sharex="col", constrained_layout=True)
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
            sub = sub.copy()
            sub["s_median"] = sub["ms_median"] / 1000.0
            sub["s_iqr"] = sub["ms_iqr"] / 1000.0
            line_with_iqr(
                ax, sub, x_col="cell", y_col="s_median",
                iqr_col="s_iqr", hue_col="structure",
                x_order=PAPER_CELLS, hue_labels=STRUCTURE_LABELS,
                linestyle="-",
            )
            ax.grid(False)
            if i == 0:
                ax.set_title(tx, fontsize=10)
            if j == 0:
                ax.set_ylabel(f"{backend}\nseconds / query", fontsize=8)
            if i == len(backend_order) - 1:
                ax.set_xticks(np.arange(len(PAPER_CELLS)))
                ax.set_xticklabels(
                    [PAPER_CELL_TICK.get(c, c) for c in PAPER_CELLS],
                    fontsize=8)
            ax.tick_params(axis="y", labelsize=7)
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
                          label=PAPER_STRUCTURE_LABELS[s])
               for s in PAPER_STRUCTURES]
    fig.legend(handles=handles, loc="upper center", ncol=4,
               fontsize=6, bbox_to_anchor=(0.5, 1.04),
               frameon=False, columnspacing=1.2, handletextpad=0.4)
    dest = data.figures_root / "paper" / data.paper_name("paper_geo_condensed")
    return _save(fig, dest, data.footer, include_footer=False)[0]


# ---------------------------------------------------------------------------
# Diagnostics exploration mode
# ---------------------------------------------------------------------------
#
# Plot the candidate metric families from §Diagnostics in the plan. Goal:
# scan visually after the run, pick 1-2 panels that earn paper-mode
# treatment. Outputs live under figures/diagnostics/.

# Per-query × per-backend diagnostics. btree and lsm have disjoint
# signal sets in the post-fix CSV (see paper-data/CLAUDE.md and the
# plan file): btree gets the full CPU + BM + CR + DT counter family;
# lsm gets cycles + util + SST timing surrogates. Each entry below is
# (figure-name suffix, column, panel-y-label, log-scale?).
DIAG_BTREE_METRICS: List[Tuple[str, str, str, bool]] = [
    ("llc_miss",  "cpu_llc_miss_per_tx", "LLC miss / TX",     True),
    ("bm_rounds", "bm_rounds",           "BM eviction rounds", True),
    ("dt_split",  "dt_struct_split",     "B-tree splits",      True),
]
DIAG_LSM_METRICS: List[Tuple[str, str, str, bool]] = [
    ("sst_read",       "sst_read_us_per_tx",  r"SST read $\mu$s / TX",    True),
    ("sst_compaction", "sst_compaction_us",   r"SST compaction $\mu$s",   True),
    ("cpu_cycles",     "cpu_cycles_per_tx",   "CPU cycles / TX",     True),
]


def _diag_filter(diag: pd.DataFrame) -> pd.DataFrame:
    """Restrict to the paper's contention cohort and the 4 paper structures."""
    return diag[(diag["family"].isin(["vanilla", "tpchi"]))
                & (diag["bg"] == PAPER_HEADLINE_BG)
                & (diag["tx"] == "query")
                & (diag["structure"].isin(PAPER_STRUCTURES))]


def _aggregate_metric(diag: pd.DataFrame, col: str
                      ) -> pd.DataFrame:
    """Median across reps per (binary, cell, structure)."""
    if col not in diag.columns:
        return pd.DataFrame()
    sub = diag[["binary", "cell", "structure", col]].dropna(subset=[col])
    if sub.empty:
        return sub
    return (sub.groupby(["binary", "cell", "structure"])
               .median(numeric_only=True).reset_index())


def _diag_panel(ax, agg: pd.DataFrame, binary: str, col: str,
                ylabel: str, cells: Sequence[str], logy: bool,
                show_ylabel: bool) -> bool:
    """One panel of a per-query diagnostic row, matching `_paper_panel`'s
    visual conventions (same marker for all structures, alpha overlap,
    no gridlines, per-panel y-auto-scale)."""
    sub = agg[(agg["binary"] == binary) & (agg["cell"].isin(cells))]
    if sub.empty or col not in sub.columns:
        _all_or_empty(plt.gcf(), ax, "—")
        ax.set_title(_query_title(binary),
                     fontsize=10)
        return False
    drew = False
    for struct in PAPER_STRUCTURES:
        s = sub[sub["structure"] == struct].set_index("cell").reindex(cells)
        y = s[col].astype(float).values
        if np.all(np.isnan(y)):
            continue
        style = _structure_style(struct)
        ax.plot(np.arange(len(cells)), y,
                label=PAPER_STRUCTURE_LABELS[struct],
                color=style["color"], marker="o", markersize=4,
                linewidth=1.2, alpha=0.7)
        drew = True
    ax.set_title(_query_title(binary),
                 fontsize=10)
    if show_ylabel:
        ax.set_ylabel(ylabel, fontsize=8)
    ax.set_xticks(np.arange(len(cells)))
    ax.set_xticklabels([PAPER_CELL_TICK.get(c, c) for c in cells],
                       fontsize=8)
    if logy:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0))
        ax.yaxis.set_major_formatter(
            mticker.LogFormatterSciNotation(base=10.0, labelOnlyBase=True))
        ax.yaxis.set_minor_locator(
            mticker.LogLocator(base=10.0, subs=tuple(range(2, 10))))
        ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.tick_params(axis="y", which="major", labelsize=7)
    ax.tick_params(axis="y", which="minor", length=2)
    ax.grid(False)
    return drew


def _emit_diag_row(data: SweepData, backend: str, name_suffix: str,
                   col: str, ylabel: str, logy: bool) -> Optional[Path]:
    diag = _diag_filter(data.diagnostics)
    diag = diag[diag["backend"] == backend]
    agg = _aggregate_metric(diag, col)
    if agg.empty:
        return None
    binaries = [f"{q}_{backend}" for q in PAPER_TPCH_QUERIES]
    fig, axes = plt.subplots(1, 4, figsize=(6.5, 2.0), sharey=False,
                             constrained_layout=True)
    drew_any = False
    for j, binary in enumerate(binaries):
        drew = _diag_panel(axes[j], agg, binary, col, ylabel, PAPER_CELLS,
                           logy, show_ylabel=(j == 0))
        drew_any = drew_any or drew
    if not drew_any:
        plt.close(fig)
        return None
    handles = [plt.Line2D([], [], color=STYLE["structure_colors"][s],
                          marker="o", markersize=4, linewidth=1.2, alpha=0.7,
                          label=PAPER_STRUCTURE_LABELS[s])
               for s in PAPER_LEGEND_ORDER]
    fig.legend(handles=handles, loc="upper center", ncol=4,
               fontsize=8, bbox_to_anchor=(0.5, 1.14),
               frameon=False, columnspacing=1.5, handletextpad=0.4)
    dest = data.figures_root / "diagnostics" / f"diag_{backend}_{name_suffix}"
    return _save(fig, dest, data.footer, include_footer=False)[0]


def fig_diag_btree_llc_miss(data):       return _emit_diag_row(data, "btree", *DIAG_BTREE_METRICS[0])
def fig_diag_btree_bm_rounds(data):      return _emit_diag_row(data, "btree", *DIAG_BTREE_METRICS[1])
def fig_diag_btree_dt_split(data):       return _emit_diag_row(data, "btree", *DIAG_BTREE_METRICS[2])
def fig_diag_lsm_sst_read(data):         return _emit_diag_row(data, "lsm",   *DIAG_LSM_METRICS[0])
def fig_diag_lsm_sst_compaction(data):   return _emit_diag_row(data, "lsm",   *DIAG_LSM_METRICS[1])
def fig_diag_lsm_cpu_cycles(data):       return _emit_diag_row(data, "lsm",   *DIAG_LSM_METRICS[2])


def emit_diag_summary_csv(data: SweepData) -> Optional[Path]:
    """One row per (binary, cell, structure) carrying every metric we plot.
    Backend-specific metrics will be NaN for the other backend's rows —
    that's the signal."""
    diag = _diag_filter(data.diagnostics)
    if diag.empty:
        return None
    cols = ([c for _, c, _, _ in DIAG_BTREE_METRICS if c in diag.columns]
            + [c for _, c, _, _ in DIAG_LSM_METRICS  if c in diag.columns])
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
    # Paper-mode builders (typeset-ready, bg=2 only, S1-S4 only).
    "paper_tpch_btree":      lambda d: fig_paper_tpch_row(d, "btree", include_legend=True),
    "paper_tpch_lsm":        lambda d: fig_paper_tpch_row(d, "lsm",   include_legend=True),
    "paper_tpch_btree_memory": lambda d: fig_paper_memory_pressure(d, "btree", include_legend=True),
    "paper_tpch_lsm_memory":   lambda d: fig_paper_memory_pressure(d, "lsm",   include_legend=False),
    "paper_q10":             fig_paper_q10,
    "paper_geo_condensed":   fig_paper_geo_condensed,
    # Diagnostics exploration — per-query 1×4 rows. btree gets the
    # full LeanStore counter family; lsm gets RocksDB SST timing
    # surrogates (only metrics that actually populate per backend).
    "diag_btree_llc_miss":    fig_diag_btree_llc_miss,
    "diag_btree_bm_rounds":   fig_diag_btree_bm_rounds,
    "diag_btree_dt_split":    fig_diag_btree_dt_split,
    "diag_lsm_sst_read":      fig_diag_lsm_sst_read,
    "diag_lsm_sst_compaction": fig_diag_lsm_sst_compaction,
    "diag_lsm_cpu_cycles":    fig_diag_lsm_cpu_cycles,
}

# Curated subsets selectable via --mode. Legacy default-mode builders
# (2×4 headline + contention + inversions + heatmap + duration + 2×2
# diagnostics) have moved to scripts/archive/legacy_figures.py.
DIAG_EXPLORE_FIGS = [
    "diag_btree_llc_miss", "diag_btree_bm_rounds", "diag_btree_dt_split",
    "diag_lsm_sst_read", "diag_lsm_sst_compaction", "diag_lsm_cpu_cycles",
]
MODE_FIGURES: Dict[str, List[str]] = {
    "paper-figures":       ["paper_tpch_btree", "paper_tpch_lsm",
                            "paper_tpch_btree_memory",
                            "paper_tpch_lsm_memory",
                            "paper_q10",
                            "paper_geo_condensed"],
    "diagnostics-explore": DIAG_EXPLORE_FIGS,
    "all":                 list(FIGURE_BUILDERS.keys()),
}
# 'default' is an alias for paper-figures so the runner script
# (experiments/run_paper_sweep.sh) keeps working without a flag.
MODE_FIGURES["default"] = MODE_FIGURES["paper-figures"]


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
