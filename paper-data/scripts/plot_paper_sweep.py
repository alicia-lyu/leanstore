#!/usr/bin/env python3
"""Paper-data plotter for the LeanStore sweep.

Consumes the four summary CSVs emitted by ``scripts/analyze_paper_sweep.py``
under ``paper-data/<tag>/summary/`` and produces paper-ready figures
(PDF + PNG sibling) under ``paper-data/diagrams/``. See
``scripts/PLOTTING.md`` for the figure catalog and design notes.

Each diagram is declared in ``paper-data/diagrams.yaml`` as a named entry
pointing at one or more sweep tag dirs. The plotter reads all sources,
concatenates their headline CSVs (tagging each row with ``_source_tag``),
and passes the merged frame to the named builder function.

CLI:
  --diagram <name>[,<name>,...]   build specific diagram(s) by YAML name
  --all                           build every diagram in diagrams.yaml
  --diag-tag <tag>                build diagnostics figures for a single sweep tag
  --format <pdf|png|svg|pgf>      primary output format (pdf also emits PNG)

Schema-driven: every series, color, and label is derived from the CSV
column values, not hardcoded against a particular sweep matrix. Adding
a new figure is four steps: write builder, register in ``BUILDERS``,
add YAML entry, add catalog row in PLOTTING.md.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import matplotlib
import os
matplotlib.use("Agg")  # no display required on the experiment host
# usetex so \textsc{...} in legends / labels renders as proper small
# caps, matching tab:exp-baselines in the paper. Requires a working
# LaTeX install (TeX Live's pdflatex on the experiment host).
# Override via MPL_USETEX=0 to render without latex (labels show
# \textsc{...} as plain text; paper-ready PDFs still need latex).
_USETEX = os.environ.get("MPL_USETEX", "1") != "0"
matplotlib.rcParams.update({
    "text.usetex": _USETEX,
    "font.family": "serif",
    "text.latex.preamble": r"\usepackage{lmodern}",
})
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import yaml

from _diagram_metadata import (
    load_metadata, sources_for, output_path, DIAGRAMS_OUTPUT_DIR,
)


# ---------------------------------------------------------------------------
# STYLE — one source of truth for color/marker/label conventions.
# Storage-structure color choices keep S3 (the merged-index headline) in
# green so it pops; S1 (baseline) in muted red; S2 (view) in blue; S4 in
# orange; S5 reserved purple. Backend distinguished by linestyle, not color.
# ---------------------------------------------------------------------------

STYLE = {
    # Okabe-Ito / Wong palette (Nature Methods 2011). Pairwise
    # distinguishable under deuteranopia, protanopia, and tritanopia,
    # and ordered so adjacent legend slots have high lightness contrast
    # — usable in monochrome print and photocopies. Partial-agg
    # variants reuse their parent's color and are distinguished by
    # hatching (see _struct_style); the 22 / 33 entries are kept here
    # only as documentation of the variant→parent mapping.
    "structure_colors": {
        1: "#D55E00",  # S1 Base-Merge       — vermillion (warm)
        2: "#0072B2",  # S2 Mat-View         — blue (cool)
        22: "#0072B2", # S2 partial-agg variant inherits S2 + hatch
        3: "#009E73",  # S3 Merged-Idx       — bluish green (HEADLINE)
        33: "#009E73", # S3 partial-agg variant inherits S3 + hatch
        4: "#E69F00",  # S4 Base-Hash        — orange / amber
        5: "#CC79A7",  # S5 aCOLI            — reddish purple (deferred)
        # S6 shared COL materialised view — darker variant of the
        # Mat-View family so it reads as a third Mat-View flavour
        # (one shared view vs the per-query views) while remaining
        # distinguishable from the CB-friendly S2 blue.
        6: "#003E5C",
    },
    "structure_markers": {1: "o", 2: "s", 3: "D", 4: "^", 5: "v", 6: "P"},
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

# Variants share their parent's color and are distinguished by hatching
# (set as a class invariant — every drawing or legend path that handles
# a "raw" structure id should go through _struct_style below).
VARIANT_PARENT = {22: 2, 33: 3}
VARIANT_HATCH = "..."


def _struct_style(struct: int) -> Tuple[str, Optional[str]]:
    """Return (color, hatch) for a structure id. Partial-agg variants
    inherit their parent's canonical color and add a hatch pattern."""
    parent = VARIANT_PARENT.get(struct, struct)
    color = STYLE["structure_colors"].get(parent, "#777777")
    hatch = VARIANT_HATCH if struct in VARIANT_PARENT else None
    return color, hatch


STRUCTURE_LABELS = {
    1: "S1 base merge-join",
    2: "S2 pipeline view",
    3: "S3 merged index",
    4: "S4 base hash-join",
    5: "S5 aCOLI MI",
    6: "S6 shared view",
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
    # Q10/Q10i partial-aggregate Merged-Idx variant (aCOL / aCOLI MI).
    33: r"\textsc{Merged-Idx} (partial agg)",
    4: r"\textsc{Base-Hash}",
    5: r"\textsc{aCOLI}",
    # S6 shared COL view — one materialised view (union schema) shared across
    # q3/q5/q10, vs the per-query \textsc{Mat-View} (S2).
    6: r"\textsc{Mat-View} (shared)",
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
CELL_DRAM_GIB = {"c0": 1.0, "c1": 0.4, "c2": 0.1, "c3": 0.4, "c4": 1.0}
CELL_SECONDARY_GIB = {"c0": 5.0, "c1": 2.0, "c2": 0.5, "c3": 5.0, "c4": 10.0}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

@dataclass
class SweepData:
    # ``name`` is the diagram name from diagrams.yaml (also the output stem).
    # ``source_tags`` is the list of tag strings that fed this diagram.
    # ``summary_root`` points to the primary tag's summary/ for the diag path.
    # ``figures_root`` is always paper-data/diagrams/ for paper builders, or
    # paper-data/<tag>/figures/ for the --diag-tag path.
    name: str
    source_tags: List[str]
    summary_root: Path
    figures_root: Path
    headline: pd.DataFrame
    stats: pd.DataFrame
    inversions: pd.DataFrame
    diagnostics: pd.DataFrame
    manifest: Dict[str, str]
    disk: Optional[str] = None  # 'hdd' / 'ssd' / None (legacy unmarked)

    @property
    def tag(self) -> str:
        """Primary source tag — used in footers and legacy callers."""
        return self.source_tags[0] if self.source_tags else self.name

    @property
    def footer(self) -> str:
        m = self.manifest
        tag_part = " + ".join(self.source_tags)
        parts = [f"tag {tag_part}"]
        for k in ("commit_sha", "host", "cells", "families"):
            v = m.get(k)
            if v:
                parts.append(f"{k.split('_')[0]} {v}")
        if self.disk:
            parts.append(f"disk {self.disk}")
        return "  |  ".join(parts)

    def paper_name(self, base: str) -> str:
        """Return the diagram output stem. For paper builders the diagram
        name from YAML is already the final filename, so ``base`` is
        ignored and ``self.name`` is returned. For the legacy diag-tag
        path (where ``name`` is the tag), fall back to appending the
        disk suffix so diagnostic filenames stay stable."""
        if self.figures_root == DIAGRAMS_OUTPUT_DIR:
            return self.name
        return f"{base}_{self.disk}" if self.disk else base


def _quantile(vals: pd.Series, p: float) -> float:
    vals = vals.dropna()
    if vals.empty:
        return float("nan")
    return float(vals.quantile(p))


def aggregate_ms_per_query(headline: pd.DataFrame,
                           group_cols: Sequence[str]) -> pd.DataFrame:
    """Aggregate per-rep ms/query (= 1000/tx_per_s) over ``group_cols``.

    Returns ``group_cols + ['ms_median', 'ms_iqr', 'ms_q25', 'ms_q75',
    'n']``. Median, IQR, and the quartiles are computed in ms space (not
    derived from TX/s aggregates) so the error bars are correct under the
    non-linear transform. ``ms_q25`` / ``ms_q75`` let bar panels draw
    asymmetric error bars anchored on the actual quartiles rather than a
    median±IQR/2 approximation.
    """
    cols = list(group_cols) + ["ms_median", "ms_iqr", "ms_q25", "ms_q75", "n"]
    if headline.empty:
        return pd.DataFrame(columns=cols)
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
    q25 = grouped.apply(lambda s: _quantile(s, 0.25)).rename("ms_q25")
    q75 = grouped.apply(lambda s: _quantile(s, 0.75)).rename("ms_q75")
    out = out.merge(q25.reset_index(), on=list(group_cols), how="left")
    out = out.merge(q75.reset_index(), on=list(group_cols), how="left")
    out["ms_iqr"] = out["ms_q75"] - out["ms_q25"]
    return out


def _read_tag_csvs(tag_root: Path,
                   tag: str) -> Dict[str, pd.DataFrame]:
    """Read the four summary CSVs for one tag dir. Returns a dict keyed
    by CSV name. Missing / empty files produce empty DataFrames."""
    summary = tag_root / "summary"
    result: Dict[str, pd.DataFrame] = {}
    for name in ("headline.csv", "stats.csv", "inversions.csv",
                 "diagnostics.csv"):
        p = summary / name
        if not p.exists() or p.stat().st_size == 0:
            print(f"[plotter] WARN: {p} missing or empty", file=sys.stderr)
            result[name] = pd.DataFrame()
        else:
            df = pd.read_csv(p)
            df["_source_tag"] = tag
            result[name] = df
    return result


def _load_manifest(tag_root: Path, tag: str) -> Dict[str, str]:
    mfile = tag_root / "manifest.yaml"
    if not mfile.exists():
        return {}
    try:
        return yaml.safe_load(mfile.read_text()) or {}
    except yaml.YAMLError as e:
        print(f"[plotter] WARN: couldn't parse manifest for {tag}: {e}",
              file=sys.stderr)
        return {}


def _disk_from_dfs(*dfs: pd.DataFrame) -> Optional[str]:
    seen = set()
    for df in dfs:
        if not df.empty and "disk" in df.columns:
            seen.update(str(v) for v in df["disk"].dropna().unique())
    if not seen:
        return None
    if len(seen) > 1:
        print(f"[plotter] WARN: mixed disk media across sources: {sorted(seen)}",
              file=sys.stderr)
    return sorted(seen)[0]


def load_diagram(name: str) -> SweepData:
    """Load a named diagram from diagrams.yaml.

    Reads all source tags, concatenates their summary CSVs (each row
    tagged with ``_source_tag``), and returns a ``SweepData`` whose
    ``figures_root`` points at ``paper-data/diagrams/``.

    The primary tag's manifest supplies commit/host metadata. When
    sources differ in commit SHA the footer shows ``commit varies``.
    """
    tag_paths = sources_for(name)
    tags = [p.name for p in tag_paths]

    all_headlines: List[pd.DataFrame] = []
    all_stats: List[pd.DataFrame] = []
    all_inversions: List[pd.DataFrame] = []
    all_diagnostics: List[pd.DataFrame] = []

    for tag, tag_root in zip(tags, tag_paths):
        csvs = _read_tag_csvs(tag_root, tag)
        all_headlines.append(csvs["headline.csv"])
        all_stats.append(csvs["stats.csv"])
        all_inversions.append(csvs["inversions.csv"])
        all_diagnostics.append(csvs["diagnostics.csv"])

    def _concat(frames: List[pd.DataFrame]) -> pd.DataFrame:
        non_empty = [f for f in frames if not f.empty]
        if not non_empty:
            return pd.DataFrame()
        return pd.concat(non_empty, ignore_index=True)

    headline = _concat(all_headlines)
    stats = _concat(all_stats)
    inversions = _concat(all_inversions)
    diagnostics = _concat(all_diagnostics)

    # Primary tag provides the manifest; note diverging commit SHAs.
    primary_manifest = _load_manifest(tag_paths[0], tags[0])
    if len(tags) > 1:
        commits = {t: _load_manifest(p, t).get("commit_sha", "")
                   for t, p in zip(tags, tag_paths)}
        unique_commits = set(v for v in commits.values() if v)
        if len(unique_commits) > 1:
            primary_manifest = dict(primary_manifest)
            primary_manifest["commit_sha"] = "varies"

    disk = _disk_from_dfs(headline, stats, inversions, diagnostics)

    DIAGRAMS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return SweepData(
        name=name,
        source_tags=tags,
        summary_root=tag_paths[0] / "summary",
        figures_root=DIAGRAMS_OUTPUT_DIR,
        headline=headline,
        stats=stats,
        inversions=inversions,
        diagnostics=diagnostics,
        manifest=primary_manifest,
        disk=disk,
    )


def load_sweep_for_diag(tag: str, root: Path) -> SweepData:
    """Load a single sweep tag for diagnostics builders (--diag-tag path).

    Unlike ``load_diagram``, this preserves the per-tag figures/ layout
    (``figures_root = <root>/figures/``) so diagnostic outputs stay
    co-located with the sweep they describe.
    """
    figures = root / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    csvs = _read_tag_csvs(root, tag)
    manifest = _load_manifest(root, tag)
    headline = csvs["headline.csv"]
    stats = csvs["stats.csv"]
    inversions = csvs["inversions.csv"]
    diagnostics = csvs["diagnostics.csv"]
    disk = _disk_from_dfs(headline, stats, inversions, diagnostics)
    return SweepData(
        name=tag,
        source_tags=[tag],
        summary_root=root / "summary",
        figures_root=figures,
        headline=headline,
        stats=stats,
        inversions=inversions,
        diagnostics=diagnostics,
        manifest=manifest,
        disk=disk,
    )


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
# Q10 / Q10I live in their own figure (paper_q10) so their 5-bar layout
# doesn't disrupt the 4-bar headline row geometry. They each carry an
# extra Mat-View bar (naive per-lineitem vs pre-agg per-order) which
# makes a 5-bar panel.
Q10_QUERY = "q10"
Q10I_QUERY = "q10i"

# Method → synthetic structure id for Q10/Q10I rows. The naive
# per-lineitem view keeps the canonical S2 (Mat-View) slot; the
# partial-aggregate variant goes to id 22. S5 (`mi_acoli_preagg`) is
# paper-deferred and dropped. Q10I's MI walker is named `mi_coli_walk`
# (COLI MI) vs Q10's `mi_col_walk` (COL MI); both map to S3.
Q10_METHOD_TO_STRUCT = {
    "base_merge_join":      1,
    "pipeline_view":        2,
    "pipeline_view_preagg": 22,
    "preagg_view":          22,  # Q10/Q10I S7 — partial-agg view (current method label)
    "mi_col_walk":          3,
    "mi_coli_walk":         3,
    "mi_acol_preagg":       33,
    "mi_acoli_preagg":      33,
    "base_hash_join":       4,
    "shared_view":          6,   # S6 shared COL union view (q10; q3/q5 too)
}
# Per-query bar order for the headline panels. Defaults to
# PAPER_LEGEND_ORDER (4 bars) when the query is not listed.
# Q10/Q10i layout: pair each canonical with its partial-agg variant
# so the two Mat-View bars sit together and the two Merged-Idx bars
# sit together at the right.
PAPER_PANEL_STRUCTURES = {
    # Q10/Q10i layout: pair each canonical with its partial-agg variant
    # so the two Mat-View bars sit together and the two Merged-Idx bars
    # sit together at the right. S6 (Mat-View shared) is deliberately
    # excluded from the paper figures.
    "q10":  [4, 1, 2, 22, 3, 33],
    "q10i": [4, 1, 2, 22, 3, 33],
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
PAPER_HEADLINE_CELL = "c4"
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
                     title: Optional[str] = None,
                     n_overflow: int = 1,
                     log_y: bool = False,
                     log_bottom: Optional[float] = None,
                     cap_override: Optional[float] = None) -> bool:
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
    for col in ("ms_q25", "ms_q75"):
        if col in sub.columns:
            sub[col.replace("ms_", "s_")] = sub[col] / 1000.0
    # One bar per structure, centred on x=0. Q10 has 5 bars (extra
    # Mat-View variant), so the per-bar width scales with the panel's
    # structure count to keep the cluster the same total width.
    n = len(panel_structs)
    bar_w = 0.72 / max(n, 1)
    drew = False
    # (x, height, struct, q25, q75) — quartiles drive the error bars.
    plotted: List[Tuple[float, float, int, float, float]] = []
    for k, struct in enumerate(panel_structs):
        row = sub[sub["structure"] == struct]
        if row.empty or pd.isna(row["s_median"].iloc[0]):
            continue
        x = (k - (n - 1) / 2) * bar_w
        h = float(row["s_median"].iloc[0])
        lo = float(row["s_q25"].iloc[0]) if "s_q25" in row.columns \
            and not pd.isna(row["s_q25"].iloc[0]) else h
        hi = float(row["s_q75"].iloc[0]) if "s_q75" in row.columns \
            and not pd.isna(row["s_q75"].iloc[0]) else h
        plotted.append((x, h, struct, lo, hi))
    # Pick the panel's y-cap from the (n_overflow+1)-th tallest bar (with
    # a small headroom factor) rather than as a multiple of the shortest.
    # ``n_overflow=1`` (default) lets a single outlier overflow; Q10/Q10i
    # uses ``n_overflow=2`` because Mat-View *and* Base-Hash are both
    # ~100s while the headline aCOL/aCOLI bar is sub-second, so a
    # 2nd-tallest cap would still squash everything visible.
    if cap_override is not None:
        # Caller forces a shared ceiling across a row of panels (used by
        # fig_paper_tpch_row so every bar — including would-be outliers —
        # fits inside the axes instead of being clipped + annotated).
        cap = cap_override
    elif plotted:
        finite = sorted((h for _, h, _, _, _ in plotted if h > 0), reverse=True)
        idx = min(n_overflow, max(len(finite) - 1, 0))
        if finite:
            cap = finite[idx] * 1.15
        else:
            cap = None
    else:
        cap = None
    # On log scale every bar fits naturally — no need to cap / overflow.
    # With cap_override the caller already added headroom; don't pad again.
    if log_y or cap is None:
        ylim_top = None
    elif cap_override is not None:
        ylim_top = cap
    else:
        ylim_top = cap * 1.08
    # Log axis needs a positive bar bottom; pick one decade below the
    # smallest positive bar so even sub-second bars render visibly.
    # Caller can pass an explicit ``log_bottom`` so a row of panels
    # shares the same anchor (otherwise per-panel mins drift and bars
    # in different panels look anchored at different floors).
    if log_y and log_bottom is None and plotted:
        positives = [h for _, h, _, _, _ in plotted if h > 0]
        if positives:
            log_bottom = min(positives) * 0.5
    # Bars under this fraction of the cap render as a thin sliver and
    # are effectively invisible; annotate their numeric value above the
    # sliver so the reader can still read the headline number.
    short_thresh = 0.04
    for x, h, struct, lo, hi in plotted:
        # Over-cap bars extend all the way to the top border; the
        # printed value sits just above the border so the reader can
        # still read the true number.
        draw_h = min(h, ylim_top) if ylim_top is not None else h
        color, hatch = _struct_style(struct)
        # Use facecolor+edgecolor instead of color= because matplotlib's
        # bar() lets a plain ``color`` arg override edgecolor, which
        # silently drops hatching even when hatch= is set.
        bar_kwargs = dict(facecolor=color, edgecolor=color, linewidth=0)
        if hatch is not None:
            # Hatch needs a visible edge to render; use a contrasting
            # darker edge so the hatch lines read on top of the fill.
            bar_kwargs.update(hatch=hatch, edgecolor="white", linewidth=0)
        if log_y and log_bottom is not None:
            # bar() expects (bottom, height) on a log axis to avoid
            # drawing from 0 (which is -inf in log space and confuses
            # the layout engine into producing a 100k-pixel figure).
            ax.bar([x], [draw_h - log_bottom], width=bar_w,
                   bottom=log_bottom, **bar_kwargs)
        else:
            ax.bar([x], [draw_h], width=bar_w,
                   clip_on=False, **bar_kwargs)
        if ylim_top is not None and h > ylim_top:
            ax.annotate(f"{h:.0f}", xy=(x, 1.0),
                        xycoords=("data", "axes fraction"),
                        xytext=(0, 2), textcoords="offset points",
                        ha="center", va="bottom", fontsize=10,
                        color=color, annotation_clip=False)
        elif ylim_top is not None and h > 0 and h < ylim_top * short_thresh:
            # Sub-second bars next to multi-tens-of-seconds bars: show
            # the value (one decimal for h<10s, integer otherwise) just
            # above the sliver so it doesn't disappear visually.
            fmt = f"{h:.1f}" if h < 10 else f"{h:.0f}"
            ax.annotate(fmt, xy=(x, draw_h),
                        xytext=(0, 2), textcoords="offset points",
                        ha="center", va="bottom", fontsize=8,
                        color=color, annotation_clip=False)
        # Error bars suppressed: 3 reps is too few for a meaningful
        # IQR — bars report the rep-median only.
        drew = True
    ax.set_xlabel(panel_title, fontsize=14)
    if show_ylabel:
        ax.set_ylabel("seconds / query", fontsize=12)
    ax.set_xticks([])
    if log_y:
        # Q10/Q10i: aCOL/aCOLI bars are 100-1000× smaller than the
        # giants, so linear scale leaves them invisible even with a
        # tight cap. Log scale lets every bar register at the cost
        # of compressing the upper end.
        ax.set_yscale("log")
        positive = [h for _, h, _, _, _ in plotted if h > 0]
        if positive:
            lo = log_bottom if log_bottom is not None else min(positive) * 0.5
            hi = max(positive) * 1.8
            ax.set_ylim(lo, hi)
    elif ylim_top is not None:
        ax.set_ylim(0, ylim_top)
    else:
        ax.set_ylim(bottom=0)
    if not log_y:
        # MaxNLocator forces linear-spaced ticks; combining it with a
        # log axis can produce a degenerate tick set (and blew up
        # constrained_layout to absurd heights). Let matplotlib's
        # default LogLocator handle log-scale ticks.
        ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5))
    ax.tick_params(axis="y", which="major", labelsize=11)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    return drew


def _add_two_row_legend(fig, legend_structs: Sequence[int],
                        *, fontsize: int,
                        bbox_main: Tuple[float, float],
                        bbox_variants: Tuple[float, float],
                        bbox_main_no_variants: Optional[Tuple[float, float]]
                        = None) -> None:
    """Render the headline legend across two rows: canonical S1..S4 on
    top, partial-agg variants (synthetic ids ≥ 22) on a separate row
    below. matplotlib's column-major fill orders entries by column so a
    single ``fig.legend`` with ``ncol=4`` would scramble row 1; using
    two legends keeps the two groups aligned independently.

    When ``legend_structs`` has no variants, the second row is omitted
    and the main row drops to ``bbox_main_no_variants`` (closer to the
    panel) — there's no second row to clear above the cut-off bar
    value annotations.
    """
    main = [s for s in legend_structs if s in PAPER_LEGEND_ORDER]
    variants = [s for s in legend_structs if s not in PAPER_LEGEND_ORDER]
    def _swatch(s: int) -> plt.Rectangle:
        color, hatch = _struct_style(s)
        kwargs = dict(facecolor=color, edgecolor=color,
                      label=PAPER_STRUCTURE_LABELS[s])
        if hatch is not None:
            kwargs.update(hatch=hatch, edgecolor="white", linewidth=0)
        return plt.Rectangle((0, 0), 1, 1, **kwargs)

    main_handles = [_swatch(s) for s in main]
    bbox_for_main = bbox_main if variants else (
        bbox_main_no_variants if bbox_main_no_variants is not None
        else bbox_variants)
    fig.legend(handles=main_handles, loc="upper center",
               ncol=len(main_handles) or 1,
               fontsize=fontsize, bbox_to_anchor=bbox_for_main,
               frameon=False, columnspacing=1.5, handletextpad=0.4)
    if not variants:
        return
    variant_handles = [_swatch(s) for s in variants]
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



def fig_paper_tpch_row(data: SweepData, backend: str,
                       include_legend: bool) -> Optional[Path]:
    """1×4 row of (q3, q5, q3i, q5i) for one backend, single-column-wide.

    The btree and lsm figures are designed to be the same width and
    panel geometry so the writer can stack them vertically in LaTeX and
    have axes line up panel-by-panel. ``include_legend=True`` attaches a
    single compact legend above the figure; the sibling figure should
    set it False so vertical space isn't duplicated.

    Q10/Q10I rows come from the concatenated ``data.headline`` frame —
    they are spliced in at load time when the diagram YAML lists the
    Q10 sibling tag as a source, so no per-builder augmentation is needed.
    """
    assert backend in ("btree", "lsm")
    head = data.headline[data.headline["family"].isin(["vanilla", "tpchi"])
                         & (data.headline["backend"] == backend)
                         & (data.headline["tx"] == "query")]
    if head.empty:
        return None
    # Map method names to synthetic structure ids for Q10/Q10I rows that
    # carry raw method strings rather than pre-assigned structure ints.
    if "method" in head.columns:
        mask = head["method"].isin(Q10_METHOD_TO_STRUCT)
        if mask.any():
            head = head.copy()
            mapped = head.loc[mask, "method"].map(Q10_METHOD_TO_STRUCT)
            head.loc[mask, "structure"] = mapped.astype(int)
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])
    binaries = [f"{q}_{backend}" for q in PAPER_TPCH_QUERIES]
    n_panels = len(binaries)
    # Author the figure at full-page width (~6.5") so each panel has
    # room for tick labels + line markers; LaTeX scales it down to
    # column width at \includegraphics time. constrained_layout
    # handles the external legend bbox without leaving stray
    # whitespace that tight_layout sometimes does with sharey=False.
    fig, axes = plt.subplots(1, n_panels, figsize=(2.1 * n_panels, 1.5),
                             sharey=False, constrained_layout=True)
    # Compute a shared y-ceiling so all four panels clip at the same
    # height. Per-panel logic clips the top n_overflow bars (default 1):
    # drop each panel's top-n_overflow heights, then take the row-wide
    # max of the remainder (+15% headroom). This preserves the
    # intentional outlier-overflow behaviour while ensuring every panel
    # uses an identical cap.
    panel_overflow = 1
    panel_caps: List[float] = []
    for binary in binaries:
        panel_sub = ms_df[(ms_df["binary"] == binary)
                          & (ms_df["cell"] == PAPER_HEADLINE_CELL)
                          & (ms_df["bg"] == PAPER_HEADLINE_BG)]
        heights = sorted((float(v) / 1000.0 for v in panel_sub["ms_median"]
                          if v == v and v > 0), reverse=True)
        if not heights:
            continue
        idx = min(panel_overflow, len(heights) - 1)
        panel_caps.append(heights[idx])
    cap_override = max(panel_caps) * 1.15 if panel_caps else None
    has_any = False
    legend_structs: List[int] = list(PAPER_LEGEND_ORDER)
    for j, (binary, q) in enumerate(zip(binaries, PAPER_TPCH_QUERIES)):
        panel_structs = PAPER_PANEL_STRUCTURES.get(q, PAPER_LEGEND_ORDER)
        for s in panel_structs:
            if s not in legend_structs:
                legend_structs.append(s)
        drew = _paper_bar_panel(axes[j], ms_df, binary, PAPER_HEADLINE_CELL,
                                show_ylabel=(j == 0),
                                structures=panel_structs,
                                cap_override=cap_override)
        has_any = has_any or drew
    if not has_any:
        plt.close(fig)
        return None
    # cap_override already enforces shared y-limits; just hide tick
    # labels on non-leftmost panels (ticks themselves stay).
    for j, ax in enumerate(axes):
        ax.tick_params(axis="y", labelleft=(j == 0))
    if include_legend:
        # Legend sits well above the panel top so cut-off bar value
        # annotations (drawn at axes-fraction y=1.0) don't collide
        # with the second row. When no variants are present, the main
        # row drops closer to the panel since there's nothing below
        # it competing with the over-cap labels.
        # Bumped up after the figure was shortened by 30% — at 1.82"
        # tall, the over-cap value annotations (drawn at axes-fraction
        # y=1.0) collide with a legend anchored at y=1.12 / 1.16.
        _add_two_row_legend(fig, legend_structs, fontsize=13,
                            bbox_main=(0.5, 1.42),
                            bbox_variants=(0.5, 1.24),
                            bbox_main_no_variants=(0.5, 1.26))
    dest = data.figures_root / data.paper_name(f"paper_tpch_{backend}_headline")
    return _save(fig, dest, data.footer, include_footer=False)[0]


def fig_paper_q10(data: SweepData) -> Optional[Path]:
    """1×4 dedicated Q10/Q10I figure: Q10 btree | Q10 lsm | Q10i btree |
    Q10i lsm. Both queries carry an extra Mat-View bar (naive per-lineitem
    vs pre-agg per-order) which makes a 5-bar panel; keeping them out of
    the four-query headline row preserves that row's regular 4-bar geometry.

    Q10/Q10I rows arrive already in ``data.headline`` — the diagram YAML
    lists the Q10 sibling tag as a source so load_diagram concatenates it.
    """
    head = data.headline[
        data.headline["query"].isin([Q10_QUERY, Q10I_QUERY])
        & data.headline["tx"].eq("query")
    ].copy()
    # Map raw method strings to synthetic structure ids.
    if "method" in head.columns:
        mask = head["method"].isin(Q10_METHOD_TO_STRUCT)
        if mask.any():
            head.loc[mask, "structure"] = (
                head.loc[mask, "method"].map(Q10_METHOD_TO_STRUCT).astype(int)
            )
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])

    # 1×4 row: Q10 btree | Q10 lsm | Q10i btree | Q10i lsm. Single row
    # keeps the figure narrow enough to slot under the main headline
    # row in LaTeX while still showing both queries × both backends.
    panels = [
        (Q10_QUERY,  "btree", "Q10 (B-tree)"),
        (Q10_QUERY,  "lsm",   "Q10 (LSM-tree)"),
        (Q10I_QUERY, "btree", "Q10i (B-tree)"),
        (Q10I_QUERY, "lsm",   "Q10i (LSM-tree)"),
    ]
    fig, axes = plt.subplots(1, len(panels), figsize=(2.1 * len(panels), 1.96),
                             sharey=False, constrained_layout=True)
    # Shared log-axis bottom across all panels so every bar is anchored
    # at the same floor — without this, panel-local mins drift and bars
    # look like they start from different heights.
    pos = ms_df["ms_median"][ms_df["ms_median"] > 0] / 1000.0
    shared_log_bottom = float(pos.min()) * 0.5 if not pos.empty else None
    drew_any = False
    legend_structs: List[int] = list(PAPER_LEGEND_ORDER)
    for j, (query, backend, title) in enumerate(panels):
        panel_structs = PAPER_PANEL_STRUCTURES.get(query, PAPER_LEGEND_ORDER)
        for s in panel_structs:
            if s not in legend_structs:
                legend_structs.append(s)
        binary = f"{query}_{backend}"
        drew = _paper_bar_panel(axes[j], ms_df, binary,
                                PAPER_HEADLINE_CELL,
                                show_ylabel=(j == 0),
                                structures=panel_structs,
                                title=title,
                                n_overflow=2,
                                log_y=True,
                                log_bottom=shared_log_bottom)
        drew_any = drew_any or drew
    if not drew_any:
        plt.close(fig)
        return None

    # Share y-limits across all panels so bar heights are directly
    # comparable. Tick marks stay on every panel; tick *labels* only
    # render on the leftmost panel since the scale is shared and
    # repeating them would just eat horizontal space.
    los = [ax.get_ylim()[0] for ax in axes if ax.get_ylim()[1] > 0]
    his = [ax.get_ylim()[1] for ax in axes if ax.get_ylim()[1] > 0]
    if los and his:
        shared_lo, shared_hi = min(los), max(his)
        for ax in axes:
            ax.set_ylim(shared_lo, shared_hi)
    for j, ax in enumerate(axes):
        ax.tick_params(axis="y", labelleft=(j == 0))

    # Bumped up after the figure was shortened by 30% — keep clear of
    # the over-cap value annotations sitting at axes-fraction y=1.0.
    _add_two_row_legend(fig, legend_structs, fontsize=13,
                        bbox_main=(0.5, 1.40),
                        bbox_variants=(0.5, 1.22))

    dest = data.figures_root / data.paper_name("paper_q10")
    return _save(fig, dest, data.footer, include_footer=False)[0]


def fig_paper_tpch_vanilla(data: SweepData) -> Optional[Path]:
    """2×3 grid of TPC-H vanilla-only headline results: rows = (B-tree,
    LSM-tree), columns = (Q3, Q5, Q10). No Qxi variants. Q3/Q5/Q10 all
    come from the concatenated ``data.headline`` frame — the diagram YAML
    lists the Q10 sibling tag as a source so load_diagram splices it in.
    Log y-axis throughout so Q10's giant Mat-View / Base-Hash bars don't
    squash Q3/Q5's headline numbers.
    """
    queries = ["q3", "q5", "q10"]
    backends = ["btree", "lsm"]
    head = data.headline[data.headline["family"].eq("vanilla")
                         & data.headline["tx"].eq("query")].copy()
    # Map method → structure for Q10 rows.
    if "method" in head.columns:
        mask = head["method"].isin(Q10_METHOD_TO_STRUCT)
        if mask.any():
            head.loc[mask, "structure"] = (
                head.loc[mask, "method"].map(Q10_METHOD_TO_STRUCT).astype(int)
            )
    if head.empty:
        return None
    ms_df = aggregate_ms_per_query(
        head, group_cols=["binary", "cell", "structure", "bg"])

    n_rows, n_cols = len(backends), len(queries)
    fig, axes = plt.subplots(n_rows, n_cols,
                             figsize=(2.1 * n_cols, 1.96 * n_rows),
                             sharey=False, constrained_layout=True)
    # Q3/Q5: linear y; Q10: log y (its Mat-View / Base-Hash bars are
    # 100-1000× the partial-agg headline). Each panel keeps its own
    # tick labels so the linear/log scales remain readable.
    log_y_for = {"q3": False, "q5": False, "q10": True}
    drew_any = False
    legend_structs: List[int] = list(PAPER_LEGEND_ORDER)
    for r, backend in enumerate(backends):
        for c, q in enumerate(queries):
            panel_structs = PAPER_PANEL_STRUCTURES.get(q, PAPER_LEGEND_ORDER)
            for s in panel_structs:
                if s not in legend_structs:
                    legend_structs.append(s)
            binary = f"{q}_{backend}"
            backend_label = "B-tree" if backend == "btree" else "LSM-tree"
            title = f"{q.upper()} ({backend_label})"
            drew = _paper_bar_panel(axes[r, c], ms_df, binary,
                                    PAPER_HEADLINE_CELL,
                                    show_ylabel=(c == 0),
                                    structures=panel_structs,
                                    title=title,
                                    n_overflow=1,
                                    log_y=log_y_for[q])
            drew_any = drew_any or drew
    if not drew_any:
        plt.close(fig)
        return None

    # Don't share y-limits across panels — Q3/Q5 are linear (seconds in
    # the tens) and Q10 is log (sub-second to ~hundreds). Show tick
    # labels on every panel so each scale reads independently.
    for r in range(n_rows):
        for c in range(n_cols):
            axes[r, c].tick_params(axis="y", labelleft=True)

    _add_two_row_legend(fig, legend_structs, fontsize=13,
                        bbox_main=(0.5, 1.20),
                        bbox_variants=(0.5, 1.10))

    dest = data.figures_root / data.paper_name("paper_tpch_vanilla")
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
    dest = data.figures_root / data.paper_name(f"paper_tpch_{backend}_memory_pressure")
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
    dest = data.figures_root / data.paper_name("paper_geo_condensed")
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


# ---------------------------------------------------------------------------
# SSD-only single-cell diagnostics. The plotter's diagnostics-explore mode
# emits line plots across PAPER_CELLS = [c2,c1,c0,c3]; the SSD tag only
# populates c0, so those line plots collapse to single-point series. The
# bar-grid builders below render the same counters as compact 1×4 rows for
# the single-cell SSD snapshot.
# ---------------------------------------------------------------------------

# Reference CPU clock used to convert cpu_cycles_per_tx → equivalent
# seconds; matches the c220g2 Xeon E5-2660 v3 nominal 2.6 GHz that the
# rest of the paper-data scripts assume.
SSD_DIAG_CPU_GHZ = 2.6


def _ssd_diag_aggregate(data: SweepData, backend: str,
                        cols: Sequence[str]) -> pd.DataFrame:
    """Median across reps for the paper cohort (cell=c0, bg=2, S1..S4)."""
    diag = _diag_filter(data.diagnostics)
    diag = diag[diag["backend"] == backend]
    if diag.empty:
        return diag
    keep = ["query", "structure"] + [c for c in cols if c in diag.columns]
    if len(keep) == 2:
        return pd.DataFrame()
    return (diag[keep]
            .groupby(["query", "structure"])
            .median(numeric_only=True).reset_index())


def _ssd_bar_panel(ax, agg: pd.DataFrame, query: str,
                   metric_specs: Sequence[Tuple[str, str, float, str]],
                   show_ylabel: bool, secondary_metric: Optional[str] = None,
                   secondary_label: Optional[str] = None,
                   secondary_log: bool = False) -> bool:
    """Render one panel: grouped bars per structure × metric. Up to one
    metric may go on a secondary right-side axis (used for the cache-
    profile figure where LLC misses and dt_split live on different
    scales)."""
    sub = agg[agg["query"] == query]
    if sub.empty:
        _all_or_empty(plt.gcf(), ax, "—")
        ax.set_xlabel(_query_title(f"{query}_btree"), fontsize=14)
        return False

    structs = [s for s in PAPER_LEGEND_ORDER]
    n_metrics = len(metric_specs)
    group_w = 0.78
    bar_w = group_w / max(n_metrics, 1)
    x_pos = np.arange(len(structs))
    drew = False
    sec_ax = ax.twinx() if secondary_metric else None
    for mi, (col, _, scale, _) in enumerate(metric_specs):
        if col not in sub.columns:
            continue
        target = sec_ax if (secondary_metric and col == secondary_metric) else ax
        offset = (mi - (n_metrics - 1) / 2) * bar_w
        for k, s in enumerate(structs):
            row = sub[sub["structure"] == s]
            if row.empty:
                continue
            val = row[col].iloc[0]
            if pd.isna(val):
                continue
            # Bar tint comes from the structure (carries paper colour
            # vocabulary); the metric is encoded by horizontal position
            # within the cluster — caption / legend explains the order.
            target.bar([x_pos[k] + offset], [val / scale], bar_w,
                       color=STYLE["structure_colors"][s], linewidth=0,
                       alpha=1.0 if mi == 0 else 0.55,
                       hatch="" if mi == 0 else "//")
            drew = True

    ax.set_xticks(x_pos)
    ax.set_xticklabels([PAPER_STRUCTURE_LABELS[s] for s in structs],
                       fontsize=8, rotation=30, ha="right",
                       rotation_mode="anchor")
    ax.set_xlim(-0.6, len(structs) - 0.4)
    if show_ylabel and metric_specs:
        ax.set_ylabel(metric_specs[0][1], fontsize=10)
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=5))
    ax.tick_params(axis="y", labelsize=8)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    if sec_ax is not None:
        sec_ax.set_ylim(bottom=0)
        if secondary_log:
            sec_ax.set_yscale("symlog", linthresh=1.0)
        if secondary_label:
            sec_ax.set_ylabel(secondary_label, fontsize=9, color="#444")
        sec_ax.tick_params(axis="y", labelsize=7, colors="#444")
    ax.set_title(_query_title(f"{query}_btree"), fontsize=12,
                 y=-0.45)
    return drew


def _save_ssd_diag(data: SweepData, fig, basename: str) -> Optional[Path]:
    dest = data.figures_root / "diagnostics" / data.paper_name(basename)
    return _save(fig, dest, data.footer, include_footer=False)[0]


def fig_diag_ssd_lsm_breakdown(data: SweepData) -> Optional[Path]:
    """1×4 LSM CPU + I/O bar grid. Per query, four structure clusters;
    within each cluster, the left bar is equivalent CPU seconds
    (cpu_cycles / 2.6 GHz) and the right bar is parallel SST read
    seconds (sst_read_us / 1e6, summed across reader threads). Backs
    the §subsec:btree_vs_lsm claim that Mat-View's LSM win is CPU-side.
    """
    metrics = [
        ("cpu_cycles_per_tx",  "seconds / query",
         SSD_DIAG_CPU_GHZ * 1e9, "CPU equivalent"),
        ("sst_read_us_per_tx", "seconds / query", 1e6, "SST read parallel"),
    ]
    agg = _ssd_diag_aggregate(data, "lsm",
                              [m[0] for m in metrics])
    if agg.empty:
        return None
    fig, axes = plt.subplots(1, 4, figsize=(8.5, 2.8), sharey=False,
                             constrained_layout=True)
    drew_any = False
    for j, q in enumerate(PAPER_TPCH_QUERIES):
        drew = _ssd_bar_panel(axes[j], agg, q, metrics,
                              show_ylabel=(j == 0))
        drew_any = drew_any or drew
    if not drew_any:
        plt.close(fig)
        return None
    # Two-tone legend: bar tint = structure (colours), hatch / alpha =
    # which counter the bar represents.
    metric_handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor="#7f7f7f", alpha=1.0,
                      label=metrics[0][3]),
        plt.Rectangle((0, 0), 1, 1, facecolor="#7f7f7f", alpha=0.55,
                      hatch="//", label=metrics[1][3]),
    ]
    fig.legend(handles=metric_handles, loc="upper center", ncol=2,
               fontsize=10, bbox_to_anchor=(0.5, 1.10),
               frameon=False, columnspacing=1.5, handletextpad=0.4)
    return _save_ssd_diag(data, fig, "diag_ssd_lsm_breakdown")


def fig_diag_ssd_btree_cache_profile(data: SweepData) -> Optional[Path]:
    """1×4 B-tree cache-profile panel. Left axis: LLC miss per tx
    (primary bar); right axis: dt_struct_split (secondary, symlog).
    Mat-View and Merged-Idx should track each other tightly on both
    counters, separating cleanly from Base-Hash and Base-Merge.
    """
    metrics = [
        ("cpu_llc_miss_per_tx", "LLC miss / tx", 1.0, "LLC miss"),
        ("dt_struct_split",     "",              1.0, "B-tree splits"),
    ]
    agg = _ssd_diag_aggregate(data, "btree", [m[0] for m in metrics])
    if agg.empty:
        return None
    fig, axes = plt.subplots(1, 4, figsize=(8.5, 2.8), sharey=False,
                             constrained_layout=True)
    drew_any = False
    for j, q in enumerate(PAPER_TPCH_QUERIES):
        drew = _ssd_bar_panel(axes[j], agg, q, metrics,
                              show_ylabel=(j == 0),
                              secondary_metric="dt_struct_split",
                              secondary_label="B-tree splits",
                              secondary_log=True)
        drew_any = drew_any or drew
    if not drew_any:
        plt.close(fig)
        return None
    metric_handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor="#7f7f7f", alpha=1.0,
                      label=metrics[0][3] + " (left)"),
        plt.Rectangle((0, 0), 1, 1, facecolor="#7f7f7f", alpha=0.55,
                      hatch="//", label=metrics[1][3] + " (right)"),
    ]
    fig.legend(handles=metric_handles, loc="upper center", ncol=2,
               fontsize=10, bbox_to_anchor=(0.5, 1.10),
               frameon=False, columnspacing=1.5, handletextpad=0.4)
    return _save_ssd_diag(data, fig, "diag_ssd_btree_cache_profile")


def fig_diag_ssd_lsm_sst_path(data: SweepData) -> Optional[Path]:
    """1×4 LSM SST-path panel. sst_read (per tx, parallel) on the
    left axis; sst_compaction (per run total) on the right axis via
    twinx. The two metrics live in different magnitudes (read µs/tx
    vs cumulative compaction µs across all background threads), so a
    shared axis would squash one of them — twinx keeps both readable.
    Every panel shows tick labels on both sides so any panel can be
    read standalone.
    """
    metrics = [
        ("sst_read_us_per_tx", "SST read (s / tx)",       1e6, "SST read"),
        ("sst_compaction_us",  "SST compaction (s)",      1e6, "SST compaction"),
    ]
    agg = _ssd_diag_aggregate(data, "lsm", [m[0] for m in metrics])
    if agg.empty:
        return None
    # Restrict to Q3/Q3i: the SST-path story is cleanest on the
    # 2-table-deep pipelines (Q5/Q5i add cross-table I/O that muddies
    # the read-vs-compaction split). Keeps the figure narrower too.
    diag_queries = ["q3", "q3i"]
    n_panels = len(diag_queries)
    # Wide-and-short layout: 4 xtick labels per panel sit horizontally
    # (no rotation) and need ~1.4" of width each to keep
    # \textsc{Base-Merge} / \textsc{Merged-Idx} from overlapping.
    fig, axes = plt.subplots(1, n_panels,
                             figsize=(3.5 * n_panels, 2.2),
                             sharey=False, constrained_layout=True)
    if n_panels == 1:
        axes = [axes]
    drew_any = False
    sec_axes: List = []
    for j, q in enumerate(diag_queries):
        # Left ylabel only on the first panel; right ylabel only on
        # the last panel (set via the secondary axis after creation).
        drew = _ssd_bar_panel(axes[j], agg, q, metrics,
                              show_ylabel=(j == 0),
                              secondary_metric="sst_compaction_us",
                              secondary_label=(metrics[1][1]
                                               if j == n_panels - 1
                                               else None))
        twins = [a for a in fig.axes if a is not axes[j]
                 and a.bbox.bounds == axes[j].bbox.bounds]
        sec_axes.append(twins[0] if twins else None)
        drew_any = drew_any or drew
        # Override the shared panel's 30° rotation: with only 4 labels
        # and a wide panel, horizontal reads cleaner.
        for lbl in axes[j].get_xticklabels():
            lbl.set_rotation(0)
            lbl.set_ha("center")
            lbl.set_rotation_mode("default")
    if not drew_any:
        plt.close(fig)
        return None

    # Share y-limits across panels so bar heights are comparable;
    # tick labels appear only at the left edge of panel 0 (primary)
    # and the right edge of the last panel (secondary) so the strip
    # reads as one shared scale rather than four independent axes.
    primary_max = max((ax.get_ylim()[1] for ax in axes), default=1.0)
    secondary_max = max((a.get_ylim()[1] for a in sec_axes if a is not None),
                        default=1.0)
    for j, (ax, sec) in enumerate(zip(axes, sec_axes)):
        ax.set_ylim(0, primary_max)
        ax.tick_params(axis="y", left=True,
                       labelleft=(j == 0), labelsize=8)
        if sec is not None:
            sec.set_ylim(0, secondary_max)
            sec.tick_params(axis="y", right=True,
                            labelright=(j == n_panels - 1),
                            labelsize=7, colors="#444")

    metric_handles = [
        plt.Rectangle((0, 0), 1, 1, facecolor="#7f7f7f", alpha=1.0,
                      label=metrics[0][3] + " (left)"),
        plt.Rectangle((0, 0), 1, 1, facecolor="#7f7f7f", alpha=0.55,
                      hatch="//", label=metrics[1][3] + " (right)"),
    ]
    fig.legend(handles=metric_handles, loc="upper center", ncol=2,
               fontsize=10, bbox_to_anchor=(0.5, 1.10),
               frameon=False, columnspacing=1.5, handletextpad=0.4)
    return _save_ssd_diag(data, fig, "diag_ssd_lsm_sst_path")


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

# ---------------------------------------------------------------------------
# Refresh-plotter shims
# ---------------------------------------------------------------------------
#
# The refresh figures have their own data shapes and standalone plotters
# (plot_refresh_lsm_vs_btree.py, plot_refresh_sales.py). These thin
# wrappers let --all in this script dispatch to their main() functions
# so every diagrams.yaml entry is reachable from a single CLI.

def _refresh_shim(script_name: str, diagram_name: str) -> Optional[Path]:
    """Invoke a refresh standalone script's main() with --diagram <name>."""
    import importlib
    import sys as _sys
    old_argv = _sys.argv[:]
    _sys.argv = [script_name, "--diagram", diagram_name]
    try:
        mod = importlib.import_module(script_name.replace(".py", ""))
        mod.main()
    except SystemExit:
        pass
    except Exception as e:
        print(f"[plotter] ERROR in {script_name}: {e}", file=_sys.stderr)
        return None
    finally:
        _sys.argv = old_argv
    # Return the primary output path (stem only; _save handles suffix).
    return output_path(diagram_name).with_suffix(".pdf")


def _fig_refresh_lsm_vs_btree(data: SweepData) -> Optional[Path]:
    return _refresh_shim("plot_refresh_lsm_vs_btree", data.name)


def _fig_refresh_sales(data: SweepData) -> Optional[Path]:
    return _refresh_shim("plot_refresh_sales", data.name)


# ---------------------------------------------------------------------------
# Builder registry
# ---------------------------------------------------------------------------
#
# BUILDERS maps the builder name (used in diagrams.yaml's ``builder:``
# field) to a callable that accepts a ``SweepData`` and returns an
# ``Optional[Path]``.
#
# Paper-mode builders produce output under paper-data/diagrams/; the
# diagram name is the output stem. Diagnostics builders are invoked via
# the separate --diag-tag path and output under <tag>/figures/diagnostics/.

BUILDERS: Dict[str, Callable[[SweepData], Optional[Path]]] = {
    # Paper-mode builders (typeset-ready, bg=2 only, S1-S4 only).
    # Registered under the names used in diagrams.yaml's builder: field.
    "paper_tpch_row_btree":    lambda d: fig_paper_tpch_row(d, "btree", include_legend=True),
    "paper_tpch_row_lsm":      lambda d: fig_paper_tpch_row(d, "lsm",   include_legend=False),
    "paper_tpch_memory_btree": lambda d: fig_paper_memory_pressure(d, "btree", include_legend=True),
    "paper_tpch_memory_lsm":   lambda d: fig_paper_memory_pressure(d, "lsm",   include_legend=False),
    "paper_q10":               fig_paper_q10,
    "paper_tpch_vanilla":      fig_paper_tpch_vanilla,
    "paper_geo_condensed":     fig_paper_geo_condensed,
    # Refresh figures — dispatched to the standalone refresh plotters.
    # Both builder names are used by diagrams.yaml entries.
    "refresh_lsm_vs_btree":    _fig_refresh_lsm_vs_btree,
    "refresh_sales":           _fig_refresh_sales,
    # Diagnostics — invoked via --diag-tag, not via YAML diagrams.
    "diag_btree_llc_miss":      fig_diag_btree_llc_miss,
    "diag_btree_bm_rounds":     fig_diag_btree_bm_rounds,
    "diag_btree_dt_split":      fig_diag_btree_dt_split,
    "diag_lsm_sst_read":        fig_diag_lsm_sst_read,
    "diag_lsm_sst_compaction":  fig_diag_lsm_sst_compaction,
    "diag_lsm_cpu_cycles":      fig_diag_lsm_cpu_cycles,
    "diag_ssd_lsm_breakdown":       fig_diag_ssd_lsm_breakdown,
    "diag_ssd_btree_cache_profile":  fig_diag_ssd_btree_cache_profile,
    "diag_ssd_lsm_sst_path":        fig_diag_ssd_lsm_sst_path,
}

# Keep FIGURE_BUILDERS as an alias so any external code that imported it
# directly (e.g. experiments/run_paper_sweep.sh subshell imports) keeps
# working without an immediate update.
FIGURE_BUILDERS = BUILDERS

DIAG_BUILDERS = [
    "diag_btree_llc_miss", "diag_btree_bm_rounds", "diag_btree_dt_split",
    "diag_lsm_sst_read", "diag_lsm_sst_compaction", "diag_lsm_cpu_cycles",
    "diag_ssd_lsm_breakdown", "diag_ssd_btree_cache_profile",
    "diag_ssd_lsm_sst_path",
]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)

    # Paper-diagram mode: reads sources from diagrams.yaml.
    diagram_group = p.add_mutually_exclusive_group()
    diagram_group.add_argument(
        "--diagram", default=None,
        metavar="NAME[,NAME,...]",
        help="build specific diagram(s) by YAML name, e.g. paper_tpch_btree_headline")
    diagram_group.add_argument(
        "--all", action="store_true",
        help="build every diagram listed in diagrams.yaml")

    # Diagnostics mode: keeps the old per-sweep-tag layout.
    p.add_argument("--diag-tag", default=None,
                   metavar="TAG",
                   help="build diagnostics figures for a single sweep tag "
                        "(output goes to paper-data/<tag>/figures/diagnostics/)")
    p.add_argument("--diag-root", type=Path, default=None,
                   help="paper-data/<tag>/ root for --diag-tag "
                        "(default: paper-data/<tag> relative to cwd)")
    p.add_argument("--diag-figures", default=None,
                   metavar="NAME[,NAME,...]",
                   help="comma list of diag figure names; default = all diag builders")

    p.add_argument("--format", default="pdf", choices=["pdf", "png", "svg", "pgf"],
                   help="primary output format; pdf also emits a PNG sibling")
    args = p.parse_args()

    if args.format == "pgf":
        matplotlib.rcParams.update({
            "pgf.texsystem": "pdflatex",
            "font.family": "serif",
            "text.usetex": False,
            "pgf.rcfonts": False,
        })

    # --- diagnostics path ---
    if args.diag_tag:
        root = args.diag_root or (Path.cwd() / "paper-data" / args.diag_tag)
        if not root.exists():
            print(f"[plotter] error: {root} does not exist", file=sys.stderr)
            return 1
        data = load_sweep_for_diag(args.diag_tag, root)
        diag_names = (
            [n.strip() for n in args.diag_figures.split(",") if n.strip()]
            if args.diag_figures else DIAG_BUILDERS
        )
        unknown = [n for n in diag_names if n not in BUILDERS]
        if unknown:
            print(f"[plotter] unknown diag figure(s): {unknown}", file=sys.stderr)
            return 1
        emitted: List[Path] = []
        for name in diag_names:
            try:
                out = BUILDERS[name](data)
            except Exception as e:
                print(f"[plotter] ERROR while building {name}: {e}",
                      file=sys.stderr)
                continue
            if out is None:
                print(f"[plotter] {name}: skipped (no data)", file=sys.stderr)
            else:
                emitted.append(out)
                print(f"[plotter] wrote {out}", file=sys.stderr)
        print(f"[plotter] done: {len(emitted)} diag figures in {data.figures_root}",
              file=sys.stderr)
        return 0

    # --- paper diagram path ---
    if not args.diagram and not args.all:
        p.error("specify --diagram <name>[,name,...] or --all "
                "(or --diag-tag <tag> for per-sweep diagnostics)")

    meta = load_metadata()
    if args.all:
        diagram_names = list(meta.keys())
    else:
        diagram_names = [n.strip() for n in args.diagram.split(",") if n.strip()]
        unknown = [n for n in diagram_names if n not in meta]
        if unknown:
            print(f"[plotter] unknown diagram(s) in YAML: {unknown}",
                  file=sys.stderr)
            return 1

    emitted = []
    for diag_name in diagram_names:
        builder_name = meta[diag_name].get("builder", "")
        if builder_name not in BUILDERS:
            print(f"[plotter] WARN: no builder '{builder_name}' for "
                  f"diagram '{diag_name}' — skipped", file=sys.stderr)
            continue
        try:
            data = load_diagram(diag_name)
        except Exception as e:
            print(f"[plotter] ERROR loading diagram '{diag_name}': {e}",
                  file=sys.stderr)
            continue
        try:
            out = BUILDERS[builder_name](data)
        except Exception as e:
            print(f"[plotter] ERROR while building '{diag_name}' "
                  f"(builder={builder_name}): {e}", file=sys.stderr)
            continue
        if out is None:
            print(f"[plotter] {diag_name}: skipped (no data)", file=sys.stderr)
        else:
            emitted.append(out)
            print(f"[plotter] wrote {out}", file=sys.stderr)

    print(f"[plotter] done: {len(emitted)} diagrams in {DIAGRAMS_OUTPUT_DIR}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
