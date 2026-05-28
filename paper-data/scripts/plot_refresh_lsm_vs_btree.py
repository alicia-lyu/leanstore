#!/usr/bin/env python3
"""Plot refresh_sales LSM-vs-btree throughput across 5L (1.0 GiB DRAM) and
5H (0.4 GiB DRAM) memory budgets.

Reads ``paper-data/<tag-5L>/summary/refresh_sales_5L_throughput.csv`` and
``paper-data/<tag-5H>/summary/refresh_sales_5H_throughput.csv``, both at
the same SF, and emits a 1x2 grouped-bar figure under

    paper-data/<tag-5L>/figures/paper/refresh_lsm_vs_btree_5L_5H_ssd.{pdf,png}

Layout: left panel = btree, right panel = lsm. Each panel has four
structure clusters (S1 split, S2 view, S3 merged) with two bars per
cluster (5L solid, 5H hatched). Y axis is absolute RF1+RF2 pair
throughput (pairs/s, tail-30s window), linear scale per the paper
convention.

S4 (base-only) is intentionally omitted: the H2 claim this figure
supports is "merged-index maintenance is as fast as traditional
secondary indexes (S1)", and S4 maintains no secondaries at all, so
it sits in a different baseline class.

Story this figure carries: LSM throughput barely moves under memory
pressure (direct I/O, already SSD-bound), while btree degrades sharply
- and the S3-vs-S1 gap stays narrow at both budgets, supporting H2.
DBToaster is intentionally absent here; this figure is about
within-LeanStore engine + memory-pressure interaction.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams.update({
    "text.usetex": True,
    "font.family": "serif",
    "text.latex.preamble": r"\usepackage{lmodern}",
})
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import yaml

from _diagram_metadata import sources_for, output_path
from plot_paper_sweep import STYLE


METRIC_COL = "pair_tps_tail30"
STRUCTURES: List[int] = [1, 2, 3]
BACKENDS: List[Tuple[str, str]] = [("btree", "B-trees"),
                                    ("lsm",   "LSM-trees")]

# Refresh-experiment labels (S4 here = "base only", no secondaries),
# matching plot_refresh_sales.py's REFRESH_LABELS for consistency.
STRUCTURE_LABELS_TEX: Dict[int, str] = {
    1: r"\textsc{Base-Merge}",
    2: r"\textsc{Mat-View}",
    3: r"\textsc{Merged-Idx}",
}

CELL_LABEL: Dict[str, str] = {
    "5L":  r"1.0\,GiB memory budget",
    "5H":  r"0.4\,GiB memory budget",
    "5HH": r"0.1\,GiB memory budget",
}


def _load_summary(tag_root: Path, expected_filename: str) -> pd.DataFrame:
    csv_path = tag_root / "summary" / expected_filename
    if not csv_path.exists():
        print(f"[plot_refresh_lsm_vs_btree] error: {csv_path} missing",
              file=sys.stderr)
        sys.exit(2)
    df = pd.read_csv(csv_path)
    if METRIC_COL not in df.columns:
        print(f"[plot_refresh_lsm_vs_btree] error: {csv_path.name} has no "
              f"{METRIC_COL} column", file=sys.stderr)
        sys.exit(2)
    df[METRIC_COL] = pd.to_numeric(df[METRIC_COL], errors="coerce")
    return df


def _load_manifest(tag_root: Path) -> Dict[str, str]:
    m = tag_root / "manifest.yaml"
    if not m.exists():
        return {}
    with m.open() as fh:
        try:
            return yaml.safe_load(fh) or {}
        except yaml.YAMLError:
            return {}


def _lookup(df: pd.DataFrame, backend: str, structure: int) -> float:
    """Return per-pair latency in µs (1e6 / pair_tps)."""
    row = df[(df["backend"] == backend) & (df["structure"] == structure)]
    if row.empty:
        return float("nan")
    tps = float(row[METRIC_COL].iloc[0])
    if not np.isfinite(tps) or tps <= 0:
        return float("nan")
    return 1e6 / tps


def _panel(ax, df_5L: pd.DataFrame, df_5H: pd.DataFrame,
           df_5HH: pd.DataFrame, backend: str, backend_label: str,
           show_ylabel: bool) -> None:
    bar_w = 0.26
    xs = np.arange(len(STRUCTURES), dtype=float)

    for i, struct in enumerate(STRUCTURES):
        color = STYLE["structure_colors"].get(struct, "#777777")
        v_5L  = _lookup(df_5L,  backend, struct)
        v_5H  = _lookup(df_5H,  backend, struct)
        v_5HH = _lookup(df_5HH, backend, struct)
        # 5L (most memory) → solid; 5H → "////"; 5HH (least memory) → "xx".
        if np.isfinite(v_5L):
            ax.bar(xs[i] - bar_w, v_5L, width=bar_w,
                   color=color, linewidth=0, zorder=2)
        if np.isfinite(v_5H):
            ax.bar(xs[i], v_5H, width=bar_w,
                   facecolor=color, edgecolor="#222",
                   linewidth=0.6, hatch="////", zorder=2)
        if np.isfinite(v_5HH):
            ax.bar(xs[i] + bar_w, v_5HH, width=bar_w,
                   facecolor=color, edgecolor="#222",
                   linewidth=0.6, hatch="xx", zorder=2)

    ax.set_title(backend_label, fontsize=10)
    ax.set_xticks(xs)
    ax.set_xticklabels([STRUCTURE_LABELS_TEX[s] for s in STRUCTURES],
                       fontsize=8)
    ax.set_xlim(-0.5, len(STRUCTURES) - 0.5)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=6))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _pos: f"{int(v):,}"
                              if v >= 1000 else f"{int(v)}"))
    ax.tick_params(axis="y", which="major", labelsize=8)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    if show_ylabel:
        ax.set_ylabel(r"$\mu$s / RF pair", fontsize=9)


def _figure_legend(fig) -> None:
    # Two legend groups: structure colours (top row), 5L/5H shading
    # (bottom row). Plain matplotlib patches with hatch markers keep
    # the encoding self-documenting without touching the bar code.
    struct_handles = [
        mpatches.Patch(facecolor=STYLE["structure_colors"][s], linewidth=0,
                       label=STRUCTURE_LABELS_TEX[s])
        for s in STRUCTURES
    ]
    cell_handles = [
        mpatches.Patch(facecolor="#888", linewidth=0,
                       label=CELL_LABEL["5L"]),
        mpatches.Patch(facecolor="#888", edgecolor="#222", linewidth=0.6,
                       hatch="////", label=CELL_LABEL["5H"]),
        mpatches.Patch(facecolor="#888", edgecolor="#222", linewidth=0.6,
                       hatch="xx", label=CELL_LABEL["5HH"]),
    ]
    fig.legend(handles=struct_handles, loc="upper center",
               bbox_to_anchor=(0.5, 1.04), ncol=len(STRUCTURES),
               fontsize=9, frameon=False,
               columnspacing=1.6, handletextpad=0.5)
    fig.legend(handles=cell_handles, loc="upper center",
               bbox_to_anchor=(0.5, 0.97), ncol=3,
               fontsize=8, frameon=False,
               columnspacing=1.6, handletextpad=0.5)


def _footer_text(tags: List[str],
                 manifests: List[Dict[str, str]]) -> str:
    parts = [f"tag {' + '.join(tags)}"]
    primary = manifests[0] if manifests else {}
    for key, label in (("commit_sha", "commit"),
                        ("host", "host"),
                        ("disk", "disk")):
        v = primary.get(key)
        if v:
            parts.append(f"{label} {v}")
    return "  |  ".join(parts)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--diagram", default="refresh_lsm_vs_btree",
                   help="diagram name in diagrams.yaml (default: refresh_lsm_vs_btree). "
                        "sources[0]=5L, sources[1]=5H, sources[2]=5HH.")
    p.add_argument("--csv-5L",
                   default="refresh_sales_5L_bg2_throughput.csv",
                   help="filename under <tag-5L>/summary/")
    p.add_argument("--csv-5H",
                   default="refresh_sales_5H_bg2_throughput.csv",
                   help="filename under <tag-5H>/summary/")
    p.add_argument("--csv-5HH",
                   default="refresh_sales_5HH_bg2_throughput.csv",
                   help="filename under <tag-5HH>/summary/")
    p.add_argument("--formats", nargs="+", default=["pdf", "png"],
                   choices=["pdf", "png", "svg"])
    args = p.parse_args()

    tag_paths = sources_for(args.diagram)
    if len(tag_paths) < 3:
        print(f"[plot_refresh_lsm_vs_btree] error: diagram '{args.diagram}' "
              f"needs 3 sources (5L/5H/5HH), got {len(tag_paths)}",
              file=sys.stderr)
        return 2
    root_5L, root_5H, root_5HH = tag_paths[0], tag_paths[1], tag_paths[2]
    tags = [p.name for p in tag_paths]

    df_5L  = _load_summary(root_5L,  args.csv_5L)
    df_5H  = _load_summary(root_5H,  args.csv_5H)
    df_5HH = _load_summary(root_5HH, args.csv_5HH)

    manifests = [_load_manifest(r) for r in (root_5L, root_5H, root_5HH)]

    fig, axes = plt.subplots(1, 2, figsize=(6.6, 2.1), sharey=True)
    for ax, (backend, label) in zip(axes, BACKENDS):
        _panel(ax, df_5L, df_5H, df_5HH, backend, label,
               show_ylabel=(ax is axes[0]))

    # Sync linear y so the 5H/5L drop is visually honest across panels.
    ymax = max(ax.get_ylim()[1] for ax in axes)
    for ax in axes:
        ax.set_ylim(0, ymax * 1.05)

    _figure_legend(fig)
    fig.tight_layout(rect=(0, 0.0, 1, 0.92))

    out_stem = output_path(args.diagram)
    out_stem.parent.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []
    for fmt in args.formats:
        dest = out_stem.with_suffix(f".{fmt}")
        # Raster formats need an explicit dpi; matplotlib's default 100
        # leaves PNGs visibly blocky next to the 6.6"-wide PDF when the
        # paper review pipeline previews them. Vector formats ignore dpi.
        save_kwargs = {"bbox_inches": "tight"}
        if fmt in ("png", "jpg", "jpeg"):
            save_kwargs["dpi"] = 300
        fig.savefig(dest, **save_kwargs)
        saved.append(dest)
    plt.close(fig)

    for path in saved:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
