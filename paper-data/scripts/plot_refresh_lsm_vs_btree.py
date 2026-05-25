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

from plot_paper_sweep import STYLE


METRIC_COL = "pair_tps_tail30"
STRUCTURES: List[int] = [1, 2, 3]
BACKENDS: List[Tuple[str, str]] = [("btree", r"\textsc{btree}"),
                                    ("lsm",   r"\textsc{lsm}")]

# Refresh-experiment labels (S4 here = "base only", no secondaries),
# matching plot_refresh_sales.py's REFRESH_LABELS for consistency.
STRUCTURE_LABELS_TEX: Dict[int, str] = {
    1: r"\textsc{Split-Idx}",
    2: r"\textsc{Mat-View}",
    3: r"\textsc{Merged-Idx}",
}

CELL_LABEL: Dict[str, str] = {
    "5L": r"5L (1.0\,GiB)",
    "5H": r"5H (0.4\,GiB)",
}

DEFAULT_TAG_5L = "2026-05-24-refresh-5L-ssd"
DEFAULT_TAG_5H = "2026-05-24-refresh-5H-ssd"


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
    row = df[(df["backend"] == backend) & (df["structure"] == structure)]
    if row.empty:
        return float("nan")
    val = float(row[METRIC_COL].iloc[0])
    return val if np.isfinite(val) and val > 0 else float("nan")


def _panel(ax, df_5L: pd.DataFrame, df_5H: pd.DataFrame,
           backend: str, backend_label: str, show_ylabel: bool) -> None:
    bar_w = 0.36
    xs = np.arange(len(STRUCTURES), dtype=float)

    for i, struct in enumerate(STRUCTURES):
        color = STYLE["structure_colors"].get(struct, "#777777")
        v_5L = _lookup(df_5L, backend, struct)
        v_5H = _lookup(df_5H, backend, struct)
        # 5L: solid filled bar (left of cluster center).
        if np.isfinite(v_5L):
            ax.bar(xs[i] - bar_w / 2, v_5L, width=bar_w,
                   color=color, linewidth=0, zorder=2)
        # 5H: hatched bar of the same colour (right of cluster center),
        # darker edge to keep the hatch readable on a coloured fill.
        if np.isfinite(v_5H):
            ax.bar(xs[i] + bar_w / 2, v_5H, width=bar_w,
                   facecolor=color, edgecolor="#222",
                   linewidth=0.6, hatch="////", zorder=2)

    ax.set_title(backend_label, fontsize=10)
    ax.set_xticks(xs)
    ax.set_xticklabels([STRUCTURE_LABELS_TEX[s] for s in STRUCTURES],
                       fontsize=8, rotation=20, ha="right",
                       rotation_mode="anchor")
    ax.set_xlim(-0.5, len(STRUCTURES) - 0.5)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=6))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _pos: f"{int(v):,}"
                              if v >= 1000 else f"{int(v)}"))
    ax.tick_params(axis="y", which="major", labelsize=8)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    if show_ylabel:
        ax.set_ylabel(r"RF1+RF2 pair throughput (pairs/s, tail-30\,s)",
                      fontsize=9)


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
    ]
    fig.legend(handles=struct_handles, loc="upper center",
               bbox_to_anchor=(0.5, 1.04), ncol=len(STRUCTURES),
               fontsize=9, frameon=False,
               columnspacing=1.6, handletextpad=0.5)
    fig.legend(handles=cell_handles, loc="upper center",
               bbox_to_anchor=(0.5, 0.97), ncol=2,
               fontsize=8, frameon=False,
               columnspacing=1.6, handletextpad=0.5)


def _footer_text(tag_5L: str, tag_5H: str,
                 m_5L: Dict[str, str], m_5H: Dict[str, str]) -> str:
    parts = [f"tag {tag_5L} + {tag_5H}"]
    for key, label in (("commit_sha", "commit"),
                        ("host", "host"),
                        ("disk", "disk")):
        v = m_5L.get(key) or m_5H.get(key)
        if v:
            parts.append(f"{label} {v}")
    return "  |  ".join(parts)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tag-5L", default=DEFAULT_TAG_5L)
    p.add_argument("--tag-5H", default=DEFAULT_TAG_5H)
    p.add_argument("--root", type=Path,
                   default=Path.cwd() / "paper-data",
                   help="paper-data/ root holding both tags "
                        "(default: ./paper-data)")
    p.add_argument("--out-tag", default=None,
                   help="snapshot tag whose figures/paper/ dir hosts the "
                        "output (default: --tag-5L)")
    p.add_argument("--basename", default="refresh_lsm_vs_btree_5L_5H_ssd")
    p.add_argument("--formats", nargs="+", default=["pdf", "png"],
                   choices=["pdf", "png", "svg"])
    args = p.parse_args()

    root_5L = args.root / args.tag_5L
    root_5H = args.root / args.tag_5H
    df_5L = _load_summary(root_5L, "refresh_sales_5L_throughput.csv")
    df_5H = _load_summary(root_5H, "refresh_sales_5H_throughput.csv")

    fig, axes = plt.subplots(1, 2, figsize=(6.6, 3.0), sharey=True)
    for ax, (backend, label) in zip(axes, BACKENDS):
        _panel(ax, df_5L, df_5H, backend, label,
               show_ylabel=(ax is axes[0]))

    # Sync linear y so the 5H/5L drop is visually honest across panels.
    ymax = max(ax.get_ylim()[1] for ax in axes)
    for ax in axes:
        ax.set_ylim(0, ymax * 1.05)

    _figure_legend(fig)
    fig.tight_layout(rect=(0, 0.0, 1, 0.92))

    out_tag = args.out_tag or args.tag_5L
    out_dir = args.root / out_tag / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    saved: List[Path] = []
    for fmt in args.formats:
        dest = out_dir / f"{args.basename}.{fmt}"
        fig.savefig(dest, bbox_inches="tight")
        saved.append(dest)
    plt.close(fig)

    for path in saved:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
