#!/usr/bin/env python3
"""Plot refresh_sales LSM-vs-btree throughput across 10L (1.0 GiB DRAM) and
10H (0.4 GiB DRAM) memory budgets.

Reads ``paper-data/<tag-10L>/summary/refresh_sales_10L_throughput.csv`` and
``paper-data/<tag-10H>/summary/refresh_sales_10H_throughput.csv``, both at
the same SF, and emits a 1x2 grouped-bar figure under

    paper-data/<tag-10L>/figures/paper/refresh_lsm_vs_btree_10L_10H_ssd.{pdf,png}

Layout: left panel = btree, right panel = lsm. Each panel has four
structure clusters (S1 split, S2 view, S3 merged) with two bars per
cluster (10L solid, 10H hatched). Y axis is absolute RF1+RF2 pair
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
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
import os
import shutil
matplotlib.use("Agg")
# Override via MPL_USETEX=0 to skip latex (labels uglier; works without
# tex install). Paper-ready PDFs still need latex.
_USETEX = (os.environ.get("MPL_USETEX", "1") != "0"
           and shutil.which("latex") is not None)
matplotlib.rcParams.update({
    "text.usetex": _USETEX,
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
    "10L":  r"1.0\,GiB DRAM",
    "10H":  r"0.5\,GiB DRAM",
    "10HH": r"0.2\,GiB DRAM",
}

# Without latex, strip \textsc{} / \, so labels read cleanly via mathtext.
if not _USETEX:
    def _detex(v):
        return re.sub(r'\\textsc\{([^}]*)\}', r'\1', v).replace('\\,', ' ')
    STRUCTURE_LABELS_TEX = {k: _detex(v) for k, v in STRUCTURE_LABELS_TEX.items()}
    CELL_LABEL = {k: _detex(v) for k, v in CELL_LABEL.items()}


def _load_summary(tag_root: Path, expected_filename: str) -> pd.DataFrame:
    csv_path = tag_root / "summary" / expected_filename
    # Missing/mismatched cell (e.g. a --smoke sweep ran only one refresh cell):
    # return an empty frame so the figure renders whatever cells ARE present
    # rather than aborting. _panel already guards each bar with np.isfinite.
    if not csv_path.exists():
        print(f"[plot_refresh_lsm_vs_btree] note: {csv_path} absent; "
              f"skipping that memory point", file=sys.stderr)
        return pd.DataFrame(columns=["backend", "structure", METRIC_COL])
    df = pd.read_csv(csv_path)
    if METRIC_COL not in df.columns:
        print(f"[plot_refresh_lsm_vs_btree] note: {csv_path.name} has no "
              f"{METRIC_COL} column; skipping", file=sys.stderr)
        return pd.DataFrame(columns=["backend", "structure", METRIC_COL])
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


def _panel(ax, df_10L: pd.DataFrame, df_10H: pd.DataFrame,
           df_10HH: pd.DataFrame, backend: str, backend_label: str,
           show_ylabel: bool) -> None:
    bar_w = 0.26
    xs = np.arange(len(STRUCTURES), dtype=float)

    for i, struct in enumerate(STRUCTURES):
        color = STYLE["structure_colors"].get(struct, "#777777")
        v_10L  = _lookup(df_10L,  backend, struct)
        v_10H  = _lookup(df_10H,  backend, struct)
        v_10HH = _lookup(df_10HH, backend, struct)
        # 10L (most memory) → solid; 10H → "////"; 10HH (least memory) → "xx".
        if np.isfinite(v_10L):
            ax.bar(xs[i] - bar_w, v_10L, width=bar_w,
                   color=color, linewidth=0, zorder=2)
        if np.isfinite(v_10H):
            ax.bar(xs[i], v_10H, width=bar_w,
                   facecolor=color, edgecolor="#222",
                   linewidth=0.6, hatch="////", zorder=2)
        if np.isfinite(v_10HH):
            ax.bar(xs[i] + bar_w, v_10HH, width=bar_w,
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
    # Only the DRAM-cell shading legend remains; structure colours are
    # already labelled by xticklabels under each bar.
    cell_handles = [
        mpatches.Patch(facecolor="#888", linewidth=0,
                       label=CELL_LABEL["10L"]),
        mpatches.Patch(facecolor="#888", edgecolor="#222", linewidth=0.6,
                       hatch="////", label=CELL_LABEL["10H"]),
        mpatches.Patch(facecolor="#888", edgecolor="#222", linewidth=0.6,
                       hatch="xx", label=CELL_LABEL["10HH"]),
    ]
    fig.legend(handles=cell_handles, loc="upper center",
               bbox_to_anchor=(0.5, 1.00), ncol=3,
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
                        "sources[0]=10L, sources[1]=10H, sources[2]=10HH.")
    p.add_argument("--csv-10L",
                   default="refresh_sales_10L_bg2_throughput.csv",
                   help="filename under <tag-10L>/summary/")
    p.add_argument("--csv-10H",
                   default="refresh_sales_10H_bg2_throughput.csv",
                   help="filename under <tag-10H>/summary/")
    p.add_argument("--csv-10HH",
                   default="refresh_sales_10HH_bg2_throughput.csv",
                   help="filename under <tag-10HH>/summary/")
    p.add_argument("--formats", nargs="+", default=["pdf", "png"],
                   choices=["pdf", "png", "svg"])
    p.add_argument("--tag-map", default=None, metavar="YAML_OR_JSON",
                   help="path to a YAML/JSON file (or an inline YAML string) mapping "
                        "authoring tag names from diagrams.yaml to neutral result-dir "
                        "names under the results root. Example: "
                        "'{\"2026-05-30-refresh-10L\": \"refresh\"}'. "
                        "Set PAPER_TAG_MAP env var as an alternative.")
    p.add_argument("--results-root", type=Path, default=None,
                   help="root directory containing neutral tag dirs (e.g. /results). "
                        "When omitted, reads from the in-repo paper-data/<tag>/ dirs.")
    args = p.parse_args()

    # Build tag resolver from --tag-map / PAPER_TAG_MAP env var.
    import os as _os
    import yaml as _yaml
    _tag_map: dict = {}
    _tag_map_raw = args.tag_map or _os.environ.get("PAPER_TAG_MAP", "")
    if _tag_map_raw:
        _src = Path(_tag_map_raw)
        if _src.exists():
            with _src.open() as _fh:
                _raw = _yaml.safe_load(_fh) or {}
        else:
            _raw = _yaml.safe_load(_tag_map_raw) or {}
        if not isinstance(_raw, dict):
            p.error("--tag-map must resolve to a YAML/JSON object (mapping)")
        _tag_map = {str(k): str(v) for k, v in _raw.items()}

    _results_root = args.results_root
    # The in-process refresh shim in plot_paper_sweep can't pass --results-root,
    # so honor PAPER_RESULTS_ROOT from the env too (same source as PAPER_TAG_MAP).
    if _results_root is None and _os.environ.get("PAPER_RESULTS_ROOT"):
        _results_root = Path(_os.environ["PAPER_RESULTS_ROOT"])
    from _diagram_metadata import load_metadata, PAPER_DATA_ROOT as _PDR

    def _resolve(authored_tag: str) -> Path:
        neutral = _tag_map.get(authored_tag, authored_tag)
        if _results_root is not None:
            return _results_root / neutral
        return _PDR / neutral

    # Resolve tag paths through the map (or direct lookup when no map).
    if _tag_map or _results_root is not None:
        meta = load_metadata()
        if args.diagram not in meta:
            print(f"[plot_refresh_lsm_vs_btree] error: diagram '{args.diagram}' not in diagrams.yaml",
                  file=sys.stderr)
            return 1
        authored_tags: List[str] = meta[args.diagram]["sources"]
        tag_paths = [_resolve(t) for t in authored_tags]
    else:
        tag_paths = sources_for(args.diagram)

    if len(tag_paths) < 3:
        print(f"[plot_refresh_lsm_vs_btree] error: diagram '{args.diagram}' "
              f"needs 3 sources (10L/10H/10HH), got {len(tag_paths)}",
              file=sys.stderr)
        return 2
    root_10L, root_10H, root_10HH = tag_paths[0], tag_paths[1], tag_paths[2]
    tags = [p.name for p in tag_paths]

    df_10L  = _load_summary(root_10L,  args.csv_10L)
    df_10H  = _load_summary(root_10H,  args.csv_10H)
    df_10HH = _load_summary(root_10HH, args.csv_10HH)

    manifests = [_load_manifest(r) for r in (root_10L, root_10H, root_10HH)]

    fig, axes = plt.subplots(1, 2, figsize=(5.94, 1.68), sharey=True)
    for ax, (backend, label) in zip(axes, BACKENDS):
        _panel(ax, df_10L, df_10H, df_10HH, backend, label,
               show_ylabel=(ax is axes[0]))

    # Sync linear y so the 10H/10L drop is visually honest across panels.
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
