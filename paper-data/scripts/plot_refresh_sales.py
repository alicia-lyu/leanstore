#!/usr/bin/env python3
"""Plot refresh_sales RF1 update latency (µs / insert).

Reads ``paper-data/<tag>/summary/refresh_sales_rf_throughput.csv`` (one row
per backend × structure) and emits a 1×2 grouped-bar figure under
``paper-data/<tag>/figures/paper/refresh_sales_rf1_latency.{pdf,png}``.

Y axis is **µs per RF1 insert** (= 1e6 / rf1_tps), log scale,
lower-is-better — matches the read-side figures' "time / TX" convention.

Optionally accepts a DBToaster sibling CSV (same schema) and renders the
extra series; the legend / bar-width math adapts to either 4 or 5 series.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import yaml

# Reuse colour / label / order conventions from the main plotter so the
# RF figure visually matches the read-side headline figures.
from plot_paper_sweep import (
    STYLE, STRUCTURE_LABELS, PAPER_LEGEND_ORDER,
)


DBTOASTER_COLOR = "#34495e"  # slate — distinct from the S1..S5 palette
DBTOASTER_LABEL = "DBToaster"

PANELS: List[Tuple[str, str]] = [
    # Only the hot window (warm steady state) — whole-run avg is
    # warmup-confounded at SF=1 and not the headline. Title is left
    # blank because there's a single panel; the figure caption
    # carries the hot-window definition.
    ("rf1_tps_hot", ""),
]

BACKENDS = ["btree", "lsm"]

# Per-CSV-schema view: which throughput column is the headline, what
# unit to label it with, and the figure basename. 5L files report a
# size-stable RF1+RF2 *pair* throughput (RF2 doesn't exhaust at SF≥15),
# which is the TPC-H-faithful unit; the SF=1 rf_throughput file falls
# back to RF1-only because its RF2 reservoir drains early.
CSV_SCHEMAS = {
    "refresh_sales_5L_throughput.csv": {
        "tps_col": "pair_tps_tail30",
        "unit": "ms / RF pair (tail 30 s)",
        "scale": 1e3,  # tps → ms/op
        "basename": "refresh_5L_pair_latency",
    },
    "refresh_sales_rf_throughput.csv": {
        "tps_col": "rf1_tps_hot",
        "unit": "µs / RF1 insert (hot)",
        "scale": 1e6,
        "basename": "refresh_sales_rf1_latency",
    },
}


def _load_dbtoaster(path: Optional[Path]) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Tag rows with a synthetic structure id outside 1..5 so they can be
    # grouped uniformly with the rest.
    df = df.copy()
    df["structure"] = 99
    df["struct_label"] = DBTOASTER_LABEL
    return df


# Labels frame the y-axis question: *what extra storage structure
# (beyond primary indexes) does Q3 / Q5 maintain?* S4 = none; S1 =
# extra secondary indexes; S2 = pipeline views; S3 = merged indexes.
REFRESH_LABELS = {
    1: r"$\mathtt{base\_merge}$",
    2: r"$\mathtt{mat\_view}$",
    3: r"$\mathtt{merged\_idx}$",
    # S4 (base_hash, "None") is intentionally omitted from the refresh
    # figure: with no secondary structures to maintain, it's the
    # trivial ceiling and crowds the comparison.
}
REFRESH_OMIT = {4}


def _series_style(struct: int) -> Tuple[str, str]:
    """Return (color, label) for a structure id, including the DBToaster
    synthetic id 99. Uses refresh-experiment labels (S4 = "base only",
    not the read-side "base hash-join")."""
    if struct == 99:
        return DBTOASTER_COLOR, DBTOASTER_LABEL
    color = STYLE["structure_colors"].get(struct, "#777777")
    label = REFRESH_LABELS.get(struct, str(struct))
    return color, label


def _panel(ax, df: pd.DataFrame, tps_col: str, scale: float, unit: str,
           series_order: List[int], show_ylabel: bool, title: str) -> None:
    """One grouped-bar panel: x = backend, one bar per series.

    ``scale`` converts ops/sec to the chosen time unit (1e6 for µs, 1e3
    for ms). ``unit`` becomes the y-axis label when ``show_ylabel``.
    """
    n = len(series_order)
    bar_w = 0.8 / max(n, 1)
    x_idx = {b: i for i, b in enumerate(BACKENDS)}
    for k, struct in enumerate(series_order):
        color, _label = _series_style(struct)
        xs, ys = [], []
        for backend in BACKENDS:
            row = df[(df["backend"] == backend)
                     & (df["structure"] == struct)]
            if row.empty:
                continue
            tps = float(row[tps_col].iloc[0])
            if not np.isfinite(tps) or tps <= 0:
                continue
            xs.append(x_idx[backend] + (k - (n - 1) / 2) * bar_w)
            ys.append(scale / tps)
        if xs:
            ax.bar(xs, ys, width=bar_w, color=color, linewidth=0)
    ax.set_title(title, fontsize=10)
    ax.set_xticks(np.arange(len(BACKENDS)))
    ax.set_xticklabels(BACKENDS, fontsize=8)
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0))
    ax.yaxis.set_major_formatter(
        mticker.LogFormatterSciNotation(base=10.0, labelOnlyBase=True))
    ax.yaxis.set_minor_locator(
        mticker.LogLocator(base=10.0, subs=tuple(range(2, 10))))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.tick_params(axis="y", which="major", labelsize=7)
    ax.tick_params(axis="y", which="minor", length=2)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    if show_ylabel:
        ax.set_ylabel(unit, fontsize=8)


def _footer(manifest: Dict[str, str], tag: str) -> str:
    parts = [f"tag {tag}"]
    for k in ("commit_sha", "host", "scale_factor"):
        v = manifest.get(k)
        if v:
            parts.append(f"{k.split('_')[0]} {v}")
    return "  |  ".join(parts)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--tag", required=True)
    p.add_argument("--root", type=Path, default=None,
                   help="paper-data/<tag>/ root (default: paper-data/<tag>)")
    p.add_argument("--format", default="pdf",
                   choices=["pdf", "png", "svg"])
    p.add_argument("--dbtoaster", type=Path, default=None,
                   help="optional path to dbtoaster_rf_throughput.csv "
                        "(default: <root>/summary/dbtoaster_rf_throughput.csv)")
    args = p.parse_args()

    root: Path = args.root or (Path.cwd() / "paper-data" / args.tag)
    summary = root / "summary"
    schema = None
    csv_path = None
    for name, spec in CSV_SCHEMAS.items():
        candidate = summary / name
        if candidate.exists():
            csv_path = candidate
            schema = spec
            break
    if csv_path is None:
        print(f"[plot_refresh_sales] error: no refresh CSV under {summary} "
              f"(expected one of {list(CSV_SCHEMAS)})", file=sys.stderr)
        return 1

    df = pd.read_csv(csv_path)
    # Disk-media tag (set by scripts/mark_disk_media.py). Used to suffix
    # the output basename so SSD/HDD reruns don't overwrite each other.
    disk_vals = (sorted({str(v) for v in df["disk"].dropna().unique()})
                 if "disk" in df.columns else [])
    disk = disk_vals[0] if disk_vals else None
    if len(disk_vals) > 1:
        print(f"[plot_refresh_sales] WARN: mixed disk media: {disk_vals}",
              file=sys.stderr)
    dbtoaster_path = (args.dbtoaster
                      or (summary / "dbtoaster_rf_throughput.csv"))
    db_df = _load_dbtoaster(dbtoaster_path)
    if not db_df.empty:
        df = pd.concat([df, db_df], ignore_index=True)

    manifest: Dict[str, str] = {}
    mfile = root / "manifest.yaml"
    if mfile.exists():
        try:
            manifest = yaml.safe_load(mfile.read_text()) or {}
        except yaml.YAMLError as e:
            print(f"[plot_refresh_sales] WARN: manifest parse: {e}",
                  file=sys.stderr)

    series_order = [s for s in PAPER_LEGEND_ORDER if s not in REFRESH_OMIT]
    if not db_df.empty:
        series_order.append(99)

    fig, ax = plt.subplots(1, 1, figsize=(3.2, 2.6))
    _panel(ax, df, schema["tps_col"], schema["scale"], schema["unit"],
           series_order, show_ylabel=True, title="")

    handles = []
    for s in series_order:
        color, label = _series_style(s)
        handles.append(plt.Rectangle((0, 0), 1, 1,
                                     color=color, label=label))
    # 2-column legend keeps width compatible with the single-panel
    # figsize; 5 series wraps to 3 rows × 2 cols.
    fig.legend(handles=handles, loc="upper center",
               ncol=2, fontsize=7,
               bbox_to_anchor=(0.5, 1.0),
               frameon=False, columnspacing=1.2, handletextpad=0.4,
               title="extra storage beyond primary indexes",
               title_fontsize=7)

    out_dir = root / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    basename = schema["basename"] + (f"_{disk}" if disk else "")
    base = out_dir / basename
    fig.text(0.5, 0.01, _footer(manifest, args.tag),
             ha="center", va="bottom",
             fontsize=STYLE["footer_fontsize"], color="#666")
    fig.subplots_adjust(left=0.18, right=0.97, top=0.78, bottom=0.18)
    primary = base.with_suffix(f".{args.format}")
    fig.savefig(primary, format=args.format, bbox_inches="tight")
    written = [primary]
    if args.format == "pdf":
        png = base.with_suffix(".png")
        fig.savefig(png, format="png", dpi=180, bbox_inches="tight")
        written.append(png)
    plt.close(fig)
    for w in written:
        print(f"[plot_refresh_sales] wrote {w}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
