#!/usr/bin/env python3
"""Diagnostics figure for the S3/S2 RocksDB anomaly on q3i/q5i.

Crosses two sweep tags (SSD + HDD), filters to lsm c0 bg=2, computes
median-then-ratio S3/S2 for three metrics, and emits a 1-column
two-panel grouped-bar figure. Standalone — not a plot_paper_sweep.py
builder, because the figure spans two tags.

Output: paper-data/2026-05-24-a-ssd/figures/paper/lsm_s3_vs_s2_diagnostics.{pdf,png}
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FixedLocator, NullLocator, ScalarFormatter

REPO_ROOT = Path(__file__).resolve().parents[2]
PAPER_DATA = REPO_ROOT / "paper-data"

TAGS = {"ssd": "2026-05-24-a-ssd", "hdd": "2026-05-18-b"}

# (column, display label, source file)
METRICS = [
    ("ms_per_tx",          r"$\mathrm{ms/tx}$",        "headline.csv"),
    ("sst_read_us_per_tx", r"$\mathrm{sst\_read}$",    "diagnostics.csv"),
    ("cpu_cycles_per_tx",  r"$\mathrm{cpu\_cycles}$",  "diagnostics.csv"),
]
METRIC_COLORS = ["#444444", "#4C72B0", "#C44E52"]

QUERIES = [
    ("q3",  r"$\mathtt{q3}$"),
    ("q5",  r"$\mathtt{q5}$"),
    ("q3i", r"$\mathtt{q3i}$"),
    ("q5i", r"$\mathtt{q5i}$"),
]


def load_ratio(tag: str, metric: str, fname: str) -> dict[str, float]:
    """Median-then-ratio S3/S2 per query on lsm c0 bg=2."""
    df = pd.read_csv(PAPER_DATA / tag / "summary" / fname)
    df = df[(df["cell"] == "c0") & (df["bg"] == 2) & (df["backend"] == "lsm")
            & (df["structure"].isin([2, 3]))]
    med = df.groupby(["query", "structure"])[metric].median().unstack()
    return (med[3] / med[2]).to_dict()


def render() -> None:
    ratios = {  # disk -> metric -> {query: ratio}
        disk: {m: load_ratio(tag, m, f) for m, _, f in METRICS}
        for disk, tag in TAGS.items()
    }

    fig, axes = plt.subplots(2, 1, figsize=(3.3, 4.0),
                             sharex=True, sharey=True)
    bar_w = 0.25
    x = np.arange(len(QUERIES))

    for ax, disk, panel_label in [
        (axes[0], "ssd", "(a) SSD"),
        (axes[1], "hdd", "(b) HDD"),
    ]:
        for i, ((metric, mlabel, _), color) in enumerate(zip(METRICS, METRIC_COLORS)):
            vals = [ratios[disk][metric].get(q, np.nan) for q, _ in QUERIES]
            ax.bar(x + (i - 1) * bar_w, vals, bar_w,
                   color=color, label=mlabel, edgecolor="white", linewidth=0.3)

        ax.axhline(1.0, color="#666", linestyle="--", linewidth=0.7, zorder=0)
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(FixedLocator([0.7, 1.0, 1.5, 2.0]))
        ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.yaxis.set_minor_locator(NullLocator())
        ax.set_ylim(0.7, 2.2)
        ax.text(0.02, 0.94, panel_label, transform=ax.transAxes,
                ha="left", va="top", fontsize=8, fontweight="bold")
        ax.tick_params(axis="both", labelsize=8)
        ax.set_ylabel("S3 / S2 ratio", fontsize=8)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)

    axes[1].set_xticks(x)
    axes[1].set_xticklabels([lbl for _, lbl in QUERIES], fontsize=9)

    # Annotate the y=1.0 baseline once on the top panel.
    axes[0].text(len(QUERIES) - 0.5, 1.0, "  S3 = S2",
                 ha="left", va="center", fontsize=7, color="#666")

    # Single legend above the top panel.
    axes[0].legend(loc="lower center", bbox_to_anchor=(0.5, 1.02),
                   ncol=3, frameon=False, fontsize=7,
                   handlelength=1.2, columnspacing=1.0)

    fig.tight_layout(rect=[0, 0, 1, 0.96])

    out_dir = PAPER_DATA / TAGS["ssd"] / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "lsm_s3_vs_s2_diagnostics"
    for ext in ("pdf", "png"):
        path = base.with_suffix(f".{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=200 if ext == "png" else None)
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    plt.close(fig)


if __name__ == "__main__":
    render()
