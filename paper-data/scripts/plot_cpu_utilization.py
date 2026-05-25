#!/usr/bin/env python3
"""CPU utilization across queries × storage structures, B-tree vs LSM.

On the 2026-05-24-a-ssd tag the measured ``cpu_util_pct`` values are
**low in absolute terms** (sub-2% B-tree, sub-5% LSM): this counter
reports a single query worker's utilization on a many-core node, not
whole-system utilization, and the workload is IO-bound at the 5L
beyond-memory operating point. The qualitative observation the figure
supports is only *relative* — LSM runs hotter than the B-tree, and the
per-structure spread within each backend. Because the magnitudes are
small, the panels use per-backend y-limits (``backend_ylim`` below), so
**bar heights are NOT comparable across the two panels** — read each
panel against its own axis. (This figure is currently commented out in
the paper LaTeX; kept here as a diagnostic.)

Reads ``diagnostics.csv`` from a tag, filters to c0 / bg=2, medians
across reps, and emits a 1x2 grouped-bar figure (btree | lsm) under the
tag's ``figures/paper/`` directory.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
matplotlib.rcParams.update({
    "text.usetex": True,
    "font.family": "serif",
    "text.latex.preamble": r"\usepackage{lmodern}",
})
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from plot_paper_sweep import STYLE, PAPER_STRUCTURE_LABELS, PAPER_LEGEND_ORDER

REPO_ROOT = Path(__file__).resolve().parents[2]
PAPER_DATA = REPO_ROOT / "paper-data"
TAG = "2026-05-24-a-ssd"

QUERIES = ["q3", "q5", "q3i", "q5i"]
QUERY_LABELS = {q: f"Q{q[1:].upper()}" for q in QUERIES}


def load_cpu(tag: str) -> pd.DataFrame:
    df = pd.read_csv(PAPER_DATA / tag / "summary" / "diagnostics.csv")
    df = df[(df["cell"] == "c0") & (df["bg"] == 2)
            & (df["structure"].isin(PAPER_LEGEND_ORDER))
            & (df["query"].isin(QUERIES))]
    return (df.groupby(["backend", "query", "structure"])["cpu_util_pct"]
              .median().reset_index())


def render() -> None:
    cpu = load_cpu(TAG)
    fig, axes = plt.subplots(1, 2, figsize=(8.5, 1.8), sharey=False)

    n_structs = len(PAPER_LEGEND_ORDER)
    bar_w = 0.8 / n_structs
    x = np.arange(len(QUERIES))

    # Per-backend y-limits — observed CPU sits well below 10% on this
    # tag, so a shared 0-100 range flattens every bar. Tighter per-axis
    # ranges keep the per-structure spread legible while still letting
    # the reader see that LSM runs noticeably hotter than the B-tree.
    backend_ylim = {"btree": (0, 2), "lsm": (0, 5)}
    for ax, backend, title in [(axes[0], "btree", "B-tree"),
                                (axes[1], "lsm", "LSM-tree")]:
        sub = cpu[cpu["backend"] == backend]
        for k, s in enumerate(PAPER_LEGEND_ORDER):
            vals = [
                sub[(sub["query"] == q) & (sub["structure"] == s)]
                    ["cpu_util_pct"].median()
                if not sub[(sub["query"] == q) & (sub["structure"] == s)].empty
                else np.nan
                for q in QUERIES
            ]
            offset = (k - (n_structs - 1) / 2) * bar_w
            ax.bar(x + offset, vals, bar_w,
                   color=STYLE["structure_colors"][s],
                   label=PAPER_STRUCTURE_LABELS[s], linewidth=0)
        ax.set_title(title, fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels([QUERY_LABELS[q] for q in QUERIES], fontsize=12)
        ax.set_ylim(*backend_ylim[backend])
        ax.tick_params(axis="y", labelsize=11)
        ax.yaxis.grid(True, linestyle=":", alpha=0.4)
        ax.set_axisbelow(True)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)

    axes[0].set_ylabel(r"CPU utilization (\%)", fontsize=12)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=n_structs,
               frameon=False, fontsize=11,
               bbox_to_anchor=(0.5, 1.05), columnspacing=1.5,
               handletextpad=0.4)

    fig.tight_layout(rect=[0, 0, 1, 0.92])

    out_dir = PAPER_DATA / TAG / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "cpu_utilization"
    for ext in ("pdf", "png"):
        fig.savefig(base.with_suffix(f".{ext}"), bbox_inches="tight",
                    dpi=200 if ext == "png" else None)
        print(f"wrote {base.with_suffix('.' + ext).relative_to(REPO_ROOT)}")
    plt.close(fig)


if __name__ == "__main__":
    render()
