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

# TPC-H vanilla → circle; TPC-Hi invoice-extended → diamond.
QUERY_FAMILY = {"q3": "tpch", "q5": "tpch", "q3i": "tpchi", "q5i": "tpchi"}
FAMILY_MARKER = {"tpch": "o", "tpchi": "D"}
FAMILY_LABEL = {"tpch": "TPC-H (q3, q5)", "tpchi": "TPC-Hi (q3i, q5i)"}
QUERY_COLOR = {
    "q3":  "#4C72B0",
    "q5":  "#55A868",
    "q3i": "#C44E52",
    "q5i": "#8172B2",
}


def load_ratio(tag: str, metric: str, fname: str) -> dict[str, float]:
    """Median-then-ratio S3/S2 per query on lsm c0 bg=2."""
    df = pd.read_csv(PAPER_DATA / tag / "summary" / fname)
    df = df[(df["cell"] == "c0") & (df["bg"] == 2) & (df["backend"] == "lsm")
            & (df["structure"].isin([2, 3]))]
    med = df.groupby(["query", "structure"])[metric].median().unstack()
    return (med[3] / med[2]).to_dict()


def render() -> None:
    tpchi_queries = [(q, lbl) for q, lbl in QUERIES if q in ("q3i", "q5i")]
    ratios = {  # disk -> metric -> {query: ratio}
        disk: {m: load_ratio(tag, m, f) for m, _, f in METRICS}
        for disk, tag in TAGS.items()
    }

    fig, axes = plt.subplots(1, 2, figsize=(5.0, 2.6),
                             sharex=True, sharey=True)
    bar_w = 0.25
    x = np.arange(len(tpchi_queries))

    for ax, disk, panel_label in [
        (axes[0], "ssd", "(a) SSD"),
        (axes[1], "hdd", "(b) HDD"),
    ]:
        for i, ((metric, mlabel, _), color) in enumerate(zip(METRICS, METRIC_COLORS)):
            vals = [ratios[disk][metric].get(q, np.nan) for q, _ in tpchi_queries]
            ax.bar(x + (i - 1) * bar_w, vals, bar_w,
                   color=color, label=mlabel, edgecolor="white", linewidth=0.3)

        ax.axhline(1.0, color="#666", linestyle="--", linewidth=0.7, zorder=0)
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(FixedLocator([0.7, 1.0, 1.5, 2.0]))
        ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.yaxis.set_minor_locator(NullLocator())
        ax.set_ylim(0.7, 2.2)
        ax.set_xticks(x)
        ax.set_xticklabels([lbl for _, lbl in tpchi_queries], fontsize=9)
        ax.text(0.03, 0.95, panel_label, transform=ax.transAxes,
                ha="left", va="top", fontsize=9, fontweight="bold")
        ax.tick_params(axis="both", labelsize=8)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)

    axes[0].set_ylabel("S3 / S2 ratio", fontsize=9)

    # Annotate the y=1.0 baseline once on the right panel.
    axes[1].text(len(tpchi_queries) - 0.55, 1.0, "  S3 = S2",
                 ha="left", va="center", fontsize=7, color="#666")

    # Shared legend centred above both panels.
    axes[0].legend(loc="lower center", bbox_to_anchor=(1.05, 1.02),
                   ncol=3, frameon=False, fontsize=8,
                   handlelength=1.2, columnspacing=1.2)

    fig.tight_layout(rect=[0, 0, 1, 0.93])

    out_dir = PAPER_DATA / TAGS["ssd"] / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "lsm_s3_vs_s2_diagnostics"
    for ext in ("pdf", "png"):
        path = base.with_suffix(f".{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=200 if ext == "png" else None)
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    plt.close(fig)


def render_correlations() -> None:
    """2x2 grid: rows = SSD/HDD, cols = sst_read vs ms/tx | cpu_cycles vs ms/tx.

    Each point is one query (4 per panel). Marker by family:
    TPC-H (q3,q5) = circle, TPC-Hi (q3i,q5i) = diamond. Dashed y=x
    diagonal — points on the line mean the counter ratio fully
    explains the latency ratio.
    """
    counter_metrics = [
        ("sst_read_us_per_tx", r"sst_read / tx ratio",   "diagnostics.csv"),
        ("cpu_cycles_per_tx",  r"cpu_cycles / tx ratio", "diagnostics.csv"),
    ]
    ms = {disk: load_ratio(tag, "ms_per_tx", "headline.csv")
          for disk, tag in TAGS.items()}
    counters = {disk: {m: load_ratio(tag, m, f) for m, _, f in counter_metrics}
                for disk, tag in TAGS.items()}

    fig, axes = plt.subplots(2, 2, figsize=(6.5, 5.2),
                             sharex="col", sharey=True)

    for row, (disk, row_label) in enumerate([("ssd", "(a) SSD"), ("hdd", "(b) HDD")]):
        for col, (metric, xlabel, _) in enumerate(counter_metrics):
            ax = axes[row, col]
            for q, _ in QUERIES:
                x = counters[disk][metric][q]
                y = ms[disk][q]
                fam = QUERY_FAMILY[q]
                ax.scatter(x, y, s=70, marker=FAMILY_MARKER[fam],
                           color=QUERY_COLOR[q], edgecolor="white",
                           linewidth=0.6, zorder=3)
                ax.annotate(q, (x, y), xytext=(5, 4),
                            textcoords="offset points",
                            fontsize=8, color="#333")

            # Shade "S3 better than S2" half (latency ratio < 1).
            lim_lo, lim_hi = 0.7, 2.1
            ax.axhspan(lim_lo, 1.0, facecolor="#27ae60", alpha=0.10, zorder=0)
            ax.text(lim_hi - 0.04, lim_lo + 0.04, "S3 better",
                    ha="right", va="bottom", fontsize=7, color="#1e7e3c",
                    style="italic", zorder=1)
            # y = x diagonal — counter ratio fully explains latency.
            ax.plot([lim_lo, lim_hi], [lim_lo, lim_hi],
                    "--", color="#999", linewidth=0.7, zorder=1)
            ax.axhline(1.0, color="#ccc", linewidth=0.5, zorder=0)
            ax.axvline(1.0, color="#ccc", linewidth=0.5, zorder=0)

            ax.set_xlim(lim_lo, lim_hi)
            ax.set_ylim(lim_lo, lim_hi)
            ax.set_aspect("equal")
            ax.tick_params(axis="both", labelsize=8)
            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)

            if row == 1:
                ax.set_xlabel(xlabel, fontsize=9)
            if col == 0:
                ax.set_ylabel(f"{row_label}\nms/tx ratio", fontsize=9)

    # Family-marker legend at the top.
    handles = [
        plt.Line2D([0], [0], marker=FAMILY_MARKER[fam], color="#555",
                   linestyle="", markersize=8, label=FAMILY_LABEL[fam])
        for fam in ("tpch", "tpchi")
    ]
    handles.append(plt.Line2D([0], [0], linestyle="--", color="#999",
                              label=r"$y = x$ (counter explains latency)"))
    fig.legend(handles=handles, loc="upper center",
               bbox_to_anchor=(0.5, 0.99), ncol=3, frameon=False, fontsize=8)

    fig.suptitle("S3 / S2 ratio: counter vs latency on RocksDB (c0, bg=2)",
                 fontsize=10, y=0.94)
    fig.tight_layout(rect=[0, 0, 1, 0.91])

    out_dir = PAPER_DATA / TAGS["ssd"] / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "lsm_s3_vs_s2_correlation"
    for ext in ("pdf", "png"):
        path = base.with_suffix(f".{ext}")
        fig.savefig(path, bbox_inches="tight",
                    dpi=200 if ext == "png" else None)
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    plt.close(fig)


def load_per_rep(tag: str, query: str, columns: list[str], fname: str) -> pd.DataFrame:
    """Per-rep absolute values on lsm c0 bg=2 for one query, structures 2 & 3."""
    df = pd.read_csv(PAPER_DATA / tag / "summary" / fname)
    df = df[(df["cell"] == "c0") & (df["bg"] == 2) & (df["backend"] == "lsm")
            & (df["structure"].isin([2, 3])) & (df["query"] == query)]
    return df[["structure", "rep"] + columns].copy()


def render_tpchi_absolute() -> None:
    """2x4 absolute-coords scatter, TPC-Hi only — S2 (x) vs S3 (y) of
    the same metric in each panel, so x/y share units and limits.

    Rows = disk (SSD top, HDD bottom).
    Cols = (q3i latency, q3i sst_read, q3i cpu_seconds,
            q5i latency, q5i sst_read, q5i cpu_seconds)  — wait, this
    docstring is informational; the actual layout below is q3i sst_read,
    q3i cpu, q5i sst_read, q5i cpu (4 cols).

    Each point is one rep: x = S2's value of the metric, y = S3's same
    metric. Dashed y=x diagonal means S3 == S2; points below the
    diagonal are wins for S3 (lower is better). cpu_cycles is converted
    at 2.6 GHz (the c220g2 Xeon E5-2660 v3 baseline). sst_read_us is
    aggregated across reader threads, so absolute values may exceed
    wall-clock time on that axis — the position relative to the
    diagonal is still the comparison that matters.
    """
    queries = ["q3i", "q5i"]
    cpu_ghz = 2.6  # Intel Xeon E5-2660 v3 nominal clock
    metrics = [  # (column, scale-to-display, axis label, source file)
        ("ms_per_tx",          1e3,            "query latency (s / tx)",        "headline.csv"),
        ("sst_read_us_per_tx", 1e6,            "sst_read (s / tx)",             "diagnostics.csv"),
        ("cpu_cycles_per_tx",  cpu_ghz * 1e9,  f"cpu_cycles @ {cpu_ghz} GHz (s / tx)", "diagnostics.csv"),
    ]
    n_metrics = len(metrics)

    # Pre-load: merge headline + diagnostics on (structure, rep) per
    # (query, disk). Latency comes from headline.csv; counters from
    # diagnostics.csv — merging both keeps a single per-rep frame.
    data = {}  # (query, disk) -> DataFrame
    for disk, tag in TAGS.items():
        for q in queries:
            head = load_per_rep(tag, q, ["ms_per_tx"], "headline.csv")
            diag = load_per_rep(tag, q,
                                [m for m, _, _, f in metrics if f == "diagnostics.csv"],
                                "diagnostics.csv")
            data[(q, disk)] = head.merge(diag, on=["structure", "rep"])

    fig, axes = plt.subplots(2, n_metrics * len(queries),
                             figsize=(2.6 * n_metrics * len(queries), 5.4))

    # Precompute medians + per-metric shared upper limit so every panel
    # of the same metric uses the same (0, hi) box. Different metrics
    # have wildly different magnitudes (cpu seconds vs sst seconds vs
    # latency seconds) so sharing across metrics would squash three of
    # the four columns into the origin — share per metric instead.
    medians = {}  # (q, disk, metric) -> (s2, s3)
    metric_max = {m: 0.0 for m, _, _, _ in metrics}
    for (q, disk), df in data.items():
        for metric, scale, _, _ in metrics:
            med = df.groupby("structure")[metric].median()
            if 2 not in med.index or 3 not in med.index:
                continue
            s2 = med[2] / scale
            s3 = med[3] / scale
            medians[(q, disk, metric)] = (s2, s3)
            metric_max[metric] = max(metric_max[metric], s2, s3)

    for row, disk in enumerate(["ssd", "hdd"]):
        for q_idx, q in enumerate(queries):
            for m_idx, (metric, scale, mlabel, _) in enumerate(metrics):
                col = q_idx * n_metrics + m_idx
                ax = axes[row, col]
                pt = medians.get((q, disk, metric))
                hi = metric_max[metric] * 1.05 or 1.0
                lo = 0.0

                if pt is not None:
                    ax.scatter([pt[0]], [pt[1]], s=70, color="#27ae60",
                               marker="o", edgecolor="white",
                               linewidth=0.6, zorder=3)

                # y = x — S3 == S2.
                ax.plot([lo, hi], [lo, hi], "--", color="#999",
                        linewidth=0.7, zorder=1)
                # Shade "S3 better" half (below diagonal).
                ax.fill_between([lo, hi], [lo, lo], [lo, hi],
                                color="#27ae60", alpha=0.07, zorder=0)
                ax.text(hi - (hi - lo) * 0.04, lo + (hi - lo) * 0.04,
                        "S3 better", ha="right", va="bottom",
                        fontsize=7, color="#1e7e3c", style="italic")

                ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)
                ax.set_aspect("equal")

                ax.set_title(f"{q}  ·  {disk.upper()}", fontsize=9,
                             fontweight="bold")
                ax.set_xlabel(f"S2 {mlabel}", fontsize=8)
                ax.set_ylabel(f"S3 {mlabel}", fontsize=8)
                ax.tick_params(axis="both", labelsize=7)
                for spine in ("top", "right"):
                    ax.spines[spine].set_visible(False)
                ax.grid(True, linestyle=":", color="#ddd",
                        linewidth=0.5, zorder=0)

    # Shared legend.
    handles = [
        plt.Line2D([0], [0], marker="o", color="#27ae60", linestyle="",
                   markersize=8, label="rep (S2 → S3)",
                   markeredgecolor="white"),
        plt.Line2D([0], [0], linestyle="--", color="#999",
                   label=r"$y = x$ (S3 = S2)"),
    ]
    fig.legend(handles=handles, loc="upper center",
               bbox_to_anchor=(0.5, 1.0), ncol=2, frameon=False, fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.94])

    out_dir = PAPER_DATA / TAGS["ssd"] / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "tpchi_lsm_absolute_scatter"
    for ext in ("pdf", "png"):
        path = base.with_suffix(f".{ext}")
        fig.savefig(path, bbox_inches="tight",
                    dpi=200 if ext == "png" else None)
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    plt.close(fig)


def render_tpchi_s2_vs_s3() -> None:
    """2x3 grid: rows = query (q3i, q5i), cols = metric.

    Each panel plots S2 value (x) vs S3 value (y) per rep, with disk
    distinguishing the two clusters (SSD = orange, HDD = purple).
    Dashed y=x diagonal; points above diagonal mean S3 is worse than
    S2 on that metric. Absolute units throughout — no ratio-over-ratio.
    """
    queries = ["q3i", "q5i"]
    metrics = [  # (column, axis label, source file, unit scale, unit suffix)
        ("ms_per_tx",          "query latency",        "headline.csv",    1e3, "s/tx"),
        ("sst_read_us_per_tx", "sst_read",             "diagnostics.csv", 1e6, "s/tx"),
        ("cpu_cycles_per_tx",  "cpu_cycles",           "diagnostics.csv", 1e9, "Gcyc/tx"),
    ]
    disk_style = {  # disk: (color, label)
        "ssd": ("#e67e22", "SSD"),
        "hdd": ("#8e44ad", "HDD"),
    }

    fig, axes = plt.subplots(2, 3, figsize=(7.5, 5.2))

    # Precompute per-metric shared limits (across both queries and both
    # disks) and start from 0, so every panel of the same metric uses
    # an identical (0, hi) box.
    panel_pts = {}  # (q, col) -> list of (xs, ys, color, label)
    metric_max = {col: 0.0 for col in range(len(metrics))}
    for row, q in enumerate(queries):
        for col, (metric, mlabel, fname, scale, unit) in enumerate(metrics):
            entries = []
            for disk, (color, dlabel) in disk_style.items():
                df = load_per_rep(TAGS[disk], q, [metric], fname)
                wide = df.pivot(index="rep", columns="structure", values=metric)
                if 2 not in wide.columns or 3 not in wide.columns:
                    continue
                xs = (wide[2] / scale).tolist()
                ys = (wide[3] / scale).tolist()
                entries.append((xs, ys, color, dlabel))
                metric_max[col] = max(metric_max[col], max(xs + ys))
            panel_pts[(q, col)] = entries

    for row, q in enumerate(queries):
        for col, (metric, mlabel, fname, scale, unit) in enumerate(metrics):
            ax = axes[row, col]

            for xs, ys, color, dlabel in panel_pts[(q, col)]:
                ax.scatter(xs, ys, s=70, color=color, marker="o",
                           edgecolor="white", linewidth=0.6,
                           label=(dlabel if (row == 0 and col == 0) else None),
                           zorder=3)

            lo = 0.0
            hi = metric_max[col] * 1.05 or 1.0
            # y = x diagonal — S3 == S2.
            ax.plot([lo, hi], [lo, hi], "--", color="#999",
                    linewidth=0.7, zorder=1)
            ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)
            ax.set_aspect("equal")

            # Shade "S3 better" half (below the diagonal).
            ax.fill_between([lo, hi], [lo, lo], [lo, hi],
                            color="#27ae60", alpha=0.07, zorder=0)
            ax.text(hi - (hi - lo) * 0.04, lo + (hi - lo) * 0.04,
                    "S3 better", ha="right", va="bottom",
                    fontsize=7, color="#1e7e3c", style="italic")

            ax.set_xlabel(f"S2 {mlabel} ({unit})", fontsize=8)
            ax.set_ylabel(f"S3 {mlabel} ({unit})", fontsize=8)
            ax.tick_params(axis="both", labelsize=7)
            for spine in ("top", "right"):
                ax.spines[spine].set_visible(False)
            ax.grid(True, linestyle=":", color="#ddd",
                    linewidth=0.5, zorder=0)

            ax.set_title(f"{q}  ·  {mlabel}", fontsize=9, fontweight="bold")

    # Shared legend at the top.
    handles = [
        plt.Line2D([0], [0], marker="o", color=color, linestyle="",
                   markersize=8, label=label, markeredgecolor="white")
        for color, label in disk_style.values()
    ]
    handles.append(plt.Line2D([0], [0], linestyle="--", color="#999",
                              label=r"$y = x$ (S3 = S2)"))
    fig.legend(handles=handles, loc="upper center",
               bbox_to_anchor=(0.5, 1.0), ncol=3, frameon=False, fontsize=9)

    fig.tight_layout(rect=[0, 0, 1, 0.94])

    out_dir = PAPER_DATA / TAGS["ssd"] / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = out_dir / "tpchi_lsm_s2_vs_s3"
    for ext in ("pdf", "png"):
        path = base.with_suffix(f".{ext}")
        fig.savefig(path, bbox_inches="tight",
                    dpi=200 if ext == "png" else None)
        print(f"wrote {path.relative_to(REPO_ROOT)}")
    plt.close(fig)


if __name__ == "__main__":
    render()
    render_correlations()
    render_tpchi_absolute()
    render_tpchi_s2_vs_s3()
