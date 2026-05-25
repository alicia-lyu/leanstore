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
# usetex so \textsc{...} in the bar xticklabels renders as proper
# small caps, matching tab:exp-baselines in the paper. Requires a
# working LaTeX install (TeX Live's pdflatex on this machine).
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

BACKENDS = ["btree"]

# Per-CSV-schema view: which throughput column is the headline, what
# unit to label it with, and the figure basename. 5L files report a
# size-stable RF1+RF2 *pair* throughput (RF2 doesn't exhaust at SF≥15),
# which is the TPC-H-faithful unit; the SF=1 rf_throughput file falls
# back to RF1-only because its RF2 reservoir drains early.
CSV_SCHEMAS = {
    "refresh_sales_5L_throughput.csv": {
        "tps_col": "pair_tps_tail30",
        "unit": r"$\mu$s / RF pair",
        "scale": 1e6,  # tps → µs/op
        "basename": "refresh_5L_pair_latency",
    },
    "refresh_prewarm9_throughput.csv": {
        # In-memory prewarm. LeanStore's harness measures the
        # interleaved RF1+RF2 pair directly per iteration but writes
        # ``pair_orders_per_s = (rf1_orders + rf2_orders) / pair_time``
        # — i.e. 2× the pair rate, counting both an insert and a
        # delete as separate "orders". Convert back to µs/pair via
        # ``2e6 / pair_med_ops`` so we report the directly-measured
        # pair latency (no posteriori reconstruction from per-phase
        # rates, unlike DBToaster which can't measure pairs).
        "tps_col": "pair_med_ops",
        "scale": 2e6,  # 2 (orders/pair) × 1e6 (µs/s)
        "unit": r"$\mu$s / RF pair",
        "basename": "refresh_prewarm9_pair_latency",
    },
    "refresh_sales_rf_throughput.csv": {
        "tps_col": "rf1_tps_hot",
        "unit": r"$\mu$s / RF1 insert (hot)",
        "scale": 1e6,
        "basename": "refresh_sales_rf1_latency",
    },
}

# Sibling tag holding the 9 GiB in-memory refresh data (LeanStore-only,
# btree). Auto-loaded alongside the primary --tag's 5L data so the
# 1 GiB + 9 GiB panels both get real bars.
PREWARM9_SIBLING = "2026-05-24-refresh-prewarm9"
PREWARM9_BUDGET_GIB = 9.0


def _load_dbtoaster(path: Optional[Path]) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _dbtoaster_pair_us(db_df: pd.DataFrame,
                       budget_gib: float) -> Optional[float]:
    """Return DBToaster's pair latency (µs) at the largest SF whose
    ``peak_rss_kb`` fits ``budget_gib``. ``None`` means OOM — no SF row
    fits, so the caller should render this as an "infinite" bar.
    """
    if db_df.empty:
        return None
    # Pin to the largest SF DBToaster managed — that's the "comparable
    # to LeanStore 5L" anchor (manifest: SF=0.36 ≈ 5 GiB working set).
    # Smaller SFs aren't meaningful baselines for the LeanStore scales.
    df = db_df
    if "mode" in df.columns:
        # Drop SUPERSEDED phased_derived rows; keep measured interleaved.
        df = df[df["mode"] == "interleaved"]
        if df.empty:
            df = db_df
    sf_max = df["sf"].max()
    rows = df[df["sf"] == sf_max]
    budget_kb = budget_gib * 1024 * 1024
    if float(rows["peak_rss_kb"].iloc[0]) > budget_kb:
        return None  # caller renders OOM
    # Prefer the directly measured interleaved pair rate when present
    # (median across runs). Falls back to the phased-derived sum of
    # per-op latencies for the legacy throughput CSV.
    if "pairs_per_s_med" in rows.columns and rows["pairs_per_s_med"].notna().any():
        pairs_per_s = float(rows["pairs_per_s_med"].median())
        if pairs_per_s > 0:
            return 1e6 / pairs_per_s
    row = rows.iloc[0]
    rf1 = float(row["rf1_orders_per_s"])
    rf2 = float(row["rf2_orders_per_s"])
    if rf1 <= 0 or rf2 <= 0:
        return None
    # RF1 + RF2 are separate phases in DBToaster (see manifest caveat).
    # Sum the per-op latencies as a comparable "size-stable pair" proxy.
    return 1e6 / rf1 + 1e6 / rf2


# Two memory budgets shown side-by-side. 1 GiB = current LeanStore data
# (dram_gib=1.0 in the 5L summaries); DBToaster OOMs at the SFs we care
# about. 9 GiB = LeanStore data pending; DBToaster's largest SF
# (~8.6 GiB peak RSS) fits.
MEMORY_BUDGETS: List[Tuple[float, str]] = [(1.0, "1 GiB DRAM"),
                                           (9.0, "9 GiB DRAM")]


# Labels frame the y-axis question: *what extra storage structure
# (beyond primary indexes) does Q3 / Q5 maintain?* S4 = none; S1 =
# extra secondary indexes; S2 = pipeline views; S3 = merged indexes.
# Labels mirror tab:exp-baselines in the paper. Wrapped in
# \textsc{} and rendered via matplotlib's usetex backend so the
# typeset small-caps style matches the table verbatim.
REFRESH_LABELS = {
    1: r"\textsc{Base-Merge}",
    2: r"\textsc{Mat-View}",
    3: r"\textsc{Merged-Idx}",
    # S4 (Base-Hash, "None") is intentionally omitted from the refresh
    # figure: with no secondary structures to maintain, it's the
    # trivial ceiling and crowds the comparison.
}
DBTOASTER_LABEL_TEX = r"\textsc{DBToaster}"
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


# With LSM removed, each LeanStore structure gets its own x slot
# labelled with the storage approach, alongside DBToaster. The
# per-bar xticklabel makes the legend redundant.


def _panel(ax, ls_df: pd.DataFrame, ls_series: List[int],
           db_df: pd.DataFrame, budget_gib: float, budget_label: str,
           y_unit: str, show_ylabel: bool) -> None:
    """One panel = one memory budget. One bar per LeanStore-btree
    structure (xticklabel = storage approach), plus a DBToaster bar.

    ``ls_df`` must carry a normalised ``pair_ms`` column and the usual
    ``backend`` / ``structure`` keys. Missing rows render as italic
    "pending" text in that x slot. DBToaster gets one bar at the
    budget-appropriate SF (the largest one fitting peak_rss); if none
    fit we render an "OOM ↑" hatched bar capped at the top of the
    axis after y-limits sync.
    """
    bar_w = 0.7

    # In-memory budgets (9 GiB here) compress into a narrow ~30-100 µs
    # range — linear scale shows the spread honestly. Memory-pressured
    # budgets span 10-1000+ µs, so log stays.
    use_log = budget_gib < PREWARM9_BUDGET_GIB
    ax.set_yscale("log" if use_log else "linear")

    panel_xlabels: List[str] = []
    for i, struct in enumerate(ls_series):
        color, label = _series_style(struct)
        panel_xlabels.append(label)
        row = (ls_df[(ls_df["backend"] == "btree")
                     & (ls_df["structure"] == struct)]
               if not ls_df.empty else pd.DataFrame())
        if row.empty:
            ax.text(i, 0.5, "pending", ha="center", va="center",
                    transform=blended_transform(ax),
                    fontsize=7, color="#999", style="italic")
            continue
        pair_ms = float(row["pair_ms"].iloc[0])
        if not np.isfinite(pair_ms) or pair_ms <= 0:
            continue
        ax.bar([i], [pair_ms], width=bar_w, color=color, linewidth=0)

    db_x = len(ls_series)
    panel_xlabels.append(DBTOASTER_LABEL_TEX)
    db_us = _dbtoaster_pair_us(db_df, budget_gib)
    if db_us is not None:
        ax.bar([db_x], [db_us], width=bar_w,
               color=DBTOASTER_COLOR, linewidth=0)
    elif not db_df.empty:
        # OOM: defer to _annotate_oom after the shared y-limits are known.
        ax._oom_bar = (db_x, bar_w)

    ax.set_title(budget_label, fontsize=9)
    ax.set_xticks(np.arange(len(panel_xlabels)))
    ax.set_xticklabels(panel_xlabels, fontsize=8, rotation=30,
                       ha="right", rotation_mode="anchor")
    ax.set_xlim(-0.5, len(panel_xlabels) - 0.5)
    if use_log:
        # Ticks at 1×, 2×, 5× of each decade with plain decimal labels —
        # log autoscale otherwise leaves narrow ranges tickless.
        ax.yaxis.set_major_locator(
            mticker.LogLocator(base=10.0, subs=(1.0, 2.0, 5.0), numticks=10))
        ax.yaxis.set_major_formatter(mticker.ScalarFormatter())
        ax.yaxis.set_minor_locator(mticker.NullLocator())
    else:
        ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=6))
        ax.yaxis.set_major_formatter(mticker.ScalarFormatter())
    ax.tick_params(axis="y", which="major", labelsize=7)
    ax.yaxis.grid(True, linestyle=":", alpha=0.4)
    ax.set_axisbelow(True)
    if show_ylabel:
        ax.set_ylabel(y_unit, fontsize=8)


def blended_transform(ax):
    """data-x, axes-y — for placing "pending" text at a given x col."""
    from matplotlib.transforms import blended_transform_factory
    return blended_transform_factory(ax.transData, ax.transAxes)


def _annotate_oom(ax) -> None:
    """Render a hatched bar from the y-axis minimum to the top, with an
    upward arrow + "OOM" annotation. Called after shared y-limits are
    fixed across panels so the bar reaches the visible top."""
    spec = getattr(ax, "_oom_bar", None)
    if spec is None:
        return
    x, bar_w = spec
    ymin, ymax = ax.get_ylim()
    ax.bar([x], [ymax - ymin], width=bar_w, bottom=ymin,
           color="none", edgecolor=DBTOASTER_COLOR,
           hatch="///", linewidth=0.8, clip_on=False)
    # Re-pin y-limits — the bar would otherwise expand the autoscale.
    ax.set_ylim(ymin, ymax)
    # Label sits in axes coords just above the top spine so it can't
    # collide with the bar or be clipped by the data area.
    ax.annotate(r"OOM $\uparrow$", xy=(x, 1.0), xycoords=("data", "axes fraction"),
                xytext=(0, 2), textcoords="offset points",
                ha="center", va="bottom", fontsize=7,
                color=DBTOASTER_COLOR, annotation_clip=False)


def _find_schema(summary: Path) -> Tuple[Optional[Dict], Optional[Path]]:
    """Pick the first CSV_SCHEMAS entry whose file exists under summary."""
    for name, spec in CSV_SCHEMAS.items():
        candidate = summary / name
        if candidate.exists():
            return spec, candidate
    return None, None


def _load_ls(csv_path: Path, schema: Dict) -> pd.DataFrame:
    """Read a LeanStore refresh CSV and normalise it to a uniform shape:
    add a ``pair_ms`` column (actually µs; the column name is a legacy
    holdover) so the panel code is schema-agnostic.

    Two schema dialects:
      - ``tps_col`` + ``scale``: pair_us = scale / tps_col. Used when
        the CSV already reports a pair throughput.
      - ``rf1_col`` + ``rf2_col``: pair_us = 1e6/rf1 + 1e6/rf2 — one
        RF1 op + one RF2 op, matching DBToaster's separate-phase
        definition. Use this when the CSV reports per-phase rates.
    """
    df = pd.read_csv(csv_path)
    if "tps_col" in schema:
        col = schema["tps_col"]
        if col not in df.columns:
            print(f"[plot_refresh_sales] WARN: {csv_path.name} missing {col}",
                  file=sys.stderr)
            df["pair_ms"] = float("nan")
            return df
        tps = pd.to_numeric(df[col], errors="coerce")
        df["pair_ms"] = schema["scale"] / tps.where(tps > 0)
    else:
        rf1 = pd.to_numeric(df[schema["rf1_col"]], errors="coerce")
        rf2 = pd.to_numeric(df[schema["rf2_col"]], errors="coerce")
        df["pair_ms"] = 1e6 / rf1.where(rf1 > 0) + 1e6 / rf2.where(rf2 > 0)
    return df


def _disk_tag(df: pd.DataFrame) -> Optional[str]:
    if "disk" not in df.columns:
        return None
    vals = sorted({str(v) for v in df["disk"].dropna().unique()})
    if len(vals) > 1:
        print(f"[plot_refresh_sales] WARN: mixed disk media: {vals}",
              file=sys.stderr)
    return vals[0] if vals else None


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
    schema, csv_path = _find_schema(summary)
    if csv_path is None:
        print(f"[plot_refresh_sales] error: no refresh CSV under {summary} "
              f"(expected one of {list(CSV_SCHEMAS)})", file=sys.stderr)
        return 1

    df = _load_ls(csv_path, schema)
    disk = _disk_tag(df)
    # Auto-load the 9 GiB prewarm sibling when present. It lives in a
    # separate tag (different binary build), but conceptually it's the
    # second memory-budget panel of the same figure.
    prewarm9 = root.parent / PREWARM9_SIBLING / "summary" \
               / "refresh_prewarm9_throughput.csv"
    df_9g = (_load_ls(prewarm9, CSV_SCHEMAS["refresh_prewarm9_throughput.csv"])
             if prewarm9.exists() else pd.DataFrame())

    # DBToaster lives in its own tag (no LeanStore rows there).
    if args.dbtoaster is not None:
        dbtoaster_path = args.dbtoaster
    else:
        db_summary = root.parent / "2026-05-24-dbtoaster" / "summary"
        # Prefer the measured-interleaved CSV (typed on_insert/on_delete
        # 1+1 pair) over the legacy phased throughput CSV, which carried
        # SUPERSEDED derived rates.
        candidates = [
            db_summary / "refresh_sales_dbtoaster_interleaved.csv",
            db_summary / "refresh_sales_dbtoaster_throughput.csv",
            summary / "dbtoaster_rf_throughput.csv",
        ]
        dbtoaster_path = next((p for p in candidates if p.exists()),
                              candidates[-1])
    db_df = _load_dbtoaster(dbtoaster_path)

    # Bucket LeanStore data by buffer-pool budget so each panel pulls
    # its own bars. Missing budgets render as "pending" placeholders.
    primary_budget = float(df["dram_gib"].iloc[0]) if "dram_gib" in df.columns else 1.0
    ls_by_budget: Dict[float, pd.DataFrame] = {primary_budget: df}
    if not df_9g.empty:
        ls_by_budget[PREWARM9_BUDGET_GIB] = df_9g

    ls_series = [s for s in PAPER_LEGEND_ORDER if s not in REFRESH_OMIT]
    # Each panel autoscales its own y-range: in-memory (9 GiB) is
    # 10-100× faster than SSD-spilling (1 GiB), so a shared axis would
    # squash one or the other. The unit is the same (µs/pair), the
    # ticks just live at different decades.
    # Narrower canvas — the \textsc xticklabels are angled below so
    # they fit without horizontal overlap. Same height as before.
    fig, axes = plt.subplots(1, len(MEMORY_BUDGETS), figsize=(5.6, 2.6),
                             sharey=False)
    for j, (budget, label) in enumerate(MEMORY_BUDGETS):
        budget_df = ls_by_budget.get(budget, pd.DataFrame())
        _panel(axes[j], budget_df, ls_series, db_df,
               budget_gib=budget, budget_label=label,
               y_unit=schema["unit"], show_ylabel=(j == 0))
    # Floor each panel's y-axis so bar height matches the labelled
    # value. Log axes: snap ymin down to the nearest decade (autoscale
    # otherwise lands just under the minimum value, drawing the
    # shortest bar as a near-zero stub). Linear axes: anchor at 0 so
    # bars draw from the natural baseline.
    for ax in axes:
        ymin, ymax = ax.get_ylim()
        if ax.get_yscale() == "log" and ymin > 0:
            floor = 10 ** np.floor(np.log10(ymin))
            ax.set_ylim(floor, ymax)
        elif ax.get_yscale() == "linear":
            ax.set_ylim(0, ymax)
    # OOM bar is drawn after each panel's y-floor + autoscale settle.
    for ax in axes:
        _annotate_oom(ax)

    # Legend removed: each bar's xticklabel names its storage approach,
    # making a separate legend redundant.

    out_dir = root / "figures" / "paper"
    out_dir.mkdir(parents=True, exist_ok=True)
    basename = schema["basename"] + (f"_{disk}" if disk else "")
    base = out_dir / basename
    fig.subplots_adjust(left=0.10, right=0.98, top=0.90, bottom=0.16,
                        wspace=0.30)
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
