#!/usr/bin/env python3
"""Generate experiment_numbers.{json,tex} from the latest paper-data sweeps.

The paper's prose (sections/experiments_revised.tex) references these
values via \\auto* macros declared in experiment_numbers.tex. Run this
script after any sweep refresh; latexmk picks up the new values on the
next paper build. See paper-data/.claude/plans/summarize-experiment-
results-not-async-minsky.md for the substitution table and rationale.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
HEADLINE_TAG = "2026-05-29-rep0-10L"
REFRESH_TAG = "2026-05-30-refresh-10L"
DBT_TAG = "2026-05-24-dbtoaster"

DEFAULT_TEX_DIR = REPO.parent.parent / "merged_index_interesting_orderings" / "sections"


# ---------------------------------------------------------------- loaders


def load_headline() -> pd.DataFrame:
    df = pd.read_csv(REPO / HEADLINE_TAG / "summary" / "headline.csv")
    df = df[(df["cell"] == "c4") & (df["bg"] == 2) & (df["tx"] == "query")]
    return df.groupby(
        ["binary", "family", "backend", "query", "structure", "method"],
        as_index=False,
    ).agg(
        size_mib=("size_mib", "median"),
        tx_per_s=("tx_per_s", "median"),
        ms_per_tx=("ms_per_tx", "median"),
    )


def load_refresh(name: str) -> pd.DataFrame:
    return pd.read_csv(REPO / REFRESH_TAG / "summary" / name)


def load_dbtoaster() -> pd.DataFrame:
    return pd.read_csv(REPO / DBT_TAG / "summary" / "refresh_sales_dbtoaster_throughput.csv")


# --------------------------------------------------------------- helpers


def size_gib(df: pd.DataFrame, binary: str, structure: int) -> float:
    row = df[(df["binary"] == binary) & (df["structure"] == structure)]
    assert len(row) == 1, f"expected 1 row, got {len(row)} for {binary} S{structure}"
    return float(row["size_mib"].iloc[0]) / 1024.0


def base_size_gib(df: pd.DataFrame, binary: str) -> float:
    """S4 (base_hash) has no secondary structures; treat it as base footprint."""
    return size_gib(df, binary, 4)


def view_delta_gib(df: pd.DataFrame, binary: str, structure: int) -> float:
    """View / MI delta over base tables (S<structure> minus S4)."""
    return size_gib(df, binary, structure) - base_size_gib(df, binary)


def joint_image_gib(df: pd.DataFrame, family_binaries: list[str], structure: int) -> float:
    """Joint image = base + sum of per-query view/MI deltas across binaries.

    For shared structures (S3 MI_B), this collapses to base + one delta.
    For per-query structures (S2), it sums deltas across the binaries that
    contribute their own views to the image. Used for Table 2 DB sizes.
    """
    base = base_size_gib(df, family_binaries[0])
    deltas = sum(view_delta_gib(df, b, structure) for b in family_binaries)
    return base + deltas


def pair_latency_ratio(df: pd.DataFrame, s_slow: int, s_fast: int, **filt) -> float:
    """latency = 1 / pair_tps; ratio s_slow.latency / s_fast.latency = s_fast.tps / s_slow.tps."""
    sel = df.copy()
    for k, v in filt.items():
        sel = sel[sel[k] == v]
    fast = sel[sel["structure"] == s_fast]["pair_tps"].iloc[0]
    slow = sel[sel["structure"] == s_slow]["pair_tps"].iloc[0]
    return float(fast / slow)


def dbtoaster_pair_tps(dbt: pd.DataFrame) -> tuple[float, float]:
    """Pick the largest-SF DBT row (maxes RSS toward the 9 GiB budget).

    Returns (harmonic-mean pair_tps, peak_rss_gib).
    """
    row = dbt.sort_values("sf").iloc[-1]
    rf1 = float(row["rf1_orders_per_s"])
    rf2 = float(row["rf2_orders_per_s"])
    pair = 1.0 / (1.0 / rf1 + 1.0 / rf2)
    rss_gib = float(row["peak_rss_kb"]) / (1024.0 * 1024.0)
    return pair, rss_gib


# --------------------------------------------------------------- macro spec


@dataclass
class Macro:
    key: str
    macro: str
    unit: str
    fmt: str
    filter: str
    compute: Callable[[dict], float]

    def render(self, ctx: dict) -> tuple[float, str]:
        v = float(self.compute(ctx))
        return v, self.fmt.format(v)


VANILLA_BTREE = ["q3_btree", "q5_btree", "q10_btree"]
VANILLA_LSM = ["q3_lsm", "q5_lsm", "q10_lsm"]


SPECS: list[Macro] = [
    # ----- Table 2 DB Size column (vanilla btree, joint image) -----
    Macro("db_size_base_hash", r"\autoDbSizeBaseHash", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S4 = base tables only",
          lambda c: base_size_gib(c["hd"], "q3_btree")),
    Macro("db_size_base_merge", r"\autoDbSizeBaseMerge", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S1 shared secondary indexes (joint)",
          lambda c: size_gib(c["hd"], "q3_btree", 1)),
    Macro("db_size_mat_view", r"\autoDbSizeMatView", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S2 joint = base + Q3+Q5+Q10 naive view deltas",
          lambda c: joint_image_gib(c["hd"], VANILLA_BTREE, 2)),
    Macro("db_size_mat_view_partial", r"\autoDbSizeMatViewPartial", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S2(q3+q5) + S7(q10 partial-agg) joint",
          lambda c: base_size_gib(c["hd"], "q3_btree")
                    + view_delta_gib(c["hd"], "q3_btree", 2)
                    + view_delta_gib(c["hd"], "q5_btree", 2)
                    + view_delta_gib(c["hd"], "q10_btree", 7)),
    Macro("db_size_merged_idx", r"\autoDbSizeMergedIdx", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S3 shared MI_B (joint)",
          lambda c: size_gib(c["hd"], "q3_btree", 3)),

    # ----- footnote 5 (Q10 view sizes, B-tree) -----
    Macro("db_size_q10_naive_view", r"\autoDbSizeQTenNaiveView", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S2.q10 view delta over base",
          lambda c: view_delta_gib(c["hd"], "q10_btree", 2)),
    Macro("db_size_q10_partial_view", r"\autoDbSizeQTenPartialView", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S7.q10 partial-agg view delta over base",
          lambda c: view_delta_gib(c["hd"], "q10_btree", 7)),

    # ----- line 200 (per-query MI vs secondary, B-tree) -----
    Macro("db_size_mi_btree", r"\autoDbSizeMiBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S3.q3 MI_B delta over base",
          lambda c: view_delta_gib(c["hd"], "q3_btree", 3)),
    Macro("db_size_secondary_btree", r"\autoDbSizeSecondaryBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S1.q3 secondary-indexes delta over base",
          lambda c: view_delta_gib(c["hd"], "q3_btree", 1)),

    # ----- lines 203-204 (per-query view vs MI, B-tree) -----
    Macro("db_size_view_q3_btree", r"\autoDbSizeViewQThreeBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S2.q3 view delta over base",
          lambda c: view_delta_gib(c["hd"], "q3_btree", 2)),
    Macro("db_size_view_q5_btree", r"\autoDbSizeViewQFiveBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S2.q5 view delta over base",
          lambda c: view_delta_gib(c["hd"], "q5_btree", 2)),
    Macro("db_size_view_q10_partial_btree", r"\autoDbSizeViewQTenPartialBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S7.q10 partial-agg view delta over base",
          lambda c: view_delta_gib(c["hd"], "q10_btree", 7)),
    Macro("db_size_mi_q10_partial_btree", r"\autoDbSizeMiQTenPartialBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; S5.q10 partial-agg MI incremental size (S5 minus shared MI_B baseline)",
          lambda c: view_delta_gib(c["hd"], "q10_btree", 5) - view_delta_gib(c["hd"], "q3_btree", 3)),

    # ----- line 206 (total views across family, B-tree) -----
    Macro("db_size_views_total_btree", r"\autoDbSizeViewsTotalBtree", "GiB", "{:.2f}",
          "vanilla,btree,bg=2,c4; sum of S2.q3 + S2.q5 + S7.q10 view deltas",
          lambda c: view_delta_gib(c["hd"], "q3_btree", 2)
                    + view_delta_gib(c["hd"], "q5_btree", 2)
                    + view_delta_gib(c["hd"], "q10_btree", 7)),

    # ----- line 214 (LSM sizes) -----
    Macro("db_size_mi_lsm", r"\autoDbSizeMiLsm", "GiB", "{:.2f}",
          "vanilla,lsm,bg=2,c4; S3.q3 MI_B delta over base",
          lambda c: view_delta_gib(c["hd"], "q3_lsm", 3)),
    Macro("db_size_view_q3_lsm", r"\autoDbSizeViewQThreeLsm", "GiB", "{:.2f}",
          "vanilla,lsm,bg=2,c4; S2.q3 view delta over base",
          lambda c: view_delta_gib(c["hd"], "q3_lsm", 2)),
    Macro("db_size_view_q5_lsm", r"\autoDbSizeViewQFiveLsm", "GiB", "{:.2f}",
          "vanilla,lsm,bg=2,c4; S2.q5 view delta over base",
          lambda c: view_delta_gib(c["hd"], "q5_lsm", 2)),

    # ----- ratios -----
    Macro("ratio_lsm_invoice_low", r"\autoRatioLsmInvoiceLow", "x", "{:.1f}",
          "tpchi,lsm,bg=2,c4,query; min over (q3i,q5i) of S2.ms / S3.ms (S3 slower => ratio>1)",
          lambda c: min(
              c["hd"][(c["hd"]["binary"] == b) & (c["hd"]["structure"] == 3)]["ms_per_tx"].iloc[0]
              / c["hd"][(c["hd"]["binary"] == b) & (c["hd"]["structure"] == 2)]["ms_per_tx"].iloc[0]
              for b in ("q3i_lsm", "q5i_lsm"))),
    Macro("ratio_lsm_invoice_high", r"\autoRatioLsmInvoiceHigh", "x", "{:.1f}",
          "tpchi,lsm,bg=2,c4,query; max over (q3i,q5i) of S3.ms / S2.ms",
          lambda c: max(
              c["hd"][(c["hd"]["binary"] == b) & (c["hd"]["structure"] == 3)]["ms_per_tx"].iloc[0]
              / c["hd"][(c["hd"]["binary"] == b) & (c["hd"]["structure"] == 2)]["ms_per_tx"].iloc[0]
              for b in ("q3i_lsm", "q5i_lsm"))),
    Macro("ratio_q10_btree_merged_vs_merge", r"\autoRatioQTenBtreeMergedVsMerge", "x", "{:.1f}",
          "vanilla,btree,q10,bg=2,c4,query; S3.ms / S1.ms (Merged-Idx vs Base-Merge)",
          lambda c:
              c["hd"][(c["hd"]["binary"] == "q10_btree") & (c["hd"]["structure"] == 3)]["ms_per_tx"].iloc[0]
              / c["hd"][(c["hd"]["binary"] == "q10_btree") & (c["hd"]["structure"] == 1)]["ms_per_tx"].iloc[0]),

    # ----- update latency ratio (1 GiB, bg=2) -----
    Macro("ratio_update_merge_vs_hash", r"\autoRatioUpdateMergeVsHash", "x", "{:.1f}",
          "10L bg=2 btree; S4.pair_tps / S1.pair_tps (Base-Merge slower than Base-Hash)",
          lambda c: pair_latency_ratio(c["rf"], s_slow=1, s_fast=4, backend="btree")),

    # ----- DBToaster (caveat: scales differ; see JSON note) -----
    Macro("dbtoaster_pair_tps", r"\autoDbtoasterPairTps", "pairs/s", "{:.0f}",
          "dbtoaster max-SF (0.36) row; harmonic mean of rf1/rf2 orders/s",
          lambda c: dbtoaster_pair_tps(c["dbt"])[0]),
    Macro("dbtoaster_peak_rss", r"\autoDbtoasterPeakRss", "GiB", "{:.1f}",
          "dbtoaster max-SF (0.36) row; peak_rss_kb / 2^20",
          lambda c: dbtoaster_pair_tps(c["dbt"])[1]),
    Macro("matview_pair_tps_10ll", r"\autoMatViewPairTpsTenLL", "pairs/s", "{:.0f}",
          "10LL btree, S2 pair_tps (DRAM=20, bg=0)",
          lambda c: float(c["rfll"][c["rfll"]["structure"] == 2]["pair_tps"].iloc[0])),
    Macro("merged_pair_tps_10ll", r"\autoMergedPairTpsTenLL", "pairs/s", "{:.0f}",
          "10LL btree, S3 pair_tps (DRAM=20, bg=0)",
          lambda c: float(c["rfll"][c["rfll"]["structure"] == 3]["pair_tps"].iloc[0])),

    # ----- scale factor constants -----
    Macro("sf_btree", r"\autoSfBtree", "SF", "{:.0f}",
          "SF_TPCH[btree][c4]",
          lambda c: 4000.0),
    Macro("sf_lsm", r"\autoSfLsm", "SF", "{:.0f}",
          "SF_TPCH[lsm][c4]",
          lambda c: 10000.0),

    # ----- base-image footprint (for environment footnote) -----
    Macro("base_size_btree", r"\autoBaseSizeBtree", "GiB", "{:.1f}",
          "vanilla,btree,bg=2,c4; S4 base tables only",
          lambda c: base_size_gib(c["hd"], "q3_btree")),
    Macro("base_size_lsm", r"\autoBaseSizeLsm", "GiB", "{:.1f}",
          "vanilla,lsm,bg=2,c4; S4 base tables only",
          lambda c: base_size_gib(c["hd"], "q3_lsm")),
    Macro("density_ratio_lsm_btree", r"\autoDensityRatioLsmBtree", "x", "{:.1f}",
          "(btree base GiB / btree SF) / (lsm base GiB / lsm SF); RocksDB-vs-LeanStore density",
          lambda c: (base_size_gib(c["hd"], "q3_btree") / 4000.0)
                    / (base_size_gib(c["hd"], "q3_lsm") / 10000.0)),

    # ----- update pair_tps ratios -----
    Macro("ratio_update_merged_vs_merge_btree", r"\autoRatioUpdateMergedVsMergeBtree", "x", "{:.1f}",
          "10L bg=2 btree; S3.pair_tps / S1.pair_tps (Merged-Idx faster than Base-Merge if >1)",
          lambda c: float(c["rf"][(c["rf"]["backend"] == "btree") & (c["rf"]["structure"] == 3)]["pair_tps"].iloc[0])
                    / float(c["rf"][(c["rf"]["backend"] == "btree") & (c["rf"]["structure"] == 1)]["pair_tps"].iloc[0])),
    Macro("ratio_update_hash_vs_merge_lsm", r"\autoRatioUpdateHashVsMergeLsm", "x", "{:.2f}",
          "10L bg=2 lsm; S4.pair_tps / S1.pair_tps (<1 means Base-Hash slower than Base-Merge — flipped from btree)",
          lambda c: float(c["rf"][(c["rf"]["backend"] == "lsm") & (c["rf"]["structure"] == 4)]["pair_tps"].iloc[0])
                    / float(c["rf"][(c["rf"]["backend"] == "lsm") & (c["rf"]["structure"] == 1)]["pair_tps"].iloc[0])),
]


# ------------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tex-dir", type=Path, default=DEFAULT_TEX_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ctx = {
        "hd": load_headline(),
        "rf": load_refresh("refresh_sales_10L_bg2_throughput.csv"),
        "rfll": load_refresh("refresh_sales_10LL_throughput.csv"),
        "dbt": load_dbtoaster(),
    }

    results = {}
    tex_lines = [
        "% AUTO-GENERATED by paper-data/scripts/refresh_tex_numbers.py",
        f"% Source: {HEADLINE_TAG} (headline), {REFRESH_TAG} (refresh), {DBT_TAG} (DBToaster).",
        "% Regenerate: python3 paper-data/scripts/refresh_tex_numbers.py",
        "% Do not edit by hand.",
        "",
    ]
    for spec in SPECS:
        value, formatted = spec.render(ctx)
        results[spec.key] = {
            "value": value,
            "formatted": formatted,
            "unit": spec.unit,
            "format": spec.fmt,
            "filter": spec.filter,
            "macro": spec.macro,
        }
        tex_lines.append(f"\\newcommand{{{spec.macro}}}{{{formatted}}}% {spec.unit}; {spec.filter}")

    payload = {
        "_meta": {
            "headline_tag": HEADLINE_TAG,
            "refresh_tag": REFRESH_TAG,
            "dbtoaster_tag": DBT_TAG,
            "note_dbtoaster_scale": (
                "DBToaster numbers are from its largest viable SF (0.36, ~8.65 GiB RSS); "
                "the 10LL Mat-View / Merged-Idx baselines run at SF=4000 (DRAM=20 GiB, bg=0). "
                "Scales differ; treat the cross-tool ratio as qualitative."
            ),
            "note_q10_partial_inversion": (
                "At SF=4000 the Q10 partial-agg merged index (S5) is larger than the "
                "partial-agg view (S7); the prose claim 'view consumes more' no longer holds "
                "for Q10 partial-agg. See db_size_mi_q10_partial_btree vs "
                "db_size_view_q10_partial_btree."
            ),
        },
        "values": results,
    }

    if args.dry_run:
        json.dump(payload, sys.stdout, indent=2)
        sys.stdout.write("\n\n--- experiment_numbers.tex ---\n")
        sys.stdout.write("\n".join(tex_lines) + "\n")
        return

    args.tex_dir.mkdir(parents=True, exist_ok=True)
    (args.tex_dir / "experiment_numbers.json").write_text(
        json.dumps(payload, indent=2) + "\n")
    (args.tex_dir / "experiment_numbers.tex").write_text("\n".join(tex_lines) + "\n")
    print(f"[refresh_tex_numbers] wrote experiment_numbers.{{json,tex}} to {args.tex_dir}")
    for k, v in results.items():
        print(f"  {v['macro']:<45s} = {v['formatted']:>10s} {v['unit']}")


if __name__ == "__main__":
    main()
