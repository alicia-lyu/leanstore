#!/usr/bin/env python3
"""Reproduce the paper's space-footprint table (``tab:exp-baselines``)
from sweep ``headline.csv`` files — so the numbers are regenerable, not
hand-pasted.

Method:
  * Per-query *secondary* size = ``size(Sx) - size(S4)`` — S4 (Base-Hash)
    stores only the base tables, so subtracting it isolates the extra
    structure each approach adds.
  * Cross-query *aggregate* per family counts each **shared** structure
    once: Base-Merge's custkey-sorted secondaries and Merged-Idx's MI are
    shared across the family (one image serves both queries), so their
    total is the single shared structure on top of the base. Mat-View is
    **not** shared — it stores one view per query, so its total sums the
    per-query view secondaries.
  * S6 ("Mat-View-Shared", vanilla only): one COL union view shared across the
    family (size(S6) - size(S4)), counted ONCE like the MI — printed alongside
    the per-query Mat-View sum so the "could a view be shared too?" question is
    answered with a measured number (the union row is wide, so S6 may exceed
    any single per-query view and approach the sum). NaN-skipped where the S6
    sweep hasn't run / for the invoice family.

Q10/Q10i variants (two columns reported per family):
  * **naive**   — Q10 uses the per-lineitem ``pipeline_view`` for
    Mat-View and the standard MI_B for Merged-Idx (shared with Q3/Q5).
  * **preagg**  — Q10 uses ``pipeline_view_preagg`` (one row per order)
    for Mat-View and the aCOL MI (S5) for Merged-Idx. The aCOL MI lives
    in its **own image** (different schema: customer + orders_acol
    only), so the family Merged-Idx footprint becomes
    ``base + MI_B (shared by Q3/Q5) + aCOL_MI (Q10-specific)``.

Families:
  * vanilla  = {q3, q5, q10}   sharing MI_B  (COL MI)
  * invoice  = {q3i, q5i, q10i} sharing MI_C (COLI MI)
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PAPER_DATA = REPO_ROOT / "paper-data"

# Data sources: the main sweep covers q3/q5/q3i/q5i with bg=2; Q10 and
# Q10i live in their own tags (bg=0, dedicated method variants).
# These defaults are the authoring tags; override via --root + --sources
# when running inside the artifact container.
_DEFAULT_SOURCES = [
    ("2026-05-24-a-ssd", 2),
    ("2026-05-25-q10", 0),
    ("2026-05-25-q10i", 0),
]
SOURCES = _DEFAULT_SOURCES

FAMILIES = {
    "vanilla (MI_B)": ["q3", "q5", "q10"],
    "invoice (MI_C)": ["q3i", "q5i", "q10i"],
}

# For Q10/Q10i S2 has two view variants; for Q3/Q5/Q3i/Q5i S2 has only
# one method (``pipeline_view``). The "preagg" variant only differs for
# Q10/Q10i; the other queries fall through to their single method.
VIEW_METHOD = {
    "naive":  {"q10": "pipeline_view",        "q10i": "pipeline_view"},
    "preagg": {"q10": "pipeline_view_preagg", "q10i": "pipeline_view_preagg"},
}

MIB = 1024.0


def _sizes() -> pd.DataFrame:
    frames = []
    for tag, bg in SOURCES:
        df = pd.read_csv(PAPER_DATA / tag / "summary" / "headline.csv")
        df = df[(df["cell"] == "c0") & (df["bg"] == bg) & (df["tx"] == "query")]
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    return (df.groupby(["query", "backend", "structure", "method"])["size_mib"]
              .median().reset_index())


def _gib(mib: float) -> float:
    return mib / MIB


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=None,
                    metavar="DIR",
                    help="root directory containing the tag sub-directories. "
                         "Default: in-repo paper-data/. "
                         "Pass /results when running inside the artifact container.")
    ap.add_argument("--sources", default=None,
                    metavar="TAG:BG[,TAG:BG,...]",
                    help="comma-separated tag:bg pairs that replace the built-in "
                         "SOURCES list. Example: "
                         "\"tpch-headline:2,tpch-q10:0,tpch-q10i:0\". "
                         "Requires --root.")
    args = ap.parse_args()

    global PAPER_DATA, SOURCES
    if args.root is not None:
        PAPER_DATA = args.root.resolve()
    if args.sources is not None:
        SOURCES = []
        for pair in args.sources.split(","):
            pair = pair.strip()
            if not pair:
                continue
            tag, bg_str = pair.rsplit(":", 1)
            SOURCES.append((tag.strip(), int(bg_str.strip())))

    sizes = _sizes()

    def size(query: str, backend: str, struct: int, method: str | None = None) -> float:
        sel = ((sizes["query"] == query) & (sizes["backend"] == backend)
               & (sizes["structure"] == struct))
        rows = sizes[sel]
        if method is not None:
            m = rows[rows["method"] == method]
            if not m.empty:
                rows = m
        if rows.empty:
            return float("nan")
        # When multiple methods exist for the same (q,b,s) and no filter
        # narrows it down, take the median across them (same image).
        return float(rows["size_mib"].median())

    print("# Space footprint (c0, both variants for Q10/Q10i)\n")
    for fam, queries in FAMILIES.items():
        rep = queries[0]
        for backend in ("btree", "lsm"):
            base = size(rep, backend, 4)
            if base != base:
                continue
            split_sec = size(rep, backend, 1) - base   # shared (S1 - S4)
            mi_sec    = size(rep, backend, 3) - base   # shared MI (S3 - S4)

            print(f"## {fam} — {backend}   (family: {', '.join(queries)})")
            print(f"  Base-Hash                       {_gib(base):5.2f} GiB")
            print(f"  Base-Merge (+ shared sec)       {_gib(base+split_sec):5.2f} GiB  "
                  f"[shared split sec {_gib(split_sec):.2f}]")

            # S6 is one shared COL union view, family-wide (vanilla only), so
            # measure it once per (family, backend) outside the variant loop.
            # NaN ⇒ no S6 sweep for this family yet; skip silently.
            shared_sec = size(rep, backend, 6) - base
            has_s6 = shared_sec == shared_sec  # not NaN

            for variant in ("naive", "preagg"):
                view_secs = {}
                for q in queries:
                    method = VIEW_METHOD[variant].get(q)
                    view_secs[q] = size(q, backend, 2, method) - size(q, backend, 4)
                views_total = sum(view_secs.values())
                mat_view = base + views_total

                # Merged-Idx footprint:
                #   naive  → base + shared MI_B (all family members reuse it)
                #   preagg → base + MI_B (Q3/Q5 share) + per-q5 MI for Q10
                #            (aCOL MI is a separate image with a different schema)
                extra_mi = {}  # query-specific MI images beyond the shared MI_B
                if variant == "preagg":
                    for q in queries:
                        if q in ("q10", "q10i"):
                            extra_mi[q] = size(q, backend, 5) - size(q, backend, 4)
                merged_idx = base + mi_sec + sum(extra_mi.values())

                tag = "naive" if variant == "naive" else "partial-agg"
                print(f"  [{tag:11s}]")
                print(f"    Mat-View   (+ Σ views)        {_gib(mat_view):5.2f} GiB  "
                      f"[Σ views {_gib(views_total):.2f} = "
                      + " + ".join(f"{q} {_gib(v):.2f}" for q, v in view_secs.items())
                      + "]")
                if extra_mi:
                    extra_desc = " + ".join(f"{q} aMI {_gib(v):.2f}"
                                            for q, v in extra_mi.items())
                    print(f"    Merged-Idx (+ MI_B + aMI)     {_gib(merged_idx):5.2f} GiB  "
                          f"[shared MI {_gib(mi_sec):.2f} + {extra_desc}]")
                else:
                    print(f"    Merged-Idx (+ shared MI)      {_gib(merged_idx):5.2f} GiB  "
                          f"[shared MI {_gib(mi_sec):.2f}]")

                # Pair the S6 shared-view line with the naive variant so the
                # comparison against Σ per-query views uses naive views_total
                # (the published Mat-View baseline). preagg numbers are still
                # printed above for reference.
                if has_s6 and variant == "naive":
                    mat_view_shared = base + shared_sec
                    print(f"    Mat-View-Shared (+ 1 view)    "
                          f"{_gib(mat_view_shared):5.2f} GiB  "
                          f"[shared view {_gib(shared_sec):.2f}]")
                    dv = shared_sec - views_total
                    print(f"      S6 shared view vs Σ per-query views: "
                          f"{_gib(shared_sec):.2f} vs {_gib(views_total):.2f}  → S6 "
                          f"{abs(_gib(dv)):.2f} GiB {'larger' if dv > 0 else 'smaller'}")
                    dm = shared_sec - mi_sec
                    print(f"      S6 shared view vs shared MI:         "
                          f"{_gib(shared_sec):.2f} vs {_gib(mi_sec):.2f}  → S6 "
                          f"{abs(_gib(dm)):.2f} GiB {'larger' if dm > 0 else 'smaller'}")
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
