#!/usr/bin/env python3
"""Reproduce the paper's space-footprint table (``tab:exp-baselines``)
from a sweep's ``headline.csv`` — so the numbers are regenerable, not
hand-pasted.

Method (the one used in the paper / re-derived in the audit):
  * Per-query *secondary* size = ``size(Sx) - size(S4)`` — S4 (Base-Hash)
    stores only the base tables, so subtracting it isolates the extra
    structure each approach adds.
  * Cross-query *aggregate* per family counts each **shared** structure
    once: Base-Merge's custkey-sorted secondaries and Merged-Idx's MI are
    shared across the family (one image serves both queries), so their
    total is the single shared structure on top of the base. Mat-View is
    **not** shared — it stores one view per query, so its total sums the
    per-query view secondaries.

Families (each lives in its own image):
  * vanilla  = {q3, q5}   sharing MI_B  (COL MI)
  * invoice  = {q3i, q5i} sharing MI_C  (COLI MI)

Usage:
  python3 paper-data/scripts/space_table.py [--tag 2026-05-24-a-ssd]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PAPER_DATA = REPO_ROOT / "paper-data"

# Representative query first — it supplies the base (S4) and the shared
# Base-Merge / Merged-Idx structures for the family aggregate.
FAMILIES = {
    "vanilla (MI_B)": ["q3", "q5"],
    "invoice (MI_C)": ["q3i", "q5i"],
}
MIB = 1024.0  # MiB per GiB


def _sizes(tag: str) -> pd.DataFrame:
    df = pd.read_csv(PAPER_DATA / tag / "summary" / "headline.csv")
    df = df[(df["cell"] == "c0") & (df["bg"] == 2) & (df["tx"] == "query")]
    # One size per (query, backend, structure) — sizes are rep-invariant.
    return (df.groupby(["query", "backend", "structure"])["size_mib"]
              .median().reset_index())


def _gib(mib: float) -> float:
    return mib / MIB


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", default="2026-05-24-a-ssd")
    args = ap.parse_args()

    sizes = _sizes(args.tag)

    def size(query: str, backend: str, struct: int) -> float:
        row = sizes[(sizes["query"] == query) & (sizes["backend"] == backend)
                    & (sizes["structure"] == struct)]
        return float(row["size_mib"].iloc[0]) if not row.empty else float("nan")

    print(f"# Space footprint from tag {args.tag} (c0/bg=2)\n")
    for fam, queries in FAMILIES.items():
        rep = queries[0]
        for backend in ("btree", "lsm"):
            base = size(rep, backend, 4)                  # S4 = base tables only
            if base != base:                              # NaN → family/backend absent
                continue
            split_sec = size(rep, backend, 1) - base      # shared (S1 - S4)
            mi_sec = size(rep, backend, 3) - base          # shared (S3 - S4)
            view_secs = {q: size(q, backend, 2) - size(q, backend, 4)
                         for q in queries}
            views_total = sum(view_secs.values())

            base_hash = base
            base_merge = base + split_sec                  # = size(rep, S1)
            merged_idx = base + mi_sec                     # = size(rep, S3)
            mat_view = base + views_total                  # base + Σ per-query views

            print(f"## {fam} — {backend}   (family: {', '.join(queries)})")
            print(f"  Base-Hash  (base only)         {_gib(base_hash):5.2f} GiB")
            print(f"  Base-Merge (base + shared sec) {_gib(base_merge):5.2f} GiB  "
                  f"[shared split secondary {_gib(split_sec):.2f}]")
            print(f"  Mat-View   (base + per-q views){_gib(mat_view):5.2f} GiB  "
                  f"[Σ views {_gib(views_total):.2f} = "
                  + " + ".join(f"{q} {_gib(v):.2f}" for q, v in view_secs.items())
                  + "]")
            print(f"  Merged-Idx (base + shared MI)  {_gib(merged_idx):5.2f} GiB  "
                  f"[shared MI {_gib(mi_sec):.2f}]")
            # Per-query MI-vs-view comparison (the LSM per-query inversion).
            for q, v in view_secs.items():
                delta = mi_sec - v
                rel = "larger" if delta > 0 else "smaller"
                print(f"    MI vs {q} view: MI {_gib(mi_sec):.2f} vs view "
                      f"{_gib(v):.2f}  → MI {abs(_gib(delta)):.2f} GiB {rel}")
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
