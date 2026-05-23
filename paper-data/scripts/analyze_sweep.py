#!/usr/bin/env python3
"""Sweep analyzer for LeanStore perf runs.

Given one or more csv_runtime directories — each corresponding to a
(query, SF, DRAM) cell of the paper sweep — joins the high-level
TPut.csv with the five detail CSVs (bm, cpu, latency, cr, dt) and
emits a flat per-(cell, method, tx) summary suitable for spreadsheet
ingestion or `column -t` inspection.

Layout assumption (matches frontend/shared/logger/logger.cpp):

    <csv_db>/TPut.csv                          # one row per (method, tx)
    <csv_runtime = csv_db/<scale>-in-<dram>>/
        <tx>/<method>/bm.csv                   # one row per run, c_hash
        <tx>/<method>/cpu.csv
        <tx>/<method>/latency.csv
        <tx>/<method>/cr.csv
        <tx>/<method>/dt.csv

The script takes the *last* row from each detail CSV (most recent
invocation) and joins it with the last TPut.csv row that matches
(method, tx). This matches the paper-sweep workflow where each run
appends one row per phase to each CSV.

The S3/S2 inversion check: for each (cell, tx) pair where both
storage_structure=2 (mat_view → method='mat_view') and
storage_structure=3 (merged_idx → method='merged_idx') ran, compute
the throughput delta. If S3 < S2 by more than --inversion-threshold
(default 5%), flag the row and emit a side-by-side BM/CPU column
view so the inversion is attributable to a specific counter.

Usage:
    scripts/analyze_sweep.py build/q3i_lsm/15-in-0.1 build/q12_lsm/15-in-0.1
    scripts/analyze_sweep.py --inversion-threshold 0.05 \
        build/q3i_lsm/*-in-*

The script is pure stdlib (no pandas) so it runs anywhere the build
host can run Python 3.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional


DETAIL_TABLES = ("bm", "cpu", "latency", "cr", "dt")


def read_last_row(path: Path) -> Optional[Dict[str, str]]:
    """Return the last row of a CSV as a dict, or None if file is empty/missing."""
    if not path.exists() or path.stat().st_size == 0:
        return None
    with path.open() as f:
        reader = csv.DictReader(f)
        last = None
        for row in reader:
            last = row
        return last


def read_all_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open() as f:
        reader = csv.DictReader(f)
        return list(reader)


def collect_cell(csv_runtime: Path) -> List[Dict[str, str]]:
    """Build one flat row per (method, tx) under this csv_runtime dir.

    Returns a list of merged-column dicts, one per (method, tx) row in
    TPut.csv. Each dict carries the TPut columns (prefixed `tput_`),
    plus the detail-table columns prefixed by table name (`bm_`,
    `cpu_`, etc.).
    """
    csv_db = csv_runtime.parent
    tput_path = csv_db / "TPut.csv"
    rows: List[Dict[str, str]] = []
    if not tput_path.exists():
        print(f"[warn] no TPut.csv under {csv_db}", file=sys.stderr)
        return rows
    # Group TPut rows by (method, tx) and take the most recent per group.
    grouped: "OrderedDict[tuple, Dict[str, str]]" = OrderedDict()
    for trow in read_all_rows(tput_path):
        key = (trow.get("method", ""), trow.get("tx", ""))
        grouped[key] = trow
    for (method, tx), trow in grouped.items():
        merged: Dict[str, str] = {"cell": str(csv_runtime), "method": method, "tx": tx}
        for k, v in trow.items():
            merged[f"tput_{k}"] = v
        for table in DETAIL_TABLES:
            detail_path = csv_runtime / tx / method / f"{table}.csv"
            row = read_last_row(detail_path)
            if row is None:
                continue
            for k, v in row.items():
                merged[f"{table}_{k}"] = v
        rows.append(merged)
    return rows


# Columns we surface in the headline summary. The full union is also
# emitted at the end for cells that have richer data.
HEADLINE_COLUMNS = (
    "cell",
    "tput_method",
    "tput_tx",
    "tput_scale",
    "tput_DRAM (GiB)",
    "tput_TPut (TX/s)",
    "tput_size (MiB)",
    # CPU
    "cpu_workers Cycles / TX",
    "cpu_workers Instructions / TX",
    "cpu_workers L1-misses / TX",
    "cpu_workers LLC-misses / TX",
    "cpu_workers branch-misses / TX",
    "cpu_workers CPU Util (%)",
    # BM (buffer manager) — free/eviction pressure
    "bm_space_usage_gib",
    "bm_consumed_pages",
    "bm_p1_pct",
    "bm_p2_pct",
    "bm_p3_pct",
    "bm_free_pct",
    "bm_evicted_mib",
    "bm_rounds",
    # CR (concurrency / recovery) — restart and GC behavior
    "cr_committed",
    "cr_restarts",
    "cr_gct_committed",
    "cr_wal_write_gib",
    # DT (data structure)
    "dt_struct_split",
    "dt_misses_counter",
    # Latency
    "latency_TXT P99",
    "latency_TXT P50",
)


def emit_csv(rows: List[Dict[str, str]], path: Optional[Path]):
    if not rows:
        return
    # Build the column union but order by HEADLINE_COLUMNS first, then
    # the rest alphabetically. Missing-column lookups return ''.
    all_cols = set()
    for r in rows:
        all_cols.update(r.keys())
    ordered: List[str] = []
    for c in HEADLINE_COLUMNS:
        if c in all_cols:
            ordered.append(c)
            all_cols.discard(c)
    ordered.extend(sorted(all_cols))
    out = path.open("w") if path else sys.stdout
    try:
        writer = csv.DictWriter(out, fieldnames=ordered, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    finally:
        if path:
            out.close()


def s3_vs_s2_inversions(rows: List[Dict[str, str]], threshold: float):
    """Flag (cell, tx) pairs where merged_idx < mat_view by more than threshold.

    Threshold is a fraction (0.05 == 5%). Emits to stderr a small
    side-by-side BM/CPU view so the user can attribute each inversion.
    """
    by_cell_tx: Dict[tuple, Dict[str, Dict[str, str]]] = {}
    for r in rows:
        cell = r.get("cell", "")
        tx = r.get("tput_tx", "")
        method = r.get("tput_method", "")
        by_cell_tx.setdefault((cell, tx), {})[method] = r
    print("# S3 vs S2 inversion report (S3 = merged_idx, S2 = mat_view)", file=sys.stderr)
    print(f"# threshold = {threshold:.1%} (rows where S3/S2 < 1 - threshold are flagged)", file=sys.stderr)
    flagged = 0
    for (cell, tx), methods in by_cell_tx.items():
        s2 = methods.get("mat_view")
        s3 = methods.get("merged_idx")
        if not (s2 and s3):
            continue
        try:
            t2 = float(s2.get("tput_TPut (TX/s)", "0") or 0)
            t3 = float(s3.get("tput_TPut (TX/s)", "0") or 0)
        except ValueError:
            continue
        if t2 <= 0 or t3 <= 0:
            continue
        ratio = t3 / t2
        # Always print summary line; mark flagged rows with "FLAG".
        tag = "FLAG" if ratio < (1 - threshold) else "ok  "
        print(f"  [{tag}] {cell}  tx={tx}  S3/S2={ratio:.3f}  S2={t2:.1f} TX/s  S3={t3:.1f} TX/s",
              file=sys.stderr)
        if tag == "FLAG":
            flagged += 1
            # Side-by-side attribution columns
            attr_cols = (
                "cpu_workers Cycles / TX",
                "cpu_workers LLC-misses / TX",
                "bm_free_pct",
                "bm_p2_pct",
                "bm_evicted_mib",
                "cr_restarts",
                "cr_gct_committed",
            )
            for c in attr_cols:
                v2 = s2.get(c, "")
                v3 = s3.get(c, "")
                if v2 == "" and v3 == "":
                    continue
                print(f"         {c:40s}  S2={v2:>14s}  S3={v3:>14s}", file=sys.stderr)
    print(f"# inversions flagged: {flagged}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cells", nargs="+", type=Path,
                    help="csv_runtime directories (e.g. build/q3i_lsm/15-in-0.1)")
    ap.add_argument("--out", type=Path, default=None,
                    help="output CSV path (default: stdout)")
    ap.add_argument("--inversion-threshold", type=float, default=0.05,
                    help="fraction below which S3 < S2 counts as an inversion (default 0.05 = 5%%)")
    args = ap.parse_args()

    rows: List[Dict[str, str]] = []
    for cell in args.cells:
        if not cell.is_dir():
            print(f"[warn] not a directory: {cell}", file=sys.stderr)
            continue
        rows.extend(collect_cell(cell))

    if not rows:
        print("no rows collected", file=sys.stderr)
        sys.exit(1)

    emit_csv(rows, args.out)
    s3_vs_s2_inversions(rows, args.inversion_threshold)


if __name__ == "__main__":
    main()
