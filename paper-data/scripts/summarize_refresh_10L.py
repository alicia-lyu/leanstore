#!/usr/bin/env python3
"""Summarize the 10L refresh sweep (paper-data/2026-05-30-refresh-10L).

Walks raw/<cell>/<be>.s<N>.csv + <be>_s<N>.out for each cell in
{10LL, 10L, 10H, 10HH} and writes per-cell summary CSVs under
summary/refresh_sales_<cell>_throughput.csv (10LL bg=0 prewarm) or
summary/refresh_sales_<cell>_bg2_throughput.csv (others). Schema matches
the 5L sweep summarizers so downstream plotters see the same columns.
"""
from __future__ import annotations

import csv
import re
import subprocess
import sys
from pathlib import Path

TAG_DIR = Path("/users/alicial/leanstore/paper-data/2026-05-30-refresh-10L")
SECS = 90
TAIL_WIN = 30

SF = {"btree": 4000, "lsm": 10000}
CELLS = {
    # cell -> (dram_gib, bg, backends, csv_basename)
    "10LL": (20.0, 0, ["btree"],         "refresh_sales_10LL_throughput.csv"),
    "10L":  (1.0,  2, ["btree", "lsm"],  "refresh_sales_10L_bg2_throughput.csv"),
    "10H":  (0.5,  2, ["btree", "lsm"],  "refresh_sales_10H_bg2_throughput.csv"),
    "10HH": (0.2,  2, ["btree", "lsm"],  "refresh_sales_10HH_bg2_throughput.csv"),
}
STRUCT_LABEL = {1: "split_S1", 2: "view_S2", 3: "merged_S3", 4: "base_S4"}

RF_RE = re.compile(r"RF1=(\d+)\s+RF2=(\d+)")
BG_RE = re.compile(r"bg_query_thread:\s*(\d+)\s*bg TXs")

FIELDS = [
    "binary", "backend", "cell", "scale", "dram_gib", "bg",
    "structure", "struct_label",
    "total_rf1", "total_rf2", "bg_txs",
    "elapsed_s", "n_rows",
    "rf1_tps", "rf2_tps", "pair_tps", f"pair_tps_tail{TAIL_WIN}",
    "commit", "host",
]


def parse_out(path: Path):
    """Return (total_rf1, total_rf2, bg_txs) from a refresh_sales stdout file."""
    if not path.exists():
        return 0, 0, 0
    text = path.read_text(errors="ignore")
    rf = RF_RE.search(text)
    bg = BG_RE.search(text)
    return (int(rf.group(1)) if rf else 0,
            int(rf.group(2)) if rf else 0,
            int(bg.group(1)) if bg else 0)


def parse_iters(path: Path):
    """Return (n_rows, last_elapsed_s, n_in_tail) from a per-iteration CSV.
    Expected schema: elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s
    """
    if not path.exists():
        return 0, 0.0, 0
    n = 0
    last = 0.0
    tail = 0
    with path.open() as f:
        rdr = csv.reader(f)
        next(rdr, None)  # header
        rows = list(rdr)
    if not rows:
        return 0, 0.0, 0
    last = float(rows[-1][0])
    cutoff = last - TAIL_WIN
    for r in rows:
        try:
            t = float(r[0])
        except ValueError:
            continue
        n += 1
        if t >= cutoff:
            tail += 1
    return n, last, tail


def commit_sha():
    return subprocess.run(
        ["git", "-C", str(TAG_DIR.parent.parent), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True).stdout.strip()


def host():
    try:
        return subprocess.run(["hostname", "-f"], capture_output=True, text=True,
                              check=True).stdout.strip()
    except subprocess.CalledProcessError:
        return subprocess.run(["hostname"], capture_output=True, text=True,
                              check=True).stdout.strip()


def main():
    summary_dir = TAG_DIR / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    sha = commit_sha()
    h = host()

    for cell, (dram, bg, backends, basename) in CELLS.items():
        cell_dir = TAG_DIR / "raw" / cell
        if not cell_dir.exists():
            print(f"[skip] {cell}: no raw/{cell}/", file=sys.stderr)
            continue
        out_csv = summary_dir / basename
        with out_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            for be in backends:
                for N in (1, 2, 3, 4):
                    iters_csv = cell_dir / f"{be}.s{N}.csv"
                    out_file = cell_dir / f"{be}_s{N}.out"
                    if not iters_csv.exists():
                        continue
                    t1, t2, bg_tx = parse_out(out_file)
                    n_rows, elapsed, tail = parse_iters(iters_csv)
                    if elapsed <= 0:
                        elapsed = SECS
                    rf1_tps = t1 / elapsed if elapsed > 0 else 0.0
                    rf2_tps = t2 / elapsed if elapsed > 0 else 0.0
                    pair_tps = n_rows / elapsed if elapsed > 0 else 0.0
                    pair_tail = tail / TAIL_WIN if TAIL_WIN > 0 else 0.0
                    w.writerow({
                        "binary": f"refresh_sales_{be}",
                        "backend": be,
                        "cell": cell,
                        "scale": SF[be],
                        "dram_gib": dram,
                        "bg": bg,
                        "structure": N,
                        "struct_label": STRUCT_LABEL[N],
                        "total_rf1": t1,
                        "total_rf2": t2,
                        "bg_txs": bg_tx,
                        "elapsed_s": f"{elapsed:.2f}",
                        "n_rows": n_rows,
                        "rf1_tps": f"{rf1_tps:.1f}",
                        "rf2_tps": f"{rf2_tps:.1f}",
                        "pair_tps": f"{pair_tps:.1f}",
                        f"pair_tps_tail{TAIL_WIN}": f"{pair_tail:.1f}",
                        "commit": sha,
                        "host": h,
                    })
        print(f"wrote {out_csv}")


if __name__ == "__main__":
    main()
