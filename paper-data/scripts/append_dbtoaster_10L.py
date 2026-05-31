#!/usr/bin/env python3
"""Append SF=0.7 and SF=1.0 DBToaster rows to the existing
paper-data/2026-05-24-dbtoaster summary CSVs.

Inputs (raw/ under the dbtoaster tag):
  sf<SF>.log              — harness stdout (RF1/RF2 lines, VmRSS, view rows)
  sf<SF>.time.log         — /usr/bin/time -v stderr (Maximum resident set size)
  sf<SF>.interleaved.csv  — per-iter (elapsed_s, rf1_ops, rf2_ops, pair_ops)
                             from RF_INTERLEAVE=1 harness pass

Output: appends to
  summary/refresh_sales_dbtoaster_interleaved.csv

The plotter (plot_refresh_sales._dbtoaster_pair_us) iterates rows by SF
and picks the largest fitting peak_rss_kb per memory budget, so existing
rows (SF=0.36) stay in place.
"""
from __future__ import annotations

import csv
import re
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path("/users/alicial/leanstore")
TAG_DIR = ROOT / "paper-data" / "2026-05-24-dbtoaster"
RAW = TAG_DIR / "raw"
SUMMARY = TAG_DIR / "summary"
INTERLEAVED_CSV = SUMMARY / "refresh_sales_dbtoaster_interleaved.csv"

SFs = ["0.7", "1.0"]

QUERY_ROWS_RE = re.compile(r"QUERY_1 \(Q3 view\) rows:\s+(\d+)")
RF1_RE = re.compile(r"RF1 \(interleaved\):\s+(\d+)\s+orders\s+/\s+(\d+)\s+lineitems\s+->\s+([\d.]+)\s+orders/s")
RF2_RE = re.compile(r"RF2 \(interleaved\):\s+(\d+)\s+orders\s+/\s+(\d+)\s+lineitems\s+->\s+([\d.]+)\s+orders/s")
PAIR_AGG_RE = re.compile(r"pair orders/s \(aggregate\):\s+([\d.]+)\s+\(=\s+([\d.]+)\s+pairs/s\)")
PEAK_RSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s+(\d+)")


def parse_sf(sf: str):
    log = (RAW / f"sf{sf}.log").read_text(errors="ignore")
    time_log = (RAW / f"sf{sf}.time.log").read_text(errors="ignore")
    iters_csv = RAW / f"sf{sf}.interleaved.csv"

    q1 = QUERY_ROWS_RE.search(log)
    rf1 = RF1_RE.search(log)
    rf2 = RF2_RE.search(log)
    pair = PAIR_AGG_RE.search(log)
    rss = PEAK_RSS_RE.search(time_log)
    if not all([q1, rf1, rf2, pair, rss]):
        missing = [n for n, m in (("query", q1), ("rf1", rf1), ("rf2", rf2),
                                   ("pair", pair), ("rss", rss)) if not m]
        raise SystemExit(f"sf={sf}: could not parse fields: {missing}")

    # pairs_per_s_med = median of pair_orders_per_s column in per-iter CSV.
    # The harness writes pair_orders_per_s = 2.0/(rf1_us+rf2_us)*1e6 per iter;
    # in the summary CSV the existing convention is to record the MEDIAN
    # per-iter pair rate as pairs_per_s_med (the size-stable pair throughput
    # that's robust to a few slow iterations).
    pairs_per_s_med = None
    if iters_csv.exists():
        with iters_csv.open() as f:
            rdr = csv.DictReader(f)
            vals = []
            for row in rdr:
                try:
                    vals.append(float(row["pair_orders_per_s"]))
                except (KeyError, ValueError):
                    continue
        if vals:
            # The per-iter pair_orders_per_s = 2/(rf1_us+rf2_us)*1e6 counts
            # BOTH RF1 and RF2 as "orders" — divide by 2 to get pairs/s,
            # matching the harness's "(= X pairs/s)" line in the log.
            pairs_per_s_med = statistics.median(vals) / 2.0

    if pairs_per_s_med is None:
        # Fall back to the log's printed "(= X pairs/s)" aggregate.
        pairs_per_s_med = float(pair.group(2))

    return {
        "view_rows": int(q1.group(1)),
        "rf1_orders_per_s": float(rf1.group(3)),
        "rf2_orders_per_s": float(rf2.group(3)),
        "pair_orders_per_s_agg": float(pair.group(1)),
        "pairs_per_s_med": pairs_per_s_med,
        "peak_rss_kb": int(rss.group(1)),
    }


def commit_sha():
    return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                          capture_output=True, text=True, check=True).stdout.strip()


def host():
    return subprocess.run(["hostname", "-f"], capture_output=True, text=True,
                          check=True).stdout.strip()


def main():
    sha = commit_sha()
    h = host()

    # Existing CSV columns (must match — order matters).
    with INTERLEAVED_CSV.open() as f:
        header = next(csv.reader(f))
    expected = ["tool","sf","mode","rf1_orders_per_s","rf2_orders_per_s",
                "pair_orders_per_s_agg","pairs_per_s_med","view_rows",
                "peak_rss_kb","run","commit","host","note"]
    if header != expected:
        print(f"WARN: header mismatch:\n  got:  {header}\n  want: {expected}",
              file=sys.stderr)

    rows_added = []
    with INTERLEAVED_CSV.open("a", newline="") as f:
        w = csv.writer(f)
        for sf in SFs:
            try:
                parsed = parse_sf(sf)
            except SystemExit as e:
                print(f"[skip] {e}", file=sys.stderr)
                continue
            row = [
                "dbtoaster", sf, "interleaved",
                f"{parsed['rf1_orders_per_s']}",
                f"{parsed['rf2_orders_per_s']}",
                f"{parsed['pair_orders_per_s_agg']}",
                f"{parsed['pairs_per_s_med']:.1f}",
                parsed["view_rows"],
                parsed["peak_rss_kb"],
                1, sha, h,
                f"10L anchor — refresh_sales SF=4000 btree alignment",
            ]
            w.writerow(row)
            rows_added.append(row)

    if not rows_added:
        print("no rows appended (no parseable raw logs)")
        return
    print(f"appended {len(rows_added)} row(s) to {INTERLEAVED_CSV}:")
    for row in rows_added:
        print("  " + ",".join(map(str, row[:9])) + " ...")


if __name__ == "__main__":
    main()
