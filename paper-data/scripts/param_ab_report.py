#!/usr/bin/env python3
"""Report the substitution-parameter A/B.

A = param_seed=0  (the validation default parameter — the historical
                   single-parameter point every prior run used)
B = param_seed=K  (a rotated parameter, same image)

Both arms run against the SAME loaded image (one reload, two query runs per
structure — see experiments/run_param_ab.sh), so this is a same-image A/B in
the sense of feedback_perf_fixes_ab_same_image. It answers the reviewer
concern that the headline numbers rest on a single substitution parameter:
if the *ranking* of structures (which approach wins) is preserved from A to
B, the single-parameter headline is robust; the per-structure B/A ratio
quantifies how much the absolute latency moves with the parameter.

Reads the per-structure A/B logs (structure<N>_A.log / structure<N>_B.log)
emitted by run_param_ab.sh, parses each run's "Summary: ... ms/query" line,
and prints a per-structure table + the rank order under A vs B.

Usage:
  python3 paper-data/scripts/param_ab_report.py <outdir> \
      [--binary q3_lsm] [--seed-b 7] [--structures 1,2,3,4]
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

# tpch_executable_helper.hpp prints e.g.
#   "  Summary: 12.34 TX/s  |  81.04 ms/query  |  3 queries in 0.2s  |  ..."
SUMMARY_RE = re.compile(r"Summary:\s*([\d.]+)\s*TX/s\s*\|\s*([\d.]+)\s*ms/query")
LABELS = {1: "S1 base-merge", 2: "S2 mat-view", 3: "S3 merged-idx",
          4: "S4 base-hash", 5: "S5 preagg"}


def parse_ms(log: Path) -> float | None:
    """Return the ms/query from the last Summary line in a structure log."""
    if not log.exists():
        return None
    ms = None
    for line in log.read_text(errors="replace").splitlines():
        m = SUMMARY_RE.search(line)
        if m:
            ms = float(m.group(2))  # last Summary wins (one per run)
    return ms


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("outdir", help="dir with structure<N>_{A,B}.log")
    ap.add_argument("--binary", default="")
    ap.add_argument("--seed-b", default="?")
    ap.add_argument("--structures", default="1,2,3,4")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    structs = [int(s) for s in args.structures.split(",") if s.strip()]
    rows = [(s, parse_ms(outdir / f"structure{s}_A.log"),
                parse_ms(outdir / f"structure{s}_B.log")) for s in structs]

    print(f"# Param-rotation A/B   binary={args.binary or '?'}")
    print(f"#   A = param_seed=0 (validation default)")
    print(f"#   B = param_seed={args.seed_b} (rotated, same image)")
    print(f"#   lower ms/query is better\n")
    print(f"{'structure':<16}{'A ms (default)':>16}{'B ms (rotated)':>16}{'B/A':>9}")
    for s, a, b in rows:
        lbl = LABELS.get(s, f"S{s}")
        astr = f"{a:.1f}" if a is not None else "—"
        bstr = f"{b:.1f}" if b is not None else "—"
        rstr = f"{b / a:.3f}" if (a and b) else "—"
        print(f"{lbl:<16}{astr:>16}{bstr:>16}{rstr:>9}")

    # Rank order (fastest→slowest by ms), per arm, over structures with data.
    def order(idx: int) -> list[int]:
        vals = [(s, (a, b)[idx]) for s, a, b in rows if (a, b)[idx] is not None]
        return [s for s, _ in sorted(vals, key=lambda t: t[1])]

    oa, ob = order(0), order(1)
    print("\nrank (fastest -> slowest):")
    print(f"  A (default): {' < '.join('S' + str(s) for s in oa) or '—'}")
    print(f"  B (rotated): {' < '.join('S' + str(s) for s in ob) or '—'}")
    if oa and ob:
        print(f"  ranking preserved across parameters: "
              f"{'YES' if oa == ob else 'NO — see B/A column'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
