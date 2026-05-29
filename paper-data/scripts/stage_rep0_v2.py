#!/usr/bin/env python3
"""Stage rep 0 (and the S2 v2 rerun) into paper-data/2026-05-28-rep0-5L/raw/.

Bypasses the umbrella build/<binary>/TPut.csv whose header is stale relative
to the current bg=2 5-worker cohort runs. Instead, parses each cell's
`structure<N>.log` directly: pulls the method from "Running query on <m>",
the headline numbers from the Summary line, and (optionally) the bg_cohort /
bg_lookup TX counts. Writes a clean per-cell TPut.s<N>.csv that the analyzer
can ingest.

Re-running is idempotent: existing TPut.s<N>.csv files get overwritten with
the latest log content. Safe to invoke mid-sweep — incomplete cells produce
empty / partial rows that the analyzer tolerates.
"""
import csv
import re
import shutil
import subprocess
from pathlib import Path

REPO = Path("/users/alicial/leanstore")
TAG = "2026-05-28-rep0-5L"
TAG_DIR = REPO / "paper-data" / TAG

BTREE_SCALE = "1550"
LSM_SCALE = "3850"
DRAM = "1.0"

BINARIES = [
    ("q3_btree",  BTREE_SCALE, [1, 2, 3, 4]),
    ("q5_btree",  BTREE_SCALE, [1, 2, 3, 4]),
    ("q10_btree", BTREE_SCALE, [1, 2, 3, 4, 5, 7]),
    ("q3i_btree", BTREE_SCALE, [1, 2, 3, 4]),
    ("q5i_btree", BTREE_SCALE, [1, 2, 3, 4]),
    ("q10i_btree", BTREE_SCALE, [1, 2, 3, 4, 5, 7]),
    ("q3_lsm",  LSM_SCALE, [1, 2, 3, 4]),
    ("q5_lsm",  LSM_SCALE, [1, 2, 3, 4]),
    ("q10_lsm", LSM_SCALE, [1, 2, 3, 4, 5, 7]),
    ("q3i_lsm", LSM_SCALE, [1, 2, 3, 4]),
    ("q5i_lsm", LSM_SCALE, [1, 2, 3, 4]),
    ("q10i_lsm", LSM_SCALE, [1, 2, 3, 4, 5, 7]),
]

RUNNING_QUERY_RE = re.compile(r"Running query on (\S+) for")
SUMMARY_RE = re.compile(
    r"Summary:\s+([\d.]+)\s+TX/s\s+\|\s+([\d.]+)\s+ms/query\s+\|\s+(\d+)\s+queries.*?\|\s+([\d.]+)\s+MiB"
)
BG_COHORT_RE = re.compile(r"bg_cohort\s*\([^)]*\):\s+(\d+)\s+TXs")
BG_LOOKUP_RE = re.compile(r"bg_lookup\s*\([^)]*\):\s+(\d+)\s+TXs")
SIZE_AUDIT_RE = re.compile(
    r"size_audit\s+\S+\s+sx=\d+\s+per_adapter=([\d.]+)\s+MiB\s+image_disk=([\d.]+)\s+MiB\s+delta=([+-][\d.]+)"
)


def parse_log(log_path: Path):
    """Return dict with method, tx_per_s, ms_per_tx, tx_count, size_mib,
    bg_cohort_tx, bg_lookup_tx, image_disk_mib, audit_delta_pct.
    Missing fields are None."""
    if not log_path.exists():
        return None
    text = log_path.read_text(errors="ignore")
    out = {}
    m = RUNNING_QUERY_RE.search(text)
    out["method"] = m.group(1) if m else None
    s = SUMMARY_RE.search(text)
    if s:
        ms_per_tx = float(s.group(2))
        # The Summary line prints TX/s at 2 dp (0.04 for cells slower than
        # 25 ms/q) — recompute from ms_per_tx so downstream analyzer's
        # ms_per_tx = 1000 / tx_per_s keeps full precision.
        out["tx_per_s"] = 1000.0 / ms_per_tx if ms_per_tx > 0 else None
        out["ms_per_tx"] = ms_per_tx
        out["tx_count"] = int(s.group(3))
        out["size_mib"] = float(s.group(4))
    else:
        out["tx_per_s"] = out["ms_per_tx"] = out["tx_count"] = out["size_mib"] = None
    b = BG_COHORT_RE.search(text)
    out["bg_cohort_tx"] = int(b.group(1)) if b else None
    bl = BG_LOOKUP_RE.search(text)
    out["bg_lookup_tx"] = int(bl.group(1)) if bl else None
    a = SIZE_AUDIT_RE.search(text)
    if a:
        out["audit_per_adapter_mib"] = float(a.group(1))
        out["audit_image_disk_mib"] = float(a.group(2))
        out["audit_delta_pct"] = float(a.group(3))
    else:
        out["audit_per_adapter_mib"] = out["audit_image_disk_mib"] = out["audit_delta_pct"] = None
    return out


def stage_binary(binary: str, scale: str, expected_sx: list):
    src_run_dir = REPO / "build" / binary / f"{scale}-in-{DRAM}"
    dst = TAG_DIR / "raw" / binary / "c0-bg2-r0"
    dst.mkdir(parents=True, exist_ok=True)

    ok, missing = [], []
    fields = ["method", "tx", "DRAM (GiB)", "scale", "tentative_skip_bytes",
              "bgw_pct", "TPut (TX/s)", "TX count", "ms/query",
              "bg_cohort TXs", "bg_lookup TXs", "size (MiB)",
              "audit per_adapter (MiB)", "audit image_disk (MiB)", "audit delta %"]

    for sx in expected_sx:
        log_path = src_run_dir / f"structure{sx}.log"
        parsed = parse_log(log_path)
        if parsed is None or parsed["method"] is None or parsed["tx_per_s"] is None:
            missing.append(sx)
            continue

        # Write per-Sx TPut.s<N>.csv
        out_csv = dst / f"TPut.s{sx}.csv"
        row = {
            "method": parsed["method"],
            "tx": "query",
            "DRAM (GiB)": DRAM,
            "scale": scale,
            "tentative_skip_bytes": "0",
            "bgw_pct": "0",
            "TPut (TX/s)": parsed["tx_per_s"],
            "TX count": parsed["tx_count"],
            "ms/query": parsed["ms_per_tx"],
            "bg_cohort TXs": parsed["bg_cohort_tx"] if parsed["bg_cohort_tx"] is not None else "",
            "bg_lookup TXs": parsed["bg_lookup_tx"] if parsed["bg_lookup_tx"] is not None else "",
            "size (MiB)": parsed["size_mib"],
            "audit per_adapter (MiB)": parsed["audit_per_adapter_mib"] if parsed["audit_per_adapter_mib"] is not None else "",
            "audit image_disk (MiB)": parsed["audit_image_disk_mib"] if parsed["audit_image_disk_mib"] is not None else "",
            "audit delta %": parsed["audit_delta_pct"] if parsed["audit_delta_pct"] is not None else "",
        }
        with out_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerow(row)

        # Copy the log + stderr for traceability
        shutil.copy2(log_path, dst / f"structure{sx}.log")
        stderr_path = src_run_dir / f"structure{sx}_stderr.txt"
        if stderr_path.exists():
            shutil.copy2(stderr_path, dst / f"structure{sx}_stderr.txt")
        ok.append(sx)

    # Copy sstables.csv if present (LSM only)
    sstables = src_run_dir / "sstables.csv"
    if sstables.exists():
        shutil.copy2(sstables, dst / "sstables.csv")

    return {"ok": ok, "missing": missing}


def main():
    TAG_DIR.mkdir(parents=True, exist_ok=True)
    commit_sha = subprocess.run(
        ["git", "-C", str(REPO), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True).stdout.strip()
    host = subprocess.run(
        ["hostname", "-f"], capture_output=True, text=True, check=True
    ).stdout.strip()
    total_ok = 0
    total_missing = 0
    for binary, scale, sx_list in BINARIES:
        res = stage_binary(binary, scale, sx_list)
        print(f"{binary}: ok={res['ok']} missing={res['missing']}")
        total_ok += len(res["ok"])
        total_missing += len(res["missing"])

    manifest = TAG_DIR / "manifest.yaml"
    with manifest.open("w") as f:
        f.write(f"""tag: {TAG}
experiment: TPC-H + TPCHi headline sweep, bg=2 cohort, --param_seed=0 (rep 0)
commit_sha: {commit_sha}
host: {host}
cells: [c0]   # 5L: btree SF=1550, lsm SF=3850, dram=1.0
families: [vanilla, tpchi]
reps: 1
runs_ok: {total_ok}
runs_err: {total_missing}
backends: [btree, lsm]
binaries: [q3, q5, q3i, q5i, q10, q10i]
structures:
  q3_q5_q3i_q5i: [1, 2, 3, 4]
  q10_q10i: [1, 2, 3, 4, 5, 7]
bg: 2 (--bg_query_thread=true --bg_point_lookups=true; 3-thread cohort + 1 lookup; worker_threads=5)
disk: ssd
note: >
  Rep 0 of the 5L cell, post LSM-recover-fix (acf78a35) +
  size_audit (c9406c44) + S2 cohort-asymmetry fix:

  - q3/q5/q3i/q5i_btree S=2 cohort: Q10 thread reads PREAGG view
    (1.07 GiB) instead of naive (5.10 GiB). Avoids dragging Q3/Q5
    perf via Q10's join fan-out — pre-fix Q3 btree S2 was 201 s/q,
    post-fix ~28 s/q (matches pre-cohort-refactor baseline
    2026-05-24-a-ssd).
  - q10/q10i_btree S=2 cohort: Q10 thread reads NAIVE (matches
    foreground variant). Cohort represents same-workload contention
    for Q10's own headline.
  - All other Sx (S1, S3, S4, S5, S7) unchanged.

  Headline numbers parsed directly from each cell's structure<N>.log
  Summary line (size_audit / bg_cohort / bg_lookup TX counts also
  captured for diagnostics). Avoids the umbrella TPut.csv whose
  header is stale relative to the bg=2 cohort schema.
""")
    print(f"Wrote {manifest}")
    print(f"Total: ok={total_ok}, missing={total_missing}")


if __name__ == "__main__":
    main()
