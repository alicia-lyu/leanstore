#!/usr/bin/env python3
"""Stage 10L sweep (cell c4) into paper-data/2026-05-29-rep0-10L/raw/.

Mirrors stage_from_raw.py but with 10L SFs and the c4 cell tag in
the directory layout. Walks
paper-data/2026-05-29-rep0-10L/raw/<binary>/c4-bg2-r<rep>/structure<sx>.log
and writes TPut.s<sx>.csv per cell. Updates manifest.yaml in-place.
"""
import csv
import re
import subprocess
from pathlib import Path

TAG_DIR = Path("/users/alicial/leanstore/paper-data/2026-05-29-rep0-10L")
REP_RE = re.compile(r"c4-bg2-r(\d+)")
STRUCT_RE = re.compile(r"structure(\d+)\.log")
SCALES = {"lsm": "10000", "btree": "4000"}
DRAM = "1.0"

RUNNING_QUERY_RE = re.compile(r"Running query on (\S+) for")
SUMMARY_RE = re.compile(
    r"Summary:\s+([\d.]+)\s+TX/s\s+\|\s+([\d.]+)\s+ms/query\s+\|\s+(\d+)\s+queries.*?\|\s+([\d.]+)\s+MiB"
)
BG_COHORT_RE = re.compile(r"bg_cohort\s*\([^)]*\):\s+(\d+)\s+TXs")
BG_LOOKUP_RE = re.compile(r"bg_lookup\s*\([^)]*\):\s+(\d+)\s+TXs")
SIZE_AUDIT_RE = re.compile(
    r"size_audit\s+\S+\s+sx=\d+\s+per_adapter=([\d.]+)\s+MiB\s+image_disk=([\d.]+)\s+MiB\s+delta=([+-][\d.]+)"
)

FIELDS = ["method", "tx", "DRAM (GiB)", "scale", "tentative_skip_bytes",
          "bgw_pct", "TPut (TX/s)", "TX count", "ms/query",
          "bg_cohort TXs", "bg_lookup TXs", "size (MiB)",
          "audit per_adapter (MiB)", "audit image_disk (MiB)", "audit delta %"]


def parse_log(path: Path):
    text = path.read_text(errors="ignore")
    m = RUNNING_QUERY_RE.search(text)
    method = m.group(1) if m else None
    s = SUMMARY_RE.search(text)
    if not s or method is None:
        return None
    ms = float(s.group(2))
    out = {
        "method": method,
        "tx_per_s": (1000.0 / ms) if ms > 0 else None,
        "ms_per_tx": ms,
        "tx_count": int(s.group(3)),
        "size_mib": float(s.group(4)),
    }
    b = BG_COHORT_RE.search(text); out["bg_cohort_tx"] = int(b.group(1)) if b else None
    bl = BG_LOOKUP_RE.search(text); out["bg_lookup_tx"] = int(bl.group(1)) if bl else None
    a = SIZE_AUDIT_RE.search(text)
    if a:
        out["audit_per_adapter_mib"] = float(a.group(1))
        out["audit_image_disk_mib"] = float(a.group(2))
        out["audit_delta_pct"] = float(a.group(3))
    else:
        out["audit_per_adapter_mib"] = out["audit_image_disk_mib"] = out["audit_delta_pct"] = None
    return out


def main():
    rep_counts = {}
    for binary_dir in sorted((TAG_DIR / "raw").iterdir()):
        binary = binary_dir.name
        backend = "btree" if binary.endswith("_btree") else "lsm"
        scale = SCALES[backend]
        for rep_dir in sorted(binary_dir.iterdir()):
            m = REP_RE.match(rep_dir.name)
            if not m:
                continue
            rep = int(m.group(1))
            for log in sorted(rep_dir.glob("structure*.log")):
                sm = STRUCT_RE.match(log.name)
                if not sm:
                    continue
                sx = int(sm.group(1))
                parsed = parse_log(log)
                if parsed is None:
                    continue
                row = {
                    "method": parsed["method"], "tx": "query",
                    "DRAM (GiB)": DRAM, "scale": scale,
                    "tentative_skip_bytes": "0", "bgw_pct": "0",
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
                out_csv = rep_dir / f"TPut.s{sx}.csv"
                with out_csv.open("w", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=FIELDS)
                    w.writeheader()
                    w.writerow(row)
                rep_counts.setdefault(rep, {}).setdefault(binary, []).append(sx)

    total_runs = sum(sum(len(v) for v in rd.values()) for rd in rep_counts.values())
    reps_seen = sorted(rep_counts.keys())
    manifest = TAG_DIR / "manifest.yaml"
    commit_sha = subprocess.run(
        ["git", "-C", str(TAG_DIR.parent.parent), "rev-parse", "--short", "HEAD"],
        capture_output=True, text=True, check=True).stdout.strip()
    text = manifest.read_text() if manifest.exists() else ""
    lines = text.splitlines()
    out_lines = []
    for line in lines:
        if line.startswith("commit_sha:"):
            out_lines.append(f"commit_sha: {commit_sha}")
        elif line.startswith("reps:"):
            out_lines.append(f"reps: {len(reps_seen)}  # {reps_seen}")
        elif line.startswith("runs_ok:"):
            out_lines.append(f"runs_ok: {total_runs}")
        else:
            out_lines.append(line)
    if not text or "commit_sha:" not in text:
        out_lines.append(f"commit_sha: {commit_sha}")
        out_lines.append(f"reps: {len(reps_seen)}  # {reps_seen}")
        out_lines.append(f"runs_ok: {total_runs}")
    manifest.write_text("\n".join(out_lines) + "\n")

    for rep in reps_seen:
        cells = sum(len(v) for v in rep_counts[rep].values())
        print(f"rep {rep}: {cells} cells staged")
    print(f"manifest updated: commit_sha={commit_sha}, runs_ok={total_runs}")


if __name__ == "__main__":
    main()
