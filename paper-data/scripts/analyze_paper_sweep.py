#!/usr/bin/env python3
"""Paper-sweep analyzer.

Walks paper-data/<tag>/raw/<binary>/<cell>-bg<bg>-r<rep>/ snapshots emitted
by experiments/run_paper_sweep.sh and produces the four summary CSVs that
PAPER_SWEEP.md §Output format pins:

    summary/headline.csv      one row per (binary,cell,structure,bg,rep,method,tx)
    summary/diagnostics.csv   one row per same key, with attribution columns
    summary/inversions.csv    one row per (binary,cell,bg,rep,tx) where S3 < S2 by >threshold
    summary/stats.csv         one row per (binary,cell,structure,bg) with median+IQR

Each per-rep snapshot directory carries:
    TPut.csv                 high-level (one row per (method,tx) appended per
                             structure run)
    TPut.s<N>.csv            slice written by the runner: rows from
                             `make <binary>_<N>` only (header preserved)
    size.csv / size.s<N>.csv similar
    structure<N>_stderr.txt  binary stderr; carries bg_query_thread count
                             when --bg_query_thread=true
    <tx>/<method>/{bm,cpu,cr,dt,latency}.csv  detail CSVs

Diagnostics columns are best-effort: blank when the source CSV is missing
or empty. inversions.csv is empty if no flag fires (expected outcome).

This script is pure stdlib so it runs anywhere Python 3 is available.
"""

from __future__ import annotations

import argparse
import csv
import re
import statistics
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


RUN_RE = re.compile(r"^(?P<cell>c\d+)-bg(?P<bg>\d+)-r(?P<rep>\d+)$")

# (family, backend, query_label)
FAMILY_OF: Dict[str, Tuple[str, str, str]] = {
    "q3_lsm":    ("vanilla", "lsm",   "q3"),
    "q3_btree":  ("vanilla", "btree", "q3"),
    "q5_lsm":    ("vanilla", "lsm",   "q5"),
    "q5_btree":  ("vanilla", "btree", "q5"),
    "q3i_lsm":   ("tpchi",   "lsm",   "q3i"),
    "q3i_btree": ("tpchi",   "btree", "q3i"),
    "q5i_lsm":   ("tpchi",   "lsm",   "q5i"),
    "q5i_btree": ("tpchi",   "btree", "q5i"),
    "geo_lsm":   ("geo",     "lsm",   "geo"),
    "geo_btree": ("geo",     "btree", "geo"),
}

CELL_DRAM = {"c0": 1.0, "c1": 0.4, "c2": 0.1, "c3": 0.4}
SF_TPCH = {
    "lsm":   {"c0": 3850, "c1": 1500, "c2": 380,  "c3": 3850},
    "btree": {"c0": 1550, "c1": 620,  "c2": 150,  "c3": 1550},
}
SF_GEO = {
    "geo_lsm":   {"c0": 515, "c1": 206, "c2": 52, "c3": 515},
    "geo_btree": {"c0": 194, "c1": 78,  "c2": 19, "c3": 194},
}

# Header-name aliases. The TPut.csv writer emits canonical names; older
# detail CSVs may use slightly different ones. Map them to our column names.
TPUT_TPS_KEY = "TPut (TX/s)"
TPUT_TX_COUNT_KEY = "TX count"  # may or may not exist; tolerate absence
TPUT_SIZE_KEY = "size (MiB)"

BG_RE = re.compile(r"bg_query_thread:\s*(\d+)")
ROCKS_BLOCK_READ_RE = re.compile(r"rocksdb\.block\.read\.count(?:\s*COUNT)?\s*[:=]?\s*(\d+)", re.I)
ROCKS_CACHE_HIT_RE = re.compile(r"rocksdb\.block\.cache\.hit(?:\s*COUNT)?\s*[:=]?\s*(\d+)", re.I)
ROCKS_CACHE_MISS_RE = re.compile(r"rocksdb\.block\.cache\.miss(?:\s*COUNT)?\s*[:=]?\s*(\d+)", re.I)


def sf_for(binary: str, cell: str) -> int:
    if binary in SF_GEO:
        return SF_GEO[binary][cell]
    backend = "lsm" if binary.endswith("_lsm") else "btree"
    return SF_TPCH[backend][cell]


def read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        with path.open() as f:
            return list(csv.DictReader(f))
    except (OSError, csv.Error):
        return []


def read_last_row(path: Path) -> Optional[Dict[str, str]]:
    rows = read_rows(path)
    return rows[-1] if rows else None


def parse_stderr_counts(path: Path) -> Dict[str, Optional[int]]:
    out: Dict[str, Optional[int]] = {
        "bg_tx_count": None,
        "lsm_block_read_count": None,
        "lsm_block_cache_hit": None,
        "lsm_block_cache_miss": None,
    }
    if not path.exists():
        return out
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return out
    m = BG_RE.search(text)
    if m:
        out["bg_tx_count"] = int(m.group(1))
    for key, regex in (
        ("lsm_block_read_count", ROCKS_BLOCK_READ_RE),
        ("lsm_block_cache_hit", ROCKS_CACHE_HIT_RE),
        ("lsm_block_cache_miss", ROCKS_CACHE_MISS_RE),
    ):
        m = regex.search(text)
        if m:
            out[key] = int(m.group(1))
    return out


def to_float(v: Optional[str]) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def to_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def iqr(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return 0.0 if vals else None
    vals = sorted(vals)
    n = len(vals)
    def quantile(p):
        idx = (n - 1) * p
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return vals[lo] + (vals[hi] - vals[lo]) * frac
    return quantile(0.75) - quantile(0.25)


def median(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


# ----- per-rep collection -----

HEADLINE_FIELDS = [
    "binary", "family", "backend", "query", "cell", "sf", "dram_gib",
    "structure", "method", "tx", "bg", "rep",
    "tx_per_s", "tx_count", "bg_tx_count", "ms_per_tx", "size_mib",
    "commit_sha",
]

DIAG_FIELDS = HEADLINE_FIELDS + [
    "cpu_cycles_per_tx", "cpu_instr_per_tx", "cpu_l1_miss_per_tx",
    "cpu_llc_miss_per_tx", "cpu_branch_miss_per_tx", "cpu_util_pct",
    "bm_free_pct", "bm_consumed_pages", "bm_p1_pct", "bm_p2_pct", "bm_p3_pct",
    "bm_evicted_mib", "bm_rounds",
    "cr_committed", "cr_restarts", "cr_gct_committed", "cr_wal_write_gib",
    "dt_struct_split", "dt_misses_counter",
    "latency_p50_ms", "latency_p99_ms",
    "lsm_block_read_count", "lsm_block_cache_hit", "lsm_block_cache_miss",
]


def collect_rep(binary: str, cell: str, bg: int, rep: int, run_dir: Path,
                commit_sha: str) -> List[Dict[str, object]]:
    family, backend, query = FAMILY_OF[binary]
    sf = sf_for(binary, cell)
    dram_gib = CELL_DRAM[cell]
    rows: List[Dict[str, object]] = []

    # The runner emits TPut.s<N>.csv slices per structure; iterate those.
    slice_files = sorted(run_dir.glob("TPut.s*.csv"))
    if not slice_files:
        # Fall back to whole TPut.csv with no structure attribution
        tput_rows = read_rows(run_dir / "TPut.csv")
        for trow in tput_rows:
            rows.append(_build_row(binary, family, backend, query, cell, sf,
                                   dram_gib, None, bg, rep, trow, run_dir, commit_sha))
        return rows

    for path in slice_files:
        m = re.match(r"TPut\.s(\d+)\.csv", path.name)
        if not m:
            continue
        structure = int(m.group(1))
        stderr_path = run_dir / f"structure{structure}_stderr.txt"
        stderr_counts = parse_stderr_counts(stderr_path)
        for trow in read_rows(path):
            rows.append(_build_row(binary, family, backend, query, cell, sf,
                                   dram_gib, structure, bg, rep, trow, run_dir,
                                   commit_sha, stderr_counts))
    return rows


def _build_row(binary, family, backend, query, cell, sf, dram_gib, structure,
               bg, rep, trow, run_dir: Path, commit_sha: str,
               stderr_counts: Optional[Dict] = None) -> Dict[str, object]:
    method = trow.get("method", "")
    tx = trow.get("tx", "")
    tx_per_s = to_float(trow.get(TPUT_TPS_KEY))
    size_mib = to_float(trow.get(TPUT_SIZE_KEY))
    tx_count = to_int(trow.get(TPUT_TX_COUNT_KEY))
    bg_tx_count = stderr_counts.get("bg_tx_count") if stderr_counts else None
    ms_per_tx = (1000.0 / tx_per_s) if (tx_per_s and tx_per_s > 0) else None

    row: Dict[str, object] = {
        "binary": binary, "family": family, "backend": backend,
        "query": query, "cell": cell, "sf": sf, "dram_gib": dram_gib,
        "structure": structure if structure is not None else "",
        "method": method, "tx": tx, "bg": bg, "rep": rep,
        "tx_per_s": tx_per_s, "tx_count": tx_count, "bg_tx_count": bg_tx_count,
        "ms_per_tx": ms_per_tx, "size_mib": size_mib, "commit_sha": commit_sha,
    }

    # Diagnostics — best effort.
    cpu = read_last_row(run_dir / tx / method / "cpu.csv") if method and tx else None
    bm = read_last_row(run_dir / tx / method / "bm.csv") if method and tx else None
    cr = read_last_row(run_dir / tx / method / "cr.csv") if method and tx else None
    dt = read_last_row(run_dir / tx / method / "dt.csv") if method and tx else None
    lat = read_last_row(run_dir / tx / method / "latency.csv") if method and tx else None

    def _g(d, k):
        return d.get(k) if d else None

    row["cpu_cycles_per_tx"] = _g(cpu, "workers Cycles / TX")
    row["cpu_instr_per_tx"] = _g(cpu, "workers Instructions / TX")
    row["cpu_l1_miss_per_tx"] = _g(cpu, "workers L1-misses / TX")
    row["cpu_llc_miss_per_tx"] = _g(cpu, "workers LLC-misses / TX")
    row["cpu_branch_miss_per_tx"] = _g(cpu, "workers branch-misses / TX")
    row["cpu_util_pct"] = _g(cpu, "workers CPU Util (%)")
    row["bm_free_pct"] = _g(bm, "bm_free_pct")
    row["bm_consumed_pages"] = _g(bm, "bm_consumed_pages")
    row["bm_p1_pct"] = _g(bm, "bm_p1_pct")
    row["bm_p2_pct"] = _g(bm, "bm_p2_pct")
    row["bm_p3_pct"] = _g(bm, "bm_p3_pct")
    row["bm_evicted_mib"] = _g(bm, "bm_evicted_mib")
    row["bm_rounds"] = _g(bm, "bm_rounds")
    row["cr_committed"] = _g(cr, "cr_committed")
    row["cr_restarts"] = _g(cr, "cr_restarts")
    row["cr_gct_committed"] = _g(cr, "cr_gct_committed")
    row["cr_wal_write_gib"] = _g(cr, "cr_wal_write_gib")
    row["dt_struct_split"] = _g(dt, "dt_struct_split")
    row["dt_misses_counter"] = _g(dt, "dt_misses_counter")
    row["latency_p50_ms"] = _g(lat, "TXT P50")
    row["latency_p99_ms"] = _g(lat, "TXT P99")

    if stderr_counts:
        row["lsm_block_read_count"] = stderr_counts.get("lsm_block_read_count")
        row["lsm_block_cache_hit"] = stderr_counts.get("lsm_block_cache_hit")
        row["lsm_block_cache_miss"] = stderr_counts.get("lsm_block_cache_miss")
    else:
        row["lsm_block_read_count"] = None
        row["lsm_block_cache_hit"] = None
        row["lsm_block_cache_miss"] = None

    return row


def write_csv(rows: List[Dict[str, object]], path: Path, fields: List[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in fields})


def compute_stats(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    # Group by (binary, cell, structure, bg, method, tx)
    grouped: Dict[Tuple, List[Dict[str, object]]] = {}
    for r in rows:
        key = (r["binary"], r["cell"], r["structure"], r["bg"], r["method"], r["tx"])
        grouped.setdefault(key, []).append(r)
    out = []
    for key, group in grouped.items():
        binary, cell, structure, bg, method, tx = key
        tps_vals = [r["tx_per_s"] for r in group if r.get("tx_per_s") is not None]
        bg_vals = [r["bg_tx_count"] for r in group if r.get("bg_tx_count") is not None]
        family, backend, query = FAMILY_OF.get(binary, ("", "", ""))
        out.append({
            "binary": binary, "family": family, "backend": backend, "query": query,
            "cell": cell, "structure": structure, "bg": bg, "method": method, "tx": tx,
            "n": len(group),
            "tx_per_s_median": median(tps_vals),
            "tx_per_s_iqr": iqr(tps_vals),
            "bg_tx_count_median": median(bg_vals),
        })
    return out


STATS_FIELDS = [
    "binary", "family", "backend", "query", "cell", "structure", "bg",
    "method", "tx", "n", "tx_per_s_median", "tx_per_s_iqr", "bg_tx_count_median",
]


def compute_inversions(rows: List[Dict[str, object]], threshold: float) -> List[Dict[str, object]]:
    # Group by (binary, cell, bg, rep, tx); compare S3 vs S2.
    grouped: Dict[Tuple, Dict[object, Dict[str, object]]] = {}
    for r in rows:
        key = (r["binary"], r["cell"], r["bg"], r["rep"], r["tx"])
        grouped.setdefault(key, {})[r["structure"]] = r
    out = []
    for key, by_struct in grouped.items():
        s2 = by_struct.get(2)
        s3 = by_struct.get(3)
        if not (s2 and s3):
            continue
        t2 = s2.get("tx_per_s") or 0
        t3 = s3.get("tx_per_s") or 0
        if not (t2 and t3 and t2 > 0):
            continue
        ratio = t3 / t2
        if ratio >= (1 - threshold):
            continue
        binary, cell, bg, rep, tx = key
        delta_cols = []
        for col in ("cpu_llc_miss_per_tx", "cpu_cycles_per_tx",
                    "bm_p2_pct", "bm_evicted_mib", "cr_restarts"):
            v2 = to_float(s2.get(col))
            v3 = to_float(s3.get(col))
            if v2 is not None and v3 is not None and v2 != 0:
                delta_cols.append((abs(v3 - v2) / max(v2, 1e-9), col))
        hint = max(delta_cols)[1] if delta_cols else ""
        out.append({
            "binary": binary, "cell": cell, "bg": bg, "rep": rep, "tx": tx,
            "s2_method": s2["method"], "s3_method": s3["method"],
            "s2_tx_per_s": s2["tx_per_s"], "s3_tx_per_s": s3["tx_per_s"],
            "ratio": ratio,
            "s2_cpu_cycles_per_tx": s2.get("cpu_cycles_per_tx"),
            "s3_cpu_cycles_per_tx": s3.get("cpu_cycles_per_tx"),
            "s2_cpu_llc_miss_per_tx": s2.get("cpu_llc_miss_per_tx"),
            "s3_cpu_llc_miss_per_tx": s3.get("cpu_llc_miss_per_tx"),
            "s2_bm_p2_pct": s2.get("bm_p2_pct"),
            "s3_bm_p2_pct": s3.get("bm_p2_pct"),
            "attribution_hint": hint,
        })
    return out


INV_FIELDS = [
    "binary", "cell", "bg", "rep", "tx", "s2_method", "s3_method",
    "s2_tx_per_s", "s3_tx_per_s", "ratio",
    "s2_cpu_cycles_per_tx", "s3_cpu_cycles_per_tx",
    "s2_cpu_llc_miss_per_tx", "s3_cpu_llc_miss_per_tx",
    "s2_bm_p2_pct", "s3_bm_p2_pct", "attribution_hint",
]


def read_commit_sha(manifest_path: Path) -> str:
    if not manifest_path.exists():
        return ""
    try:
        for line in manifest_path.read_text().splitlines():
            if line.startswith("commit_sha:"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return ""


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", required=True, help="sweep tag (informational)")
    ap.add_argument("--root", required=True, type=Path,
                    help="paper-data/<tag>/ directory")
    ap.add_argument("--inversion-threshold", type=float, default=0.05,
                    help="fraction below which S3 < S2 counts as an inversion (default 0.05)")
    args = ap.parse_args()

    raw_root = args.root / "raw"
    summary_root = args.root / "summary"
    summary_root.mkdir(parents=True, exist_ok=True)
    commit_sha = read_commit_sha(args.root / "manifest.yaml")

    all_rows: List[Dict[str, object]] = []
    if not raw_root.exists():
        print(f"[analyzer] no raw dir: {raw_root}", file=sys.stderr)
        sys.exit(1)

    for binary_dir in sorted(raw_root.iterdir()):
        if not binary_dir.is_dir():
            continue
        binary = binary_dir.name
        if binary not in FAMILY_OF:
            print(f"[analyzer] skipping unknown binary: {binary}", file=sys.stderr)
            continue
        for run_dir in sorted(binary_dir.iterdir()):
            m = RUN_RE.match(run_dir.name)
            if not m or not run_dir.is_dir():
                continue
            cell = m["cell"]; bg = int(m["bg"]); rep = int(m["rep"])
            try:
                rows = collect_rep(binary, cell, bg, rep, run_dir, commit_sha)
            except Exception as e:
                print(f"[analyzer] error in {run_dir}: {e}", file=sys.stderr)
                continue
            all_rows.extend(rows)

    print(f"[analyzer] collected {len(all_rows)} rows from {raw_root}", file=sys.stderr)

    write_csv(all_rows, summary_root / "headline.csv", HEADLINE_FIELDS)
    write_csv(all_rows, summary_root / "diagnostics.csv", DIAG_FIELDS)
    stats_rows = compute_stats(all_rows)
    write_csv(stats_rows, summary_root / "stats.csv", STATS_FIELDS)
    inv_rows = compute_inversions(all_rows, args.inversion_threshold)
    write_csv(inv_rows, summary_root / "inversions.csv", INV_FIELDS)

    print(f"[analyzer] wrote {summary_root}/{{headline,diagnostics,stats,inversions}}.csv",
          file=sys.stderr)
    print(f"[analyzer] inversions flagged: {len(inv_rows)}", file=sys.stderr)


if __name__ == "__main__":
    main()
