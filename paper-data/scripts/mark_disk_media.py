#!/usr/bin/env python3
"""Annotate sweep summary CSVs with the storage medium they were measured on.

Adds a trailing `disk` column (value `hdd` or `ssd`) to every
`<root>/**/summary/*.csv`. Idempotent: a file that already carries a `disk`
column is left untouched, so re-running is safe.

Context: through 2026-05-24 the experiment mount `/mnt/ssd` was actually a
spinning SAS HDD (/dev/sdb); the real Intel DC S3500 SATA SSD (/dev/sdc) was
unmounted. Every pre-2026-05-24 summary is therefore HDD-measured. This script
stamps that fact onto the CSVs so downstream plots/readers can distinguish the
HDD baselines from the SSD reruns.

Usage:
    python3 scripts/mark_disk_media.py <root-dir> <hdd|ssd>
    # e.g.
    python3 scripts/mark_disk_media.py 2026-05-18-b hdd
    python3 scripts/mark_disk_media.py 2026-05-24-a-ssd ssd
"""
import sys
import glob
import os


def mark_csv(path: str, value: str) -> str:
    with open(path) as fh:
        lines = fh.read().splitlines()
    if not lines:
        return "empty"
    header = lines[0].split(",")
    if "disk" in header:
        return "already-marked"
    out = [lines[0] + ",disk"]
    for ln in lines[1:]:
        if ln.strip() == "":
            out.append(ln)            # preserve blank lines verbatim
        else:
            out.append(ln + "," + value)
    with open(path, "w") as fh:
        fh.write("\n".join(out) + "\n")
    return f"marked {len(out) - 1} rows"


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[2] not in ("hdd", "ssd"):
        print(__doc__)
        return 2
    root, value = sys.argv[1], sys.argv[2]
    csvs = sorted(glob.glob(os.path.join(root, "**", "summary", "*.csv"), recursive=True))
    if not csvs:
        print(f"[mark_disk_media] no summary CSVs under {root}")
        return 0
    for p in csvs:
        print(f"[{value}] {p}: {mark_csv(p, value)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
