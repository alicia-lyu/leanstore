#!/usr/bin/env bash
# Quick status snapshot for a running paper sweep.
# Usage: experiments/sweep_status.sh [tag]

set -uo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
    # Pick most-recent tag dir.
    TAG=$(ls -1d paper-data/*/ 2>/dev/null | sort | tail -1 | sed 's:^paper-data/::; s:/$::')
fi
if [[ -z "$TAG" || ! -d "paper-data/$TAG" ]]; then
    echo "[status] no paper-data/<tag>/ directory found" >&2
    exit 1
fi

OUT_DIR="paper-data/${TAG}"
echo "=== sweep $TAG ==="
echo "dir: $OUT_DIR"
if [[ -f "${OUT_DIR}/sweep.pid" ]]; then
    PID=$(cat "${OUT_DIR}/sweep.pid")
    if kill -0 "$PID" 2>/dev/null; then
        echo "pid: $PID (running)"
    else
        echo "pid: $PID (not running)"
    fi
fi
if [[ -f "${OUT_DIR}/run.log" ]]; then
    echo "log lines: $(wc -l < "${OUT_DIR}/run.log")"
    echo "last 10 [runner] lines:"
    grep -E "^\[20[0-9]{2}" "${OUT_DIR}/run.log" | tail -10 | sed 's/^/  /'
fi

if [[ -d "${OUT_DIR}/raw" ]]; then
    echo ""
    echo "snapshots:"
    for bdir in "${OUT_DIR}/raw"/*; do
        [[ -d "$bdir" ]] || continue
        n=$(ls -1 "$bdir" 2>/dev/null | wc -l)
        echo "  $(basename "$bdir"): $n run dirs"
    done
fi

if [[ -d "${OUT_DIR}/summary" ]]; then
    echo ""
    echo "summary:"
    for f in headline.csv stats.csv inversions.csv; do
        p="${OUT_DIR}/summary/$f"
        [[ -f "$p" ]] && echo "  $f: $(($(wc -l < "$p") - 1)) rows"
    done
fi
