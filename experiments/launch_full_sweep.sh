#!/usr/bin/env bash
#
# Launch the full paper-ready sweep in the background.
#
# Cell order: c2 → c1 → c3 → c0. c2 is smallest (fastest validation).
# c1 doubles the secondary. c3 jumps SF to anchor-size at 0.4 GiB DRAM,
# which loads the heavy image; c0 then reuses that loaded image.
#
# Usage:
#   experiments/launch_full_sweep.sh [TAG]
#
# Writes paper-data/<tag>/run.log and paper-data/<tag>/sweep.{out,err}.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TAG="${1:-}"
if [[ -z "$TAG" ]]; then
    DAY=$(date -u +%Y-%m-%d)
    SUFFIX=a
    while [[ -d "paper-data/${DAY}-${SUFFIX}" ]]; do
        SUFFIX=$(echo "$SUFFIX" | tr 'a-y' 'b-z')
    done
    TAG="${DAY}-${SUFFIX}"
fi

OUT_DIR="paper-data/${TAG}"
mkdir -p "$OUT_DIR"

LOG_OUT="${OUT_DIR}/sweep.out"
LOG_ERR="${OUT_DIR}/sweep.err"

echo "[launch] tag=$TAG  out=$OUT_DIR" | tee -a "$LOG_OUT"
echo "[launch] starting at $(date -u +'%Y-%m-%dT%H:%M:%SZ')" | tee -a "$LOG_OUT"
echo "[launch] commit=$(git rev-parse --short HEAD)  host=$(hostname)" | tee -a "$LOG_OUT"

nohup ./experiments/run_paper_sweep.sh --tag "$TAG" --cells c2,c1,c3,c0 --families tpch,tpchi,geo \
    >> "$LOG_OUT" 2>> "$LOG_ERR" &
PID=$!
echo $PID > "${OUT_DIR}/sweep.pid"
echo "[launch] pid=$PID  log=$LOG_OUT" | tee -a "$LOG_OUT"
echo "[launch] tail -f $LOG_OUT  for live progress"
