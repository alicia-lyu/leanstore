#!/usr/bin/env bash
#
# Launch the paper-ready sweep in the background, two phases per tag.
#
# Phase 1: TPC-H families (tpch,tpchi) across cells c1,c3,c0.
# Phase 2: geo family across the same cells.
# Geo runs last so a mid-sweep halt does not cost newly-collected
# TPC-H cells (TPC-H runs much faster per cell than geo).
#
# c2 is skipped by default — the prior -a sweep already covered c2 for
# 9 of 10 binaries (bg=0/1). geo_btree c2 has only bg=0 r1; we accept
# that gap rather than spend the load cost again.
#
# Usage:
#   experiments/launch_full_sweep.sh [TAG] [CELLS]
#     TAG    default = today's date with the next letter suffix.
#     CELLS  default = c1,c3,c0
#
# Writes paper-data/<tag>/run.log and paper-data/<tag>/sweep.{out,err}.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

TAG="${1:-}"
CELLS="${2:-c1,c3,c0}"
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

echo "[launch] tag=$TAG  cells=$CELLS  out=$OUT_DIR" | tee -a "$LOG_OUT"
echo "[launch] starting at $(date -u +'%Y-%m-%dT%H:%M:%SZ')" | tee -a "$LOG_OUT"
echo "[launch] commit=$(git rev-parse --short HEAD)  host=$(hostname)" | tee -a "$LOG_OUT"

nohup bash -c "
    set -o pipefail
    echo '[launch] === phase 1: TPC-H families ==='
    ./experiments/run_paper_sweep.sh --tag '$TAG' --cells '$CELLS' --families tpch,tpchi --continue
    rc1=\$?
    echo \"[launch] phase 1 exit=\$rc1\"
    echo '[launch] === phase 2: geo family (slowest, run last) ==='
    ./experiments/run_paper_sweep.sh --tag '$TAG' --cells '$CELLS' --families geo --continue
    rc2=\$?
    echo \"[launch] phase 2 exit=\$rc2\"
    echo \"[launch] all phases done. rc1=\$rc1 rc2=\$rc2\"
" >> "$LOG_OUT" 2>> "$LOG_ERR" &
PID=$!
echo $PID > "${OUT_DIR}/sweep.pid"
echo "[launch] pid=$PID  log=$LOG_OUT" | tee -a "$LOG_OUT"
echo "[launch] tail -f $LOG_OUT  for live progress"
