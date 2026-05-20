#!/usr/bin/env bash
#
# Wait for the in-flight -b sweep (PID arg or pidfile) to finish, then
# launch -c at cells c2 only to close the bg=2 vs_secondary gap.
#
# Usage:
#   nohup experiments/chain_c2_after_b.sh > /tmp/chain.log 2>&1 &

set -uo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

B_TAG="2026-05-18-b"
C_TAG="2026-05-18-c"
PID_FILE="paper-data/${B_TAG}/sweep.pid"

if [[ ! -f "$PID_FILE" ]]; then
    echo "[chain] no pidfile at $PID_FILE — abort"
    exit 1
fi
B_PID=$(cat "$PID_FILE")
echo "[chain] waiting on -b PID=$B_PID  ($(date -u +'%Y-%m-%dT%H:%M:%SZ'))"

# kill -0 returns 0 if process exists, non-zero if not. Poll every 10 min.
while kill -0 "$B_PID" 2>/dev/null; do
    sleep 600
done

echo "[chain] -b done at $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "[chain] launching -c (c2 only) ..."

# c2 bg=2 across all three families — closes the vs_secondary axis
# gap. c2 SFs are SF=150/380 (TPC-H) and SF=19/52 (geo); cached
# images should make this a runs-only sweep.
exec ./experiments/launch_full_sweep.sh "$C_TAG" c2
