#!/bin/bash
# Substitution-parameter A/B test for one TPC-H per-query binary.
#
# Runs each storage structure twice on the SAME loaded image:
#   A = --param_seed=0  (validation default parameter — the historical
#                        single point every prior sweep used)
#   B = --param_seed=K  (a rotated parameter)
# then reports the per-structure A and B ms/query and the B/A ratio, and the
# rank order of structures under A vs B (param_ab_report.py).
#
# Why A/B: the headline numbers rest on a single substitution parameter
# (at the 5L cell one query fills the measurement window, so the loop only
# ever runs PARAM_TABLE[param_seed]). This A/B shows whether the *ranking*
# (which approach wins) holds when the parameter changes — robustness — while
# keeping A (default) as the reported headline, consistent with prior runs.
# Same image (one reload, two query runs/structure): per
# feedback_perf_fixes_ab_same_image.
#
# Usage:
#   experiments/run_param_ab.sh <binary> <scale> [dram] [seed_b] [bg] [structs]
#   e.g. experiments/run_param_ab.sh q3_lsm 1550 1.0 7 2 "1 2 3 4"
#
# NOTE: this DOES run the perf binaries (it is the A/B harness). It is "code"
# in the sense of being committed tooling; only invoke it when the node is free.
set -uo pipefail
cd "$(dirname "$0")/.."   # repo root (worktree-aware)

BINARY=${1:?usage: run_param_ab.sh <binary> <scale> [dram] [seed_b] [bg] [structs]}
SCALE=${2:?scale factor}
DRAM=${3:-1.0}
SEED_B=${4:-7}
BG=${5:-2}
STRUCTS=${6:-"1 2 3 4"}

case "$BG" in
    0) BGF="bg_query_thread=false bg_point_lookups=false" ;;
    1) BGF="bg_query_thread=true bg_point_lookups=false" ;;
    2) BGF="bg_query_thread=true bg_point_lookups=true" ;;
    *) echo "bad bg=$BG (0|1|2)"; exit 1 ;;
esac

LOG="build/${BINARY}/${SCALE}-in-${DRAM}"
OUT="paper-data/param-ab/${BINARY}-sf${SCALE}-bg${BG}-seedB${SEED_B}"
mkdir -p "$OUT"
echo "[param-ab] binary=$BINARY scale=$SCALE dram=$DRAM seed_b=$SEED_B bg=$BG structs='$STRUCTS'"

# --- A arm: reload (first make) + run all structures at the default param. ---
echo "[param-ab] A (param_seed=0): reload + run"
make "$BINARY" scale="$SCALE" dram="$DRAM" $BGF param_seed=0
for s in $STRUCTS; do
    [[ -f "$LOG/structure$s.log" ]] && cp "$LOG/structure$s.log" "$OUT/structure${s}_A.log"
done

# --- B arm: rotated param on the SAME image (no reload — recover_file built). ---
echo "[param-ab] B (param_seed=$SEED_B): rerun each structure on the same image"
for s in $STRUCTS; do
    make "${BINARY}_${s}" scale="$SCALE" dram="$DRAM" $BGF param_seed="$SEED_B"
    [[ -f "$LOG/structure$s.log" ]] && cp "$LOG/structure$s.log" "$OUT/structure${s}_B.log"
done

echo "[param-ab] report:"
python3 paper-data/scripts/param_ab_report.py "$OUT" \
    --binary "$BINARY" --seed-b "$SEED_B" \
    --structures "$(echo "$STRUCTS" | tr ' ' ',')" | tee "$OUT/report.txt"
echo "[param-ab] wrote $OUT/report.txt"
