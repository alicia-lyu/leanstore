#!/bin/bash
# Q10 / Q10I bg=2, 3-rep rerun (audit-response, Finding #2).
#
# Why: the q10.pdf figure was built from hand-ported bg=0, single-rep data
# whose bg column was relabeled 0→2 in plot_paper_sweep.py. The paper setup
# commits all measured queries to bg=2 (two worker threads + point-lookup
# stream) and 3 reps. This runner re-measures Q10/Q10i under that protocol so
# the figure's provenance matches the text. Once it finishes, drop the bg
# relabel in _augment_with_q10 and regenerate q10.pdf from genuine bg=2 data.
#
# Q10/Q10i are standalone (own image dirs), so this drives their make targets
# directly rather than via the q3/q5/q3i/q5i family loader in
# run_paper_sweep.sh. It runs exactly the methods the figure shows:
#   Q10  : S1, S2-A(lineitem), S2-B(preagg), S3, S4, S5(aCOL)
#   Q10I : S1, S2-A(lineitem), S2-B(preagg), S3, S4, S5(aCOLI)
# param_seed=0 (rotation off) so these stay comparable to the main sweep.
#
# Usage:
#   experiments/run_q10_q10i_bg2.sh            # full 5L, 3 reps (~days)
#   experiments/run_q10_q10i_bg2.sh --smoke    # c2-small SF, 1 rep (validate)
#
# Cost warning: at 5L a single Q10 S4 query can take ~30-70 min, and bg=2
# adds contention. The full run is multi-day and monopolizes the node.
# Logs stream to $LOGFILE; structure logs are copied per-rep under $TAGDIR.
set -uo pipefail
cd "$(dirname "$0")/.."   # repo root

SMOKE=0
[[ "${1:-}" == "--smoke" ]] && SMOKE=1

if [[ $SMOKE -eq 1 ]]; then
    REPS=1
    declare -A SF=( [q10_btree]=150 [q10_lsm]=380 [q10i_btree]=150 [q10i_lsm]=380 )
    TAG="q10-q10i-bg2-smoke-$(date -u +%Y%m%d-%H%M%S)"
else
    REPS=3
    # 5L (c0) scale factors, matching SF_TPCH in analyze_paper_sweep.py.
    declare -A SF=( [q10_btree]=1550 [q10_lsm]=3850 [q10i_btree]=1550 [q10i_lsm]=3850 )
    TAG="q10-q10i-bg2-5L-$(date -u +%Y%m%d-%H%M%S)"
fi
DRAM=1.0
BG_FLAGS="bg_query_thread=true bg_point_lookups=true param_seed=0"

TAGDIR="paper-data/${TAG}"
LOGFILE="${TAGDIR}/run.log"
mkdir -p "$TAGDIR"

log() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOGFILE"; }

# Copy the structure logs a single `make q<q>_<be>[_<n>]` produced into a
# per-rep raw dir, with an optional method suffix.
capture() {
    local q=$1 be=$2 sf=$3 rep=$4 suffix=$5; shift 5
    local src="build/${q}_${be}/${sf}-in-${DRAM}"
    local dst="${TAGDIR}/raw/${q}_${be}/c0-bg2-r${rep}"
    mkdir -p "$dst"
    for s in "$@"; do
        [[ -f "$src/structure${s}.log" ]] && \
            cp "$src/structure${s}.log" "$dst/structure${s}${suffix}.log"
    done
    [[ -f "$src/TPut.csv" ]] && cp "$src/TPut.csv" "$dst/TPut.csv"
}

run_q10() {
    local be=$1 rep=$2; local sf=${SF[q10_${be}]}
    log "q10_${be} rep${rep} sf=${sf}: reload + S1-S4 (variant=lineitem)"
    make "q10_${be}" scale="$sf" dram="$DRAM" q10_stats=true $BG_FLAGS >>"$LOGFILE" 2>&1
    capture q10 "$be" "$sf" "$rep" "_baseA" 1 2 3 4
    log "q10_${be} rep${rep}: S2-B (preagg view)"
    make "q10_${be}_2" scale="$sf" dram="$DRAM" q10_stats=true q10_view_variant=preagg $BG_FLAGS >>"$LOGFILE" 2>&1
    capture q10 "$be" "$sf" "$rep" "_preagg" 2
    log "q10_${be} rep${rep}: S5 (aCOL)"
    make "q10_${be}_5" scale="$sf" dram="$DRAM" q10_stats=true $BG_FLAGS >>"$LOGFILE" 2>&1
    capture q10 "$be" "$sf" "$rep" "_acol" 5
}

run_q10i() {
    local be=$1 rep=$2; local sf=${SF[q10i_${be}]}
    log "q10i_${be} rep${rep} sf=${sf}: reload + S1-S5 (variant=lineitem)"
    make "q10i_${be}" scale="$sf" dram="$DRAM" $BG_FLAGS >>"$LOGFILE" 2>&1
    capture q10i "$be" "$sf" "$rep" "_baseA" 1 2 3 4 5
    log "q10i_${be} rep${rep}: S2-B (preagg view)"
    make "q10i_${be}_2" scale="$sf" dram="$DRAM" q10i_view_variant=preagg $BG_FLAGS >>"$LOGFILE" 2>&1
    capture q10i "$be" "$sf" "$rep" "_preagg" 2
}

log "START tag=${TAG} reps=${REPS} smoke=${SMOKE} bg_flags='${BG_FLAGS}'"
for rep in $(seq 1 "$REPS"); do
    for be in btree lsm; do
        run_q10  "$be" "$rep"
        run_q10i "$be" "$rep"
    done
done
log "DONE tag=${TAG}. Next: port medians into paper-data summaries (no bg"
log "relabel — data is genuinely bg=2), drop _augment_with_q10's bg line,"
log "regenerate q10.pdf. See paper-data/PAPER_EDITS.md + SWEEP_LOG.md."
