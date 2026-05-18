#!/usr/bin/env bash
#
# Paper-sweep runner (stub).
#
# Reads experiments/sweep.yaml and produces:
#   build/<binary>/<sweep-tag>/c<N>-bg<0|1>-r<R>/      raw CSVs
#   paper-data/<sweep-tag>/{headline,diagnostics,inversions,stats}.csv  summary
#   paper-data/<sweep-tag>/manifest.yaml               commit SHA + host info
#
# See ../PAPER_SWEEP.md for the canonical spec.
#
# STUB STATUS: this file pins the runner contract and CLI shape. The
# actual cell-iteration + summary-CSV emission is the next commit; this
# file lives in tree now so the spec, the matrix YAML, and the runner
# interface are co-located.

set -euo pipefail

usage() {
    cat <<EOF
Usage: $(basename "$0") [--tag <sweep-tag>] [--dry-run] [--family tpch|tpchi|geo|all]

  --tag         Sweep tag (default: YYYY-MM-DD-a). Auto-incremented suffix
                if a directory with the chosen tag already exists.
  --dry-run     Print the commands that would run; don't execute.
  --family      Limit the sweep to one family. Default: all (tpch, tpchi, geo).
  --skip-load   Skip the per-family load phase; assume images exist.

Reads:  experiments/sweep.yaml + git HEAD
Writes: build/.../*.csv (raw) + paper-data/<tag>/*.csv (summary)

The runner is a stub at HEAD; argument parsing works but iteration
is not yet implemented. The cell list, CSV schema, and output paths
are pinned by experiments/sweep.yaml and ../PAPER_SWEEP.md.
EOF
}

TAG=""
DRY_RUN=0
FAMILY="all"
SKIP_LOAD=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)        TAG="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=1; shift ;;
        --family)     FAMILY="$2"; shift 2 ;;
        --skip-load)  SKIP_LOAD=1; shift ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "unknown arg: $1" >&2; usage; exit 1 ;;
    esac
done

if [[ -z "$TAG" ]]; then
    DAY=$(date -u +%Y-%m-%d)
    # Auto-increment -a, -b, -c, ... if a tag already exists.
    SUFFIX=a
    while [[ -d "paper-data/${DAY}-${SUFFIX}" ]]; do
        SUFFIX=$(echo "$SUFFIX" | tr 'a-y' 'b-z')
    done
    TAG="${DAY}-${SUFFIX}"
fi

echo "[runner] sweep tag:       $TAG"
echo "[runner] family:          $FAMILY"
echo "[runner] dry-run:         $DRY_RUN"
echo "[runner] skip-load:       $SKIP_LOAD"
echo "[runner] commit SHA:      $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "[runner] host:            $(hostname)"
echo ""
echo "[runner] STUB: cell iteration is not yet implemented in this commit."
echo "[runner] See ../PAPER_SWEEP.md and ./sweep.yaml for the canonical matrix."
echo "[runner] To run a single cell manually, use the existing make targets:"
echo "[runner]   make q3_lsm scale=40 dram=0.1            # foreground only"
echo "[runner]   make q3_lsm scale=40 dram=0.1 \\"
echo "[runner]                bg_query_thread=true        # bg cohort cycling"
echo ""
exit 0
