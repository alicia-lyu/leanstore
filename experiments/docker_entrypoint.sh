#!/usr/bin/env bash
# docker_entrypoint.sh — artifact sweep dispatcher.
#
# Reads the CELL env var and forwards to the appropriate runner. Each cell
# writes its output under /results/<cell>/ in paper-data summary layout so
# the "plots" cell can read them all with --tag-map overrides.
#
# Cells:
#   tpch-headline      SSD headline sweep → Fig. 4 (btree+lsm), Fig. 5 (q10),
#                      and SST diagnostics (sstables.csv captured automatically)
#   tpch-headline-hdd  HDD LSM subset     → supplementary tpch_lsm_headline_hdd
#   refresh            Refresh benchmark  → Fig. 6, Fig. 7   [STUB — see below]
#   dbtoaster          DBToaster baseline → refresh_sales_dbtoaster_throughput.csv
#   plots              Paper plotter + macro generator → /results/paper-ready/
#
# NOTE: there is no separate sst-diagnostics cell. run_paper_sweep.sh already
# snapshots sstables.csv for every LSM run (lines 289/344 of that script).
# The diagnostics plotter is invoked by the "plots" cell against the
# tpch-headline tag dir — no extra sweep is needed.
#
# Env knobs (forwarded to runners; all optional):
#   SF          TPC-H scale factor override (default: per-cell table in run_paper_sweep.sh)
#   DRAM_GIB    DRAM budget in GiB (default: per-cell table)
#   QUERIES     Comma-separated binary names to restrict the sweep
#   BACKENDS    lsm, btree, or lsm,btree (default: both)
#   REPS        Repetitions per (binary,cell,structure,bg) (default: 5)
#
# Usage in docker_run.sh:
#   docker run -e CELL=tpch-headline -v $RESULTS:/results \
#              -v /mnt/nvme/leanstore:/mnt/nvme/leanstore \
#              ghcr.io/alicia-lyu/leanstore:vldb26

set -euo pipefail

CELL="${CELL:-}"
REPS="${REPS:-5}"
RESULTS="/results"
REPO="/leanstore"
SCRIPTS="$REPO/paper-data/scripts"

log() { echo "[entrypoint] $*" >&2; }

if [[ -z "$CELL" ]]; then
    echo "[entrypoint] ERROR: CELL env var not set." >&2
    echo "[entrypoint] Valid cells: tpch-headline tpch-headline-hdd refresh dbtoaster plots" >&2
    exit 1
fi

log "CELL=$CELL REPS=$REPS"

# ---------------------------------------------------------------------------
# Helper: run run_paper_sweep.sh writing output directly to /results/<tag>/.
# run_paper_sweep.sh accepts --root <dir> and writes summary/, manifest.yaml,
# and raw/ under that directory (see its output layout header comment).
# ---------------------------------------------------------------------------
run_sweep() {
    local neutral_tag="$1"; shift   # e.g. "tpch-headline"
    local extra_args=("$@")

    local out_dir="$RESULTS/$neutral_tag"
    mkdir -p "$out_dir"

    "$REPO/experiments/run_paper_sweep.sh" \
        --tag "$neutral_tag" \
        --reps "$REPS" \
        --root "$out_dir" \
        "${extra_args[@]}"

    log "sweep done → $out_dir"
}

case "$CELL" in

    # -----------------------------------------------------------------------
    tpch-headline)
        # SSD headline: all TPC-H + TPC-Hi families, both backends.
        # Drives Fig. 4 (btree + lsm), Fig. 5 (q10), the supplementary
        # paper_tpch_vanilla panel, and SST diagnostics (Fig. diag_ssd_lsm_sst_path).
        # sstables.csv is captured by run_paper_sweep.sh for every LSM run.
        log "running SSD headline sweep..."
        run_sweep "tpch-headline" \
            --families tpch,tpchi
        ;;

    # -----------------------------------------------------------------------
    tpch-headline-hdd)
        # HDD LSM subset. Drives the supplementary tpch_lsm_headline_hdd figure.
        # Requires /mnt/hdd/leanstore to be present on the host (bind-mounted).
        log "running HDD headline sweep (LSM only)..."
        run_sweep "tpch-headline-hdd" \
            --families tpch,tpchi \
            --backends lsm \
            --disk hdd
        ;;

    # -----------------------------------------------------------------------
    refresh)
        # KNOWN GAP — this cell cannot currently reproduce Fig. 6 and Fig. 7.
        #
        # The refresh figures require a dedicated runner that:
        #   1. Invokes the LeanStore binary in RF1+RF2 update mode (not query mode).
        #   2. Emits raw/<cell>/<be>.s<N>.csv consumed by summarize_refresh_10L.py.
        #   3. Recovers from per-structure image copies and drops OS page caches.
        #
        # This runner (build/scratch/run_refresh_10L_*.sh on the author's Linux
        # machine) was never committed. See frontend/tpch/refresh_sales/RUNS.md
        # for the pattern and REPRODUCE.md §Known gaps for the full explanation.
        #
        # Linux follow-up: commit experiments/run_refresh_sweep.sh that implements
        # the above layout and replace this error with a real invocation.
        echo "[entrypoint] ERROR: refresh cell is not yet implemented." >&2
        echo "[entrypoint] The refresh runner was not committed to the repo." >&2
        echo "[entrypoint] See REPRODUCE.md §Known gaps and LINUX_PENDING.md for details." >&2
        exit 1
        ;;

    # -----------------------------------------------------------------------
    dbtoaster)
        # Run the pre-built DBToaster refresh_sales binary and capture output.
        log "running DBToaster refresh_sales baseline..."
        out_dir="$RESULTS/dbtoaster"
        mkdir -p "$out_dir/summary"

        BIN="$REPO/dbtoaster/build/refresh_sales"
        if [[ ! -x "$BIN" ]]; then
            log "refresh_sales binary not found at $BIN — rebuilding..."
            make -C "$REPO/dbtoaster" build
        fi

        # entrypoint.sh wraps /usr/bin/time -v; capture CSV to summary/.
        BIN="$BIN" "$REPO/dbtoaster/entrypoint.sh" \
            > "$out_dir/summary/refresh_sales_dbtoaster_throughput.csv" 2>&1 \
            || true   # non-zero exit when /usr/bin/time is unavailable; CSV may still be valid

        # Minimal manifest so the plotter can read commit/host metadata.
        cat > "$out_dir/manifest.yaml" <<YAML
tag: dbtoaster
commit_sha: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo unknown)
host: $(hostname)
cells: dbtoaster
families: dbtoaster
reps: 1
YAML
        log "DBToaster done → $out_dir"
        ;;

    # -----------------------------------------------------------------------
    plots)
        # Invoke the paper plotter and macro generators with --tag-map so they
        # read from /results/<neutral-tag>/ instead of in-repo paper-data/<dated-tag>/.
        #
        # Tag map: authored diagrams.yaml tag → neutral cell dir under /results/.
        # CPU/memory authoring-only diagrams are excluded (not tex-referenced).
        log "running paper plotter and macro generators..."

        PAPER_READY="$RESULTS/paper-ready"
        mkdir -p "$PAPER_READY"

        TAG_MAP='{
          "2026-05-29-rep0-10L":    "tpch-headline",
          "2026-05-18-b":           "tpch-headline-hdd",
          "2026-05-30-refresh-10L": "refresh",
          "2026-05-24-dbtoaster":   "dbtoaster"
        }'

        # Tex-referenced diagrams + two supplementary ones.
        # refresh_5L_pair_latency and refresh_lsm_vs_btree are attempted but
        # will emit "no data" panels when the refresh cell was not run.
        DIAGRAMS="paper_tpch_btree_headline,paper_tpch_lsm_headline,paper_q10,\
paper_tpch_vanilla,paper_tpch_lsm_headline_hdd,\
refresh_5L_pair_latency,refresh_lsm_vs_btree"

        python3 "$SCRIPTS/plot_paper_sweep.py" \
            --diagram "$DIAGRAMS" \
            --tag-map "$TAG_MAP" \
            --results-root "$RESULTS"

        # SST diagnostics: invoke against the tpch-headline results dir.
        # sstables.csv was captured there by run_paper_sweep.sh for every LSM run.
        DIAG_ROOT="$RESULTS/tpch-headline"
        if [[ -d "$DIAG_ROOT" ]]; then
            python3 "$SCRIPTS/plot_lsm_s3_vs_s2_diagnostics.py" \
                --tag "tpch-headline" \
                --root "$DIAG_ROOT" 2>/dev/null \
                || log "WARN: SST diagnostics plotter failed (non-fatal)"
        else
            log "WARN: tpch-headline dir not found; skipping SST diagnostics"
        fi

        # Copy all produced PDFs to paper-ready/.
        DIAGRAMS_DIR="$REPO/paper-data/diagrams"
        if [[ -d "$DIAGRAMS_DIR" ]]; then
            cp "$DIAGRAMS_DIR"/*.pdf "$PAPER_READY/" 2>/dev/null || true
        fi

        # Macro generator: writes experiment_numbers.{json,tex}.
        python3 "$SCRIPTS/refresh_tex_numbers.py" \
            --paper-data-root "$RESULTS" \
            --headline-tag "tpch-headline" \
            --refresh-tag "refresh" \
            --dbt-tag "dbtoaster" \
            --tex-dir "$PAPER_READY" \
            || log "WARN: refresh_tex_numbers.py failed (non-fatal)"

        # Space table: writes space_table.txt.
        python3 "$SCRIPTS/space_table.py" \
            --root "$RESULTS" \
            --sources "tpch-headline:2,tpch-headline:0,tpch-headline:0" \
            > "$PAPER_READY/space_table.txt" \
            || log "WARN: space_table.py failed (non-fatal)"

        log "plots done → $PAPER_READY"
        ;;

    *)
        echo "[entrypoint] ERROR: unknown CELL='$CELL'" >&2
        echo "[entrypoint] Valid cells: tpch-headline tpch-headline-hdd refresh dbtoaster plots" >&2
        exit 1
        ;;
esac
