#!/usr/bin/env bash
#
# Refresh-sweep runner (TPC-H RF1/RF2 update throughput — paper Fig. 6 + 7).
#
# Committed generalization of the author's untracked
# build/scratch/run_refresh_bg2_ssd.sh: drives refresh_sales_{btree,lsm} in
# RF1+RF2 update mode across the 10-scale memory-pressure cells, recovering
# each (backend, structure) run from a PER-STRUCTURE COPY of the canonical tpch
# family image (so RF mutations never touch the shared read image), dropping the
# OS page cache before each timed run for a cold start. Hands off to
# paper-data/scripts/summarize_refresh_10L.py for the per-cell summary CSVs.
#
# Mirrors run_paper_sweep.sh's --root / --tag / --backends / --disk conventions
# so the artifact dispatcher (experiments/docker_entrypoint.sh) can invoke it the
# same way and land output at /results/refresh/{manifest.yaml,raw/,summary/}.
#
# Output layout (consumed by summarize_refresh_10L.py):
#   <root>/
#     manifest.yaml
#     raw/<cell>/<be>.s<N>.csv     (elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s)
#     raw/<cell>/<be>_s<N>.out     (binary stdout — RF1=/RF2= totals)
#     summary/refresh_sales_<cell>[_bg2]_throughput.csv
#
# Cells (mirror summarize_refresh_10L.py CELLS): 10LL (DRAM 20, bg=0, btree,
# prewarm), 10L (1.0, bg=2), 10H (0.5, bg=2), 10HH (0.2, bg=2). Only S1-S4 are
# supported (refresh_sales has no S5/S7 maintenance).

set -uo pipefail

usage() {
    cat <<EOF
Usage: $(basename "$0") [--tag <tag>] [--root <dir>] [--reps N]
                       [--backends lsm,btree] [--disk <token|path>]
                       [--cells 10LL,10L,10H,10HH]
                       [--sf-btree N] [--sf-lsm N] [--dry-run] [--skip-load]

  --tag         Sweep tag / label (default: YYYY-MM-DD-refresh-a).
  --root        Output directory. When set, output lands directly under
                <dir>/{manifest.yaml,raw/,summary/} (the artifact dispatcher
                passes /results/refresh). Default: paper-data/<tag>.
  --reps        Interface-compat only; refresh figures are single-rep, so
                values >1 are accepted but only one rep is run (warned).
  --backends    Comma-separated backend subset (lsm,btree). Default: both.
                Intersected with each cell's backend list.
  --disk        Data-disk for the family images. A bare token maps to
                /mnt/<token> (e.g. "hdd" -> /mnt/hdd); a slashed value is used
                verbatim. Default: /mnt/ssd. Threaded into the load make as
                data_disk=<path> and used as the per-structure copy root.
  --cells       Comma-separated cell subset. Default: 10LL,10L,10H,10HH.
  --sf-btree    TPC-H scale factor for the btree family image. Default: 4000.
  --sf-lsm      TPC-H scale factor for the lsm family image. Default: 10000.
  --dry-run     Print the commands; don't execute (no images required).
  --skip-load   Assume the family .json/.image already exist (no load make).
EOF
}

TAG=""
ROOT=""
REPS=1
BACKENDS="lsm,btree"
DISK=""
CELLS="10LL,10L,10H,10HH"
SF_BTREE=4000
SF_LSM=10000
DRY_RUN=0
SKIP_LOAD=0
REFRESH_SECONDS=90
UPDATE_SIZE=1
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)        TAG="$2"; shift 2 ;;
        --root)       ROOT="$2"; shift 2 ;;
        --reps)       REPS="$2"; shift 2 ;;
        --backends)   BACKENDS="$2"; shift 2 ;;
        --disk)       DISK="$2"; shift 2 ;;
        --cells)      CELLS="$2"; shift 2 ;;
        --sf-btree)   SF_BTREE="$2"; shift 2 ;;
        --sf-lsm)     SF_LSM="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=1; shift ;;
        --skip-load)  SKIP_LOAD=1; shift ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "unknown arg: $1" >&2; usage; exit 1 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Resolve the data disk: bare token -> /mnt/<token>; a path is used verbatim.
if [[ -z "$DISK" ]]; then
    DATA_DISK="/mnt/ssd"
elif [[ "$DISK" == */* ]]; then
    DATA_DISK="$DISK"
else
    DATA_DISK="/mnt/$DISK"
fi

# Parse the backend subset into a membership helper.
IFS=',' read -ra BACKEND_LIST <<< "$BACKENDS"
want_backend() {
    local b
    for b in "${BACKEND_LIST[@]}"; do
        [[ "$b" == "$1" ]] && return 0
    done
    return 1
}

if [[ "$REPS" != "1" ]]; then
    echo "[refresh] note: refresh figures are single-rep; --reps=$REPS ignored (running 1)." >&2
fi

if [[ -z "$TAG" ]]; then
    DAY=$(date -u +%Y-%m-%d)
    SUFFIX=a
    while [[ -d "paper-data/${DAY}-refresh-${SUFFIX}" ]]; do
        SUFFIX=$(echo "$SUFFIX" | tr 'a-y' 'b-z')
    done
    TAG="${DAY}-refresh-${SUFFIX}"
fi

if [[ -n "$ROOT" ]]; then
    OUT_DIR="$ROOT"
else
    OUT_DIR="paper-data/${TAG}"
fi
RAW_DIR="${OUT_DIR}/raw"
SUMMARY_DIR="${OUT_DIR}/summary"
LOG="${OUT_DIR}/run.log"
mkdir -p "$RAW_DIR" "$SUMMARY_DIR"

log() {
    local ts; ts="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    printf '[%s] %s\n' "$ts" "$*" | tee -a "$LOG"
}

COMMIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
HOST=$(hostname -f 2>/dev/null || hostname)
log "tag=$TAG cells=$CELLS backends=$BACKENDS data_disk=$DATA_DISK out_dir=$OUT_DIR"
log "sf_btree=$SF_BTREE sf_lsm=$SF_LSM refresh_seconds=$REFRESH_SECONDS update_size=$UPDATE_SIZE dry_run=$DRY_RUN skip_load=$SKIP_LOAD"
log "commit_sha=$COMMIT_SHA host=$HOST repo=$REPO_ROOT"

# ---------------- cell table ----------------
# (mirror summarize_refresh_10L.py CELLS exactly)
cell_dram() {
    case "$1" in
        10LL) echo 20.0 ;;
        10L)  echo 1.0  ;;
        10H)  echo 0.5  ;;
        10HH) echo 0.2  ;;
        *)    echo "[refresh] unknown cell: $1" >&2; exit 1 ;;
    esac
}
cell_bg()       { [[ "$1" == 10LL ]] && echo 0 || echo 2 ; }
cell_backends() { [[ "$1" == 10LL ]] && echo "btree" || echo "btree lsm" ; }
cell_prewarm()  { [[ "$1" == 10LL ]] && echo 1 || echo 0 ; }

sf_for()  { [[ "$1" == btree ]] && echo "$SF_BTREE" || echo "$SF_LSM" ; }
# Per-Sx image layout (post-2026-05-28 refactor): structure N's image lives in
# tpch_<be>_S<n>/ (loaded by `make <that json>` with storage_structure=N).
img_dir() { echo "${DATA_DISK}/tpch_$1_S$2" ; }

# ---------------- helpers ----------------

drop_caches() {
    [[ $DRY_RUN -eq 1 ]] && { log "        (dry-run) sync; drop_caches"; return 0; }
    sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>/dev/null || true
}

# Build the per-Sx image for (backend, structure) if absent (unless
# --skip-load). The per-Sx json target loads only (persist), no query run;
# same image the headline runner's `make <binary>_<n>` produces, so they share.
declare -A LOAD_DONE
trigger_load() {
    local backend="$1" sf="$2" n="$3"
    local key="${backend}/${sf}/S${n}"
    [[ -n "${LOAD_DONE[$key]:-}" ]] && return "${LOAD_DONE[$key]}"
    local json="$(img_dir "$backend" "$n")/build/${sf}.json"
    if [[ $SKIP_LOAD -eq 1 || -f "$json" ]]; then
        [[ -f "$json" ]] && log "  load: present  $json" || log "  load: (skip-load) assuming $json"
        LOAD_DONE[$key]=0; return 0
    fi
    if [[ $DRY_RUN -eq 1 ]]; then
        log "  load: (dry-run) make $json scale=$sf data_disk=$DATA_DISK"
        LOAD_DONE[$key]=0; return 0
    fi
    log "  load: triggering per-Sx image S${n}  ($json)"
    if make "$json" scale="$sf" dram="0.1" data_disk="$DATA_DISK" >> "$LOG" 2>&1; then
        LOAD_DONE[$key]=0; return 0
    fi
    log "  ERROR: load failed for backend=$backend sf=$sf S${n}"
    LOAD_DONE[$key]=1; return 1
}

# Run one (cell, backend, structure) refresh: copy image -> drop caches ->
# timed RF1/RF2 -> collect CSV + stdout -> delete the copy.
run_one() {
    local cell="$1" be="$2" n="$3" dram="$4" bg="$5" prewarm="$6" sf="$7"
    local bin="${REPO_ROOT}/build/frontend/refresh_sales_${be}"
    local dir; dir="$(img_dir "$be" "$n")"
    local cell_raw="${RAW_DIR}/${cell}"
    local suf="rf${cell}_s${n}"
    mkdir -p "$cell_raw"

    # Ensure the per-Sx image for this structure exists (loads on demand).
    if ! trigger_load "$be" "$sf" "$n"; then
        log "        skip ${cell}/${be} S${n} (load unavailable)"
        return 0
    fi

    local bg_flags=""
    [[ "$bg" == 2 ]] && bg_flags="--bg_query_thread=true --bg_point_lookups=true"
    # Prewarm is LeanStore-specific and backfires on LSM (RUNS.md); btree only.
    local pw_flag=""
    [[ "$prewarm" == 1 && "$be" == btree ]] && pw_flag="--prewarm=true"

    local scratch cmd
    if [[ "$be" == btree ]]; then
        scratch="${dir}/${sf}.${suf}.image"
        cmd=("$bin" --recover --recover_file="${dir}/build/${sf}.json" \
             --ssd_path="$scratch" --trunc=false --wal=true \
             --dram_gib="$dram" --tpch_scale_factor="$sf" \
             --storage_structure="$n" --update_size="$UPDATE_SIZE" \
             --refresh_seconds="$REFRESH_SECONDS" $bg_flags $pw_flag)
    else
        scratch="${dir}/${sf}_${suf}"
        cmd=("$bin" --recover --ssd_path="$scratch" \
             --csv_path="${cell_raw}/lc${n}" \
             --dram_gib="$dram" --tpch_scale_factor="$sf" \
             --storage_structure="$n" --update_size="$UPDATE_SIZE" \
             --refresh_seconds="$REFRESH_SECONDS" $bg_flags)
    fi

    log "      ${cell}/${be} S${n} (dram=$dram bg=$bg sf=$sf)"
    if [[ $DRY_RUN -eq 1 ]]; then
        if [[ "$be" == btree ]]; then
            log "        cp ${dir}/${sf}.image $scratch"
        else
            log "        cp -r ${dir}/${sf} $scratch"
        fi
        drop_caches
        log "        ${cmd[*]} > ${cell_raw}/${be}_s${n}.out"
        log "        mv RefreshTPut.s${n}.csv ${cell_raw}/${be}.s${n}.csv ; rm scratch"
        return 0
    fi

    # Copy the canonical image to a per-structure scratch path (RF mutates it).
    if [[ "$be" == btree ]]; then
        cp "${dir}/${sf}.image" "$scratch"
    else
        cp -r "${dir}/${sf}" "$scratch"
    fi
    drop_caches
    ( cd "$cell_raw" && "${cmd[@]}" ) > "${cell_raw}/${be}_s${n}.out" 2>&1 || \
        log "        WARN: refresh_sales_${be} S${n} exited non-zero (see ${be}_s${n}.out)"
    if [[ -f "${cell_raw}/RefreshTPut.s${n}.csv" ]]; then
        mv -f "${cell_raw}/RefreshTPut.s${n}.csv" "${cell_raw}/${be}.s${n}.csv"
    else
        log "        WARN: no RefreshTPut.s${n}.csv produced for ${cell}/${be}"
    fi
    # Drop the per-structure copy (btree: file; lsm: dir).
    rm -rf "$scratch"
    local rf; rf=$(grep -o 'RF1=[0-9]* RF2=[0-9]*' "${cell_raw}/${be}_s${n}.out" 2>/dev/null | head -1)
    log "        done ${cell}/${be} S${n}: ${rf:-RF1=? RF2=?}"
}

# ---------------- main loop ----------------

IFS=',' read -ra CELL_LIST <<< "$CELLS"
for cell in "${CELL_LIST[@]}"; do
    dram=$(cell_dram "$cell")
    bg=$(cell_bg "$cell")
    prewarm=$(cell_prewarm "$cell")
    log "=== cell $cell (dram=${dram} GiB, bg=${bg}) ==="
    for be in $(cell_backends "$cell"); do
        want_backend "$be" || { log "  skip $be (not in --backends)"; continue; }
        sf=$(sf_for "$be")
        # Each structure loads its own per-Sx image on demand (see run_one).
        for n in 1 2 3 4; do
            run_one "$cell" "$be" "$n" "$dram" "$bg" "$prewarm" "$sf"
        done
    done
done

# ---------------- manifest + summary ----------------

cat > "${OUT_DIR}/manifest.yaml" <<EOF
tag: ${TAG}
experiment: refresh_sales (TPC-H RF1/RF2 update throughput) — Fig. 6 + 7
commit_sha: ${COMMIT_SHA}
host: ${HOST}
disk: ${DATA_DISK} (per-structure image copy; canonical read images untouched)
cells: ${CELLS}
backends: ${BACKENDS}
scale_factor: {btree: ${SF_BTREE}, lsm: ${SF_LSM}}
refresh_seconds: ${REFRESH_SECONDS}
update_size: ${UPDATE_SIZE}
structures: {1: split_S1, 2: view_S2, 3: merged_S3, 4: base_S4}
note: foreground RF loop on MAIN_WORKER; bg=2 cohort (Q3/Q5 reads + point lookups) on BG_WORKER. OS page cache dropped between copy and timed run. 10LL is btree-only, bg=0, prewarmed (DBToaster in-memory comparison).
EOF
log "manifest written: ${OUT_DIR}/manifest.yaml"

if [[ $DRY_RUN -eq 1 ]]; then
    log "dry-run: skipping summarizer"
    exit 0
fi

SUMMARIZER="${REPO_ROOT}/paper-data/scripts/summarize_refresh_10L.py"
if [[ -f "$SUMMARIZER" ]]; then
    log "running summarizer..."
    if python3 "$SUMMARIZER" --root "$OUT_DIR" --tag "$TAG" >> "$LOG" 2>&1; then
        log "summarizer: ok"
    else
        log "summarizer: FAILED (see $LOG)"
    fi
else
    log "summarizer not found at $SUMMARIZER; skipping summary CSVs"
fi

log "all done"
