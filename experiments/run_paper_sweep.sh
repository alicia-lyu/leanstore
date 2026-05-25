#!/usr/bin/env bash
#
# Paper-sweep runner.
#
# Drives the matrix from experiments/sweep.yaml against the existing
# make targets, snapshots per-run CSVs into paper-data/<tag>/raw/, and
# hands off to scripts/analyze_paper_sweep.py for the summary files.
#
# Output layout (matches PAPER_SWEEP.md §Output format):
#   paper-data/<tag>/
#     manifest.yaml
#     raw/<binary>/<cell>-bg<0|1>-r<R>/   (snapshot of build/<binary>/...)
#     summary/headline.csv
#     summary/diagnostics.csv             (best-effort; columns blank if absent)
#     summary/stats.csv
#     summary/inversions.csv
#
# Per-(family,backend,SF), the make-target dependency graph triggers
# the family load once and subsequent binary runs in that family
# recover from the same persisted .json.
#
# Cells run in this order so the cheapest one validates the pipeline
# first: c2 → c1 → c3 → c0 (small-beyond-memory → 1:5 → dram-pressured
# → anchor). c3 and c0 share SF, so c0's load is a no-op after c3.

set -uo pipefail

usage() {
    cat <<EOF
Usage: $(basename "$0") [--tag <tag>] [--cells c2,c1,c3,c0]
                       [--families tpch,tpchi,geo] [--reps N]
                       [--smoke-test] [--dry-run] [--skip-load]
                       [--continue]

  --tag         Sweep tag (default: YYYY-MM-DD-a, auto-incremented).
  --cells       Comma-separated cell tags in run order
                (default c2,c1,c3,c0).
  --families    Comma-separated family tags (tpch,tpchi,geo).
                Default: all three.
  --reps        Override repetitions per (binary,cell,structure,bg).
                Default: 3 (from sweep.yaml).
  --smoke-test  One cell (c2), one binary per family, structures 1+3,
                bg0 only, 1 rep. ~5-10 min end-to-end validation.
  --dry-run     Print the commands; don't execute.
  --skip-load   Assume the family .json images already exist.
  --drop-caches Drop the OS page cache (sync; echo 3 > drop_caches, needs
                sudo) before each per-structure run for a cold start.
  --continue    Re-use the latest existing tag instead of creating a
                new one; skip per-run snapshots that already exist.
  --rotate-params  Pass param_seed=<rep> (identical across the four
                structures in each rep) so the reps sample distinct
                substitution parameters. Off by default (param_seed=0,
                the historical single-validation-param behaviour).

Tag dir layout:
  paper-data/<tag>/{manifest.yaml,run.log,raw/,summary/}
EOF
}

TAG=""
CELLS="c2,c1,c3,c0"
FAMILIES="tpch,tpchi,geo"
REPS=""
DRY_RUN=0
SKIP_LOAD=0
SMOKE_TEST=0
CONTINUE=0
DROP_CACHES=0
ROTATE_PARAMS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)        TAG="$2"; shift 2 ;;
        --cells)      CELLS="$2"; shift 2 ;;
        --families)   FAMILIES="$2"; shift 2 ;;
        --reps)       REPS="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=1; shift ;;
        --skip-load)  SKIP_LOAD=1; shift ;;
        --smoke-test) SMOKE_TEST=1; shift ;;
        --drop-caches) DROP_CACHES=1; shift ;;
        --continue)   CONTINUE=1; shift ;;
        --rotate-params) ROTATE_PARAMS=1; shift ;;
        -h|--help)    usage; exit 0 ;;
        *)            echo "unknown arg: $1" >&2; usage; exit 1 ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ $SMOKE_TEST -eq 1 ]]; then
    CELLS="c2"
    REPS=1
fi
[[ -z "$REPS" ]] && REPS=3

if [[ $CONTINUE -eq 1 && -z "$TAG" ]]; then
    DAY=$(date -u +%Y-%m-%d)
    # Pick the highest existing suffix for today.
    LATEST=""
    for d in paper-data/${DAY}-*; do
        [[ -d "$d" ]] && LATEST="$d"
    done
    if [[ -n "$LATEST" ]]; then
        TAG="${LATEST#paper-data/}"
    fi
fi
if [[ -z "$TAG" ]]; then
    DAY=$(date -u +%Y-%m-%d)
    SUFFIX=a
    while [[ -d "paper-data/${DAY}-${SUFFIX}" ]]; do
        SUFFIX=$(echo "$SUFFIX" | tr 'a-y' 'b-z')
    done
    TAG="${DAY}-${SUFFIX}"
fi

OUT_DIR="paper-data/${TAG}"
RAW_DIR="${OUT_DIR}/raw"
SUMMARY_DIR="${OUT_DIR}/summary"
LOG="${OUT_DIR}/run.log"
mkdir -p "$RAW_DIR" "$SUMMARY_DIR"

log() {
    local ts; ts="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    printf '[%s] %s\n' "$ts" "$*" | tee -a "$LOG"
}

run() {
    log "RUN: $*"
    if [[ $DRY_RUN -eq 1 ]]; then
        return 0
    fi
    eval "$@"
}

COMMIT_SHA=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
HOST=$(hostname)
log "tag=$TAG cells=$CELLS families=$FAMILIES reps=$REPS dry_run=$DRY_RUN skip_load=$SKIP_LOAD smoke_test=$SMOKE_TEST"
log "commit_sha=$COMMIT_SHA host=$HOST repo=$REPO_ROOT"

# ---------------- cell lookup table ----------------
# (cell, sf_lsm, sf_btree, sf_geo_lsm, sf_geo_btree, dram_gib)
cell_field() {
    local cell="$1" field="$2"
    case "$cell" in
        c0) local d=1.0; local s_lsm=3850; local s_btree=1550; local g_lsm=515; local g_btree=194 ;;
        c1) local d=0.4; local s_lsm=1500; local s_btree=620;  local g_lsm=206; local g_btree=78  ;;
        c2) local d=0.1; local s_lsm=380;  local s_btree=150;  local g_lsm=52;  local g_btree=19  ;;
        c3) local d=0.4; local s_lsm=3850; local s_btree=1550; local g_lsm=515; local g_btree=194 ;;
        *)  echo "[runner] unknown cell: $cell" >&2; exit 1 ;;
    esac
    case "$field" in
        dram)        echo "$d" ;;
        sf_lsm)      echo "$s_lsm" ;;
        sf_btree)    echo "$s_btree" ;;
        sf_geo_lsm)  echo "$g_lsm" ;;
        sf_geo_btree) echo "$g_btree" ;;
    esac
}

binary_sf() {
    local binary="$1" cell="$2"
    case "$binary" in
        geo_lsm)    cell_field "$cell" sf_geo_lsm ;;
        geo_btree)  cell_field "$cell" sf_geo_btree ;;
        *_lsm)      cell_field "$cell" sf_lsm ;;
        *_btree)    cell_field "$cell" sf_btree ;;
    esac
}

# Family configuration.
families_for() {
    local f="$1"
    case "$f" in
        tpch)  echo "q3_lsm q3_btree q5_lsm q5_btree" ;;
        tpchi) echo "q3i_lsm q3i_btree q5i_lsm q5i_btree" ;;
        geo)   echo "geo_lsm geo_btree" ;;
        *)     echo ""; return 1 ;;
    esac
}

structures_for() {
    local binary="$1"
    if [[ $SMOKE_TEST -eq 1 ]]; then
        echo "1 3"
    else
        echo "1 2 3 4"
    fi
}

bg_for() {
    # Sweep -b matrix collapses to bg=2 only: heterogeneous cohort
    # (Q3+Q5+point_lookups, time-balanced) for TPC-H binaries; single
    # contention thread for geo. bg=0 (isolated) and bg=1 (same-family
    # query thread only) are dropped — they don't reflect the realistic
    # storage-shared-by-queries shape the paper targets.
    if [[ $SMOKE_TEST -eq 1 ]]; then
        echo "2"
    else
        echo "2"
    fi
}

# ---------------- helpers ----------------

# Returns the make-flag assignments for a given (binary, bg) tuple.
# - bg=0: no background activity.
# - bg=1: same-family query thread only (no point lookups).
# - bg=2: same-family query thread + cross-table point lookups (TPC-H only;
#         geo collapses to geo_bg_thread=true since geo has no point-lookup
#         analog yet).
bg_make_flags() {
    local binary="$1" bg="$2"
    if [[ "$binary" == geo_* ]]; then
        # Geo only has one bg knob.
        case "$bg" in
            0) echo "geo_bg_thread=false" ;;
            1|2) echo "geo_bg_thread=true" ;;
        esac
        return
    fi
    case "$bg" in
        0) echo "bg_query_thread=false bg_point_lookups=false" ;;
        1) echo "bg_query_thread=true bg_point_lookups=false" ;;
        2) echo "bg_query_thread=true bg_point_lookups=true" ;;
    esac
}

family_dep_target() {
    # For TPC-H families, the family load is keyed on the image dir of any
    # member binary (they share the dir via TPCH_FAMILY in
    # generate_targets.py). Use the first member as the load entrypoint.
    local family="$1" backend="$2"
    case "$family" in
        tpch)  [[ "$backend" == lsm ]] && echo "q3_lsm" || echo "q3_btree" ;;
        tpchi) [[ "$backend" == lsm ]] && echo "q3i_lsm" || echo "q3i_btree" ;;
        geo)   echo "geo_$backend" ;;
    esac
}

trigger_load() {
    # Trigger the family load by depending on the recover .json. For TPC-H
    # we leverage make's own dep graph: `make <binary>_1 scale=... dram=...`
    # would also load + run; here we just touch the json target so we don't
    # waste a structure run.
    local family="$1" backend="$2" sf="$3"
    [[ $SKIP_LOAD -eq 1 ]] && return 0
    local data_disk="/mnt/ssd"
    case "$family" in
        tpch)  local img_dir="tpch_${backend}" ;;
        tpchi) local img_dir="tpchi_${backend}" ;;
        geo)   local img_dir="geo_${backend}" ;;
    esac
    local json="${data_disk}/${img_dir}/build/${sf}.json"
    if [[ -f "$json" ]]; then
        log "  load: already present  $json"
        return 0
    fi
    log "  load: triggering for family=$family backend=$backend sf=$sf  ($json)"
    if [[ $DRY_RUN -eq 1 ]]; then
        log "  load: (dry-run) make $json scale=$sf"
        return 0
    fi
    # Use the recover-file target directly. dram is irrelevant for the
    # load (load_dram=8 is pinned inside the recipe); pass it just to
    # satisfy the Makefile.
    if ! make "$json" scale="$sf" dram="0.1" >> "$LOG" 2>&1; then
        log "  ERROR: load failed for family=$family backend=$backend sf=$sf"
        return 1
    fi
    log "  load: complete  $json"
}

snapshot_run_dir() {
    # The binary's logger splits output across two dirs:
    #   csv_db      = build/<binary>/        (TPut.csv, size.csv, sstables.csv)
    #   csv_runtime = build/<binary>/<sf>-in-<dram>/
    #                                        (structure logs, per-tx/per-method
    #                                         detail CSVs for LeanStore)
    # Snapshot both into paper-data/<tag>/raw/<binary>/<cell>-bg<bg>-r<rep>/.
    local binary="$1" sf="$2" dram="$3" dest="$4"
    local csv_db="build/${binary}"
    local csv_runtime="build/${binary}/${sf}-in-${dram}"
    mkdir -p "$dest"
    if [[ -d "$csv_runtime" ]]; then
        cp -a "$csv_runtime/." "$dest/"
    else
        log "  WARN: csv_runtime missing for snapshot: $csv_runtime"
    fi
    for f in TPut.csv size.csv Elapsed.csv sstables.csv; do
        [[ -f "$csv_db/$f" ]] && cp -f "$csv_db/$f" "$dest/$f"
    done
}

# Slice the most-recently-appended rows of a CSV (since prev_count rows) into
# a per-structure file, preserving the header. Used so the analyzer can map
# TPut.csv rows back to the structure that produced them.
slice_csv_since() {
    local src_csv="$1" dest_csv="$2" prev_count="$3"
    [[ ! -f "$src_csv" ]] && return 0
    local total
    total=$(wc -l < "$src_csv")
    # head -1 = header, body rows = total - 1
    local body=$((total - 1))
    if (( body <= prev_count )); then
        return 0
    fi
    # Write header from src + body rows after prev_count
    head -n 1 "$src_csv" > "$dest_csv"
    tail -n +$((prev_count + 2)) "$src_csv" >> "$dest_csv"
}

# After a single structure run, slice the new rows in TPut/Elapsed/size CSVs
# (which live at the csv_db level) into per-structure suffix files dropped
# into csv_runtime so the snapshot is self-describing.
mark_structure_slice() {
    local binary="$1" sf="$2" dram="$3" structure="$4" prev_tput="$5"
    local csv_db="build/${binary}"
    local csv_runtime="build/${binary}/${sf}-in-${dram}"
    mkdir -p "$csv_runtime"
    slice_csv_since "$csv_db/TPut.csv"    "$csv_runtime/TPut.s${structure}.csv"    "$prev_tput"
    slice_csv_since "$csv_db/size.csv"    "$csv_runtime/size.s${structure}.csv"    "$prev_tput"
    slice_csv_since "$csv_db/Elapsed.csv" "$csv_runtime/Elapsed.s${structure}.csv" "$prev_tput"
}

current_body_count() {
    local csv="$1"
    [[ ! -f "$csv" ]] && echo 0 && return
    local total
    total=$(wc -l < "$csv")
    echo $((total - 1))
}

reset_run_dir() {
    # Wipe per-run CSVs at both csv_db (binary parent) and csv_runtime
    # (<sf>-in-<dram>) so the next rep starts clean. Load-only artifacts
    # (load.log etc.) inside csv_runtime are not part of the per-rep
    # snapshot, but removing them is harmless because the load won't
    # re-run after the json image exists.
    local binary="$1" sf="$2" dram="$3"
    local csv_db="build/${binary}"
    local csv_runtime="build/${binary}/${sf}-in-${dram}"
    if [[ -d "$csv_db" ]]; then
        rm -f "$csv_db/TPut.csv" "$csv_db/Elapsed.csv" "$csv_db/size.csv" \
              "$csv_db/sstables.csv"
    fi
    if [[ -d "$csv_runtime" ]]; then
        rm -f "$csv_runtime"/structure*.log "$csv_runtime"/structure*_stderr.txt
        rm -f "$csv_runtime"/TPut.s*.csv "$csv_runtime"/size.s*.csv \
              "$csv_runtime"/Elapsed.s*.csv
        # Per-tx/per-method detail CSV trees (LeanStore B-tree).
        find "$csv_runtime" -mindepth 1 -maxdepth 2 -type d \
            -exec rm -rf {} + 2>/dev/null || true
    fi
}

# ---------------- main loop ----------------

declare -A LOAD_DONE
declare -i RUN_OK=0 RUN_ERR=0

IFS=',' read -ra CELL_LIST <<< "$CELLS"
IFS=',' read -ra FAM_LIST <<< "$FAMILIES"

for cell in "${CELL_LIST[@]}"; do
    dram=$(cell_field "$cell" dram)
    log "=== cell $cell  (dram=${dram} GiB) ==="
    for family in "${FAM_LIST[@]}"; do
        binaries=$(families_for "$family") || continue
        log "  family $family"
        # Trigger one load per backend per family per SF.
        for backend in lsm btree; do
            sample_binary=$(family_dep_target "$family" "$backend")
            sf=$(binary_sf "$sample_binary" "$cell")
            load_key="${family}/${backend}/${sf}"
            if [[ -z "${LOAD_DONE[$load_key]:-}" ]]; then
                if trigger_load "$family" "$backend" "$sf"; then
                    LOAD_DONE[$load_key]=1
                else
                    log "    skipping family=$family backend=$backend (load failed)"
                    LOAD_DONE[$load_key]=0
                fi
            fi
        done
        for binary in $binaries; do
            # Determine backend from binary name suffix.
            backend="lsm"
            [[ "$binary" == *_btree ]] && backend="btree"
            sf=$(binary_sf "$binary" "$cell")
            load_key="${family}/${backend}/${sf}"
            if [[ "${LOAD_DONE[$load_key]:-0}" == "0" ]]; then
                log "    skipping $binary (load failed)"
                continue
            fi
            log "    binary $binary  sf=$sf"
            for bg in $(bg_for); do
                bg_flags=$(bg_make_flags "$binary" "$bg")
                for rep in $(seq 1 "$REPS"); do
                    dest="${RAW_DIR}/${binary}/${cell}-bg${bg}-r${rep}"
                    if [[ $CONTINUE -eq 1 && -f "$dest/TPut.csv" ]]; then
                        log "      skip existing $dest"
                        continue
                    fi
                    log "      rep $rep / bg=$bg  ($bg_flags)"
                    reset_run_dir "$binary" "$sf" "$dram"
                    csv_db_dir="build/${binary}"
                    # Param-rotation: when --rotate-params is set, every
                    # structure in THIS rep gets the SAME seed (=$rep), so
                    # the four structures stay comparable while the reps
                    # sample distinct PARAM_TABLE entries. Default (off) =
                    # param_seed=0 for every run, i.e. the historical
                    # single-validation-param behaviour.
                    if [[ $ROTATE_PARAMS -eq 1 ]]; then
                        param_seed_flag="param_seed=$rep"
                    else
                        param_seed_flag="param_seed=0"
                    fi
                    for n in $(structures_for "$binary"); do
                        target="${binary}_${n}"
                        cmd="make $target scale=$sf dram=$dram $bg_flags $param_seed_flag"
                        log "        $cmd"
                        if [[ $DRY_RUN -eq 1 ]]; then
                            continue
                        fi
                        prev_tput=$(current_body_count "$csv_db_dir/TPut.csv")
                        # Cold-start each run: both engines use O_DIRECT so the
                        # OS page cache does not serve reads, but dropping it
                        # guarantees no buffered-IO/metadata warmth carries over.
                        if [[ $DROP_CACHES -eq 1 ]]; then
                            sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' 2>>"$LOG" || true
                        fi
                        if ! eval "$cmd" >> "$LOG" 2>&1; then
                            log "        ERROR: $cmd failed; continuing"
                            RUN_ERR=$((RUN_ERR + 1))
                            continue
                        fi
                        mark_structure_slice "$binary" "$sf" "$dram" "$n" "$prev_tput"
                        RUN_OK=$((RUN_OK + 1))
                    done
                    if [[ $DRY_RUN -eq 0 ]]; then
                        snapshot_run_dir "$binary" "$sf" "$dram" "$dest"
                    fi
                done
            done
        done
    done
done

log "done. runs ok=$RUN_OK err=$RUN_ERR"

# ---------------- manifest + summary ----------------

if [[ $DRY_RUN -eq 1 ]]; then
    log "dry-run: skipping manifest + analyzer"
    exit 0
fi

cat > "${OUT_DIR}/manifest.yaml" <<EOF
tag: ${TAG}
commit_sha: ${COMMIT_SHA}
host: ${HOST}
started_at: $(head -1 "$LOG" | awk '{print $1}' | tr -d '[]')
finished_at: $(date -u +'%Y-%m-%dT%H:%M:%SZ')
cells: ${CELLS}
families: ${FAMILIES}
reps: ${REPS}
runs_ok: ${RUN_OK}
runs_err: ${RUN_ERR}
EOF

log "manifest written: ${OUT_DIR}/manifest.yaml"

# Run analyzer over the snapshot tree.
ANALYZER="${REPO_ROOT}/scripts/analyze_paper_sweep.py"
if [[ -x "$ANALYZER" || -f "$ANALYZER" ]]; then
    log "running analyzer..."
    if python3 "$ANALYZER" --tag "$TAG" --root "$OUT_DIR" >> "$LOG" 2>&1; then
        log "analyzer: ok"
    else
        log "analyzer: FAILED (see $LOG)"
    fi
else
    log "analyzer not found at $ANALYZER; skipping summary CSV emission"
fi

# Run plotter — emits paper-data/<tag>/figures/{...}.{pdf,png}.
# See scripts/PLOTTING.md for the figure catalog.
PLOTTER="${REPO_ROOT}/scripts/plot_paper_sweep.py"
if [[ -x "$PLOTTER" || -f "$PLOTTER" ]]; then
    log "running plotter..."
    if python3 "$PLOTTER" --tag "$TAG" --root "$OUT_DIR" >> "$LOG" 2>&1; then
        log "plotter: ok"
    else
        log "plotter: FAILED (see $LOG) — non-fatal, summary CSVs still valid"
    fi
else
    log "plotter not found at $PLOTTER; skipping figure emission"
fi

log "all done"
