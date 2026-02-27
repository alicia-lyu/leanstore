#!/usr/bin/env bash
set -euo pipefail

LEANSTORE=/leanstore
BUILD=$LEANSTORE/build
DATA=/data
RESULTS=/results

BTREE_EXE=$BUILD/frontend/geo_btree
LSM_EXE=$BUILD/frontend/geo_lsm

SCALE_BTREE=15
SCALE_LSM=40

SHARED="--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
        --pp_threads=1 --csv_truncate=false --worker_threads=2 \
        --log_progress=false --tentative_skip_bytes=0 --bgw_pct=0"

run_engine() {
    local exe=$1 engine=$2 scale=$3 dram=${4:-0.1}
    local image_path recover_file runtime_dir

    if [[ $engine == "geo_lsm" ]]; then
        image_path=$DATA/$engine/$scale
        mkdir -p "$image_path"
    else
        image_path=$DATA/$engine/$scale.image
        mkdir -p "$DATA/$engine"
        touch "$image_path"
    fi

    recover_file=$DATA/$engine/${scale}.json
    runtime_dir=$RESULTS/$engine/${scale}-in-${dram}
    mkdir -p "$runtime_dir"

    # ── Load data (skip if recover file already exists) ───────────────────────
    if [[ ! -f "$recover_file" ]]; then
        echo ""
        echo "=== [$(date '+%H:%M:%S')] Loading data for $engine (scale=$scale, dram_gib=8) ==="
        echo "    This step builds the database image and may take 10-30 minutes."
        echo "    Output: $recover_file"
        $exe $SHARED \
            --recover_file=./leanstore.json \
            --persist_file="$recover_file" \
            --trunc=true \
            --ssd_path="$image_path" \
            --tpch_scale_factor=$scale \
            --dram_gib=8 \
            --csv_path="$runtime_dir"
        echo "=== [$(date '+%H:%M:%S')] Load complete for $engine ==="
    else
        echo ""
        echo "=== [$(date '+%H:%M:%S')] Recover file found, skipping load for $engine ==="
        echo "    Using: $recover_file"
    fi

    # ── Run experiments for storage structures 1–4 ───────────────────────────
    for structure in 1 2 3 4; do
        echo ""
        echo "=== [$(date '+%H:%M:%S')] $engine structure=$structure (dram=${dram}GiB) ==="
        echo "    Each run takes 5--10 minutes. Running structure $structure of 4."
        $exe $SHARED \
            --recover_file="$recover_file" \
            --persist_file=./leanstore.json \
            --trunc=false \
            --ssd_path="$image_path" \
            --tpch_scale_factor=$scale \
            --dram_gib=$dram \
            --csv_path="$runtime_dir" \
            --storage_structure=$structure \
            2>"$runtime_dir/structure${structure}_stderr.txt" || true
        echo "=== [$(date '+%H:%M:%S')] structure=$structure done ==="
    done

    # Copy TPut.csv to /results for easy extraction
    mkdir -p "$RESULTS"
    cp -f "$RESULTS/$engine/TPut.csv" "$RESULTS/${engine}_TPut.csv" 2>/dev/null || true
}

echo "=========================================================="
echo "  LeanStore Geo Experiments"
echo "  $(date)"
echo "  Note: perf counters unavailable in container --"
echo "  CPU-cycle columns will be absent; TPut (TX/s) unaffected."
echo "=========================================================="

run_engine $BTREE_EXE geo_btree $SCALE_BTREE 0.1
run_engine $LSM_EXE   geo_lsm   $SCALE_LSM   0.1

echo ""
echo "=========================================================="
echo "  Done. Results in $RESULTS/"
echo "    geo_btree_TPut.csv"
echo "    geo_lsm_TPut.csv"
echo "  $(date)"
echo "=========================================================="
