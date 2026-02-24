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

# Shared runtime flags (from generate_targets.py shared_flags)
SHARED="--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
        --pp_threads=1 --csv_truncate=false --worker_threads=2 \
        --log_progress=false --tentative_skip_bytes=0 --bgw_pct=0"

run_engine() {
    local exe=$1 engine=$2 scale=$3 dram=${4:-0.1}
    local image_path recover_file runtime_dir

    if [[ $engine == "geo_lsm" ]]; then
        image_path=$DATA/$engine/$scale       # directory for LSM
        mkdir -p "$image_path"
    else
        image_path=$DATA/$engine/$scale.image # file for B-Tree
        mkdir -p "$DATA/$engine"
        touch "$image_path"
    fi

    recover_file=$DATA/$engine/${scale}.json
    runtime_dir=$RESULTS/$engine/${scale}-in-${dram}
    mkdir -p "$runtime_dir"

    # ── Load data (skip if recover file already exists) ───────────────────────
    if [[ ! -f "$recover_file" ]]; then
        echo "=== Loading data for $engine (scale=$scale) ==="
        $exe $SHARED \
            --recover_file=./leanstore.json \
            --persist_file="$recover_file" \
            --trunc=true \
            --ssd_path="$image_path" \
            --tpch_scale_factor=$scale \
            --dram_gib=8 \
            --csv_path="$runtime_dir"
    else
        echo "=== Recover file found, skipping load for $engine ==="
    fi

    # ── Run experiments for storage structures 0–4 ────────────────────────────
    for structure in 0 1 2 3 4; do
        echo "=== $engine structure=$structure (dram=${dram}GiB) ==="
        $exe $SHARED \
            --recover_file="$recover_file" \
            --persist_file=./leanstore.json \
            --trunc=false \
            --ssd_path="$image_path" \
            --tpch_scale_factor=$scale \
            --dram_gib=$dram \
            --csv_path="$runtime_dir" \
            --storage_structure=$structure \
            2>"$runtime_dir/structure${structure}_stderr.txt"
    done

    # Copy TPut.csv to /results top-level for easy access
    cp -f "$RESULTS/$engine/TPut.csv" "$RESULTS/${engine}_TPut.csv" 2>/dev/null || true
}

run_engine $BTREE_EXE geo_btree $SCALE_BTREE 0.1
run_engine $LSM_EXE   geo_lsm   $SCALE_LSM   0.1

echo "=== Done. Results in /results/ ==="
