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
# Env knobs (all optional):
#   SMOKE       1 = fast small-scale validation: tpch-headline/-hdd run the
#               smallest cell c2 (SF lsm 380 / btree 150, DRAM 0.1) at all four
#               structures, 1 rep; refresh runs its smallest cell (10HH) at the
#               same SFs. Use to verify build + code end-to-end with complete
#               (if small) figures.
#   REPS        Repetitions per (binary,cell,structure,bg) (default: 5; forced to
#               1 under SMOKE).
#   SF, DRAM_GIB, QUERIES   RESERVED — not yet forwarded to the runners. Use SMOKE
#               for a small run, or edit the cell table in run_paper_sweep.sh.
#
# Mount contract (bind-mounts are REQUIRED — the cell fails fast otherwise):
#   /results   every cell (output CSVs + paper-ready PDFs)
#   /mnt/ssd   tpch-headline, refresh (family images + per-structure copies)
#   /mnt/hdd   tpch-headline-hdd ONLY — must be rotational media (enforced)
# A separate physical disk is NOT required; any host directory works (your boot
# disk is fine), except /mnt/hdd which must be backed by a real HDD.
#
# Usage in docker_run.sh:
#   docker run -e CELL=tpch-headline \
#              -v /host/out:/results \
#              -v /host/fast-disk:/mnt/ssd \
#              ghcr.io/alicia-lyu/leanstore:vldb26
#   # HDD cell additionally needs a rotational disk:
#   docker run -e CELL=tpch-headline-hdd \
#              -v /host/out:/results -v /host/hdd:/mnt/hdd \
#              ghcr.io/alicia-lyu/leanstore:vldb26

set -euo pipefail

CELL="${CELL:-}"
REPS="${REPS:-5}"
SMOKE="${SMOKE:-0}"
RESULTS="/results"
REPO="/leanstore"
SCRIPTS="$REPO/paper-data/scripts"

log() { echo "[entrypoint] $*" >&2; }

# Small-scale validation when SMOKE=1: the smallest cell c2 (SF lsm 380 /
# btree 150, DRAM 0.1) at all four structures, single rep. Keeps the figures
# complete (S2-vs-S3 present) while staying fast — for build + code e2e checks.
SMOKE_ARGS=()
if [[ "$SMOKE" == 1 ]]; then
    SMOKE_ARGS+=(--cells c2 --sf-lsm "${SMOKE_SF:-15}" --sf-btree "${SMOKE_SF:-15}")
    REPS=1
fi

# ---------------------------------------------------------------------------
# Mount guards. The image ships /mnt/ssd, /mnt/hdd, /results as empty
# directories purely as bind-mount targets — the host provides the storage at
# `docker run -v <host>:<target>` time. If a path is NOT a real mount, writes
# would silently land in the container's ephemeral overlay (lost on exit; can
# fill the host root disk). So we FAIL FAST when an expected mount is missing.
#
# A separate physical disk is NOT required: any host directory works, including
# one on the reviewer's boot disk. The exception is the HDD cell, which
# measures rotational-media behavior — see require_hdd().
# ---------------------------------------------------------------------------
require_mount() {
    local p="$1"
    if mountpoint -q "$p" 2>/dev/null; then
        return 0
    fi
    echo "[entrypoint] ERROR: $p is not a mounted host path." >&2
    echo "[entrypoint]   Re-run with: -v /host/path:$p" >&2
    echo "[entrypoint]   Any host directory works — a separate physical disk is NOT" >&2
    echo "[entrypoint]   required; your boot disk is fine. Refusing to write to the" >&2
    echo "[entrypoint]   ephemeral container layer (data would be lost on exit)." >&2
    exit 1
}

# The tpch-headline-hdd cell is only meaningful on rotational media. Fail fast
# if /mnt/hdd is not mounted, and refuse to run if we can CONFIDENTLY determine
# the backing device is non-rotational (an SSD/NVMe mislabeled as HDD) — we do
# not produce SSD numbers wearing an "hdd" label. If the backing device cannot
# be classified (LVM/dm/overlay/network fs), we trust the reviewer's deliberate
# mount and proceed with a warning.
require_hdd() {
    require_mount /mnt/hdd
    local src dev base rot
    src=$(findmnt -no SOURCE --target /mnt/hdd 2>/dev/null) || { log "WARN: could not resolve /mnt/hdd backing device; trusting the mount."; return 0; }
    dev=${src##*/}
    [[ -z "$dev" ]] && { log "WARN: could not resolve /mnt/hdd backing device; trusting the mount."; return 0; }
    # lsblk exits 32 when the device node is absent (e.g. inside a container the
    # host block device isn't exposed); `|| true` keeps set -e from killing us so
    # we fall through to the "can't classify → trust the mount" path below.
    base=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1 || true)
    [[ -z "$base" ]] && base="$dev"
    if [[ ! -r "/sys/block/$base/queue/rotational" ]]; then
        log "WARN: cannot read rotational flag for /mnt/hdd (dev=$base); trusting the mount."
        return 0
    fi
    rot=$(cat "/sys/block/$base/queue/rotational" 2>/dev/null) || { log "WARN: could not read rotational flag for /mnt/hdd (dev=$base); trusting the mount."; return 0; }
    if [[ "$rot" == "0" ]]; then
        echo "[entrypoint] ERROR: /mnt/hdd is backed by non-rotational storage (rotational=0, dev=$base)." >&2
        echo "[entrypoint]   The tpch-headline-hdd figure measures HDD behavior; refusing to" >&2
        echo "[entrypoint]   produce SSD numbers mislabeled as HDD. Mount a rotational disk at" >&2
        echo "[entrypoint]   /mnt/hdd, or omit this cell." >&2
        exit 1
    fi
    log "/mnt/hdd backing device $base is rotational (rotational=1)."
}

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

# Helper: run run_refresh_sweep.sh (RF1/RF2 update throughput, single-rep)
# writing output directly to /results/<tag>/. Mirrors run_sweep's --root
# contract; reps are fixed at 1 (refresh figures are single-rep).
run_refresh() {
    local neutral_tag="$1"; shift
    local extra_args=("$@")

    local out_dir="$RESULTS/$neutral_tag"
    mkdir -p "$out_dir"

    "$REPO/experiments/run_refresh_sweep.sh" \
        --tag "$neutral_tag" \
        --root "$out_dir" \
        "${extra_args[@]}"

    log "refresh sweep done → $out_dir"
}

case "$CELL" in

    # -----------------------------------------------------------------------
    tpch-headline)
        # SSD headline: all TPC-H + TPC-Hi families, both backends.
        # Drives Fig. 4 (btree + lsm), Fig. 5 (q10), the supplementary
        # paper_tpch_vanilla panel, and SST diagnostics (Fig. diag_ssd_lsm_sst_path).
        # sstables.csv is captured by run_paper_sweep.sh for every LSM run.
        require_mount /mnt/ssd
        require_mount /results
        log "running SSD headline sweep..."
        run_sweep "tpch-headline" \
            --families tpch,tpchi \
            "${SMOKE_ARGS[@]}"
        ;;

    # -----------------------------------------------------------------------
    tpch-headline-hdd)
        # HDD LSM subset. Drives the supplementary tpch_lsm_headline_hdd figure.
        # Fails fast unless /mnt/hdd is a real mount on rotational media — we do
        # not produce SSD numbers mislabeled as HDD (see require_hdd).
        require_hdd
        require_mount /results
        log "running HDD headline sweep (LSM only)..."
        run_sweep "tpch-headline-hdd" \
            --families tpch,tpchi \
            --backends lsm \
            --disk hdd \
            "${SMOKE_ARGS[@]}"
        ;;

    # -----------------------------------------------------------------------
    refresh)
        # Refresh benchmark (Fig. 6 + 7): refresh_sales RF1/RF2 update throughput
        # across the 10-scale memory-pressure cells (10LL/10L/10H/10HH), both
        # backends, S1-S4. run_refresh_sweep.sh recovers each (backend,structure)
        # run from a per-structure copy of the canonical tpch family image, drops
        # OS page caches, and hands off to summarize_refresh_10L.py. Output lands
        # at /results/refresh/{manifest.yaml,raw/,summary/}.
        require_mount /mnt/ssd
        require_mount /results
        log "running refresh sweep..."
        if [[ "$SMOKE" == 1 ]]; then
            # Smallest refresh cell at the c2 SFs, so it reuses the family images
            # the headline cell already loaded (no extra load).
            run_refresh "refresh" --cells 10HH --sf-btree "${SMOKE_SF:-15}" --sf-lsm "${SMOKE_SF:-15}"
        else
            run_refresh "refresh"
        fi
        ;;

    # -----------------------------------------------------------------------
    dbtoaster)
        # Run the pre-built DBToaster refresh_sales binary and capture output.
        require_mount /results
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
        require_mount /results
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
paper_tpch_lsm_headline_hdd,refresh_lsm_vs_btree"

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
