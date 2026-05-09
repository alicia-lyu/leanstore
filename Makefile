# Makefile
# ——————————————————————————————————————————————————————————————————
# OS detection
UNAME := $(shell uname)

# Compiler / Build flags
CMAKE_DEBUG         := cmake -DCMAKE_BUILD_TYPE=Debug
CMAKE_RELWITHDEBINFO:= cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo
CMAKE_OPTIONS       := -DCMAKE_C_COMPILER=/usr/bin/clang \
                       -DCMAKE_CXX_COMPILER=/usr/bin/clang++

ifeq ($(UNAME),Darwin)
  NUMJOBS           ?= $(shell sysctl -n hw.ncpu)
else
  NUMJOBS           ?= $(shell nproc)
endif

# Data disk path (override with: make geo_lsm data_disk=/path/to/data)
ifeq ($(UNAME),Darwin)
  data_disk         := /tmp/leanstore
else
  data_disk         := /mnt/ssd
endif

# Build directories and executables
BUILD_DIR           := build
BUILD_DIR_DEBUG     := $(BUILD_DIR)-debug
BUILD_DIRS          := $(BUILD_DIR) $(BUILD_DIR_DEBUG)
EXEC_NAMES          := basic_join basic_group basic_group_variant

# Persistence format version, **per query family**. Each family gets
# its own subtree at $(data_disk)/<exec>/<<family>_format_version>/.
# Bump only the families whose record-type byte layout changed in a
# given commit; unchanged families keep their existing images and
# don't pay reload cost. The non-suffixed `format_version` is the
# default for any family that hasn't been overridden — useful for
# global "converge to wide secondaries" once we exit the per-query
# tuning phase.
# Tags: q3-q3i-stable-v0 (2026-05-08) — all families at v0 + tput
# wiring complete; the last commit before per-family format diverged.
format_version       ?= v0
geo_format_version   ?= $(format_version)
q12_format_version   ?= $(format_version)
q3_format_version    ?= $(format_version)
q3i_format_version   ?= $(format_version)

# Experiment flags
dram                	:= 0.1
scale 			    	:= 15
tentative_skip_bytes	:= 0 # do no tentative skip bytes
bgw_pct 		  		:= 0 # background write percentage

# Diagnostic flags (Q3I A1 / cross-backend perf attribution).
# Opt-in: pass `micro_perf=true cfstats=true` on the make command line.
# When false (the default), the production binaries skip the perf-counter
# capture path entirely.
micro_perf  ?= false
cfstats     ?= false
# A2c walker A/B: 'baseline' (std::variant + visitor) or 'fused_emit'
# (tag-byte switch + raw slices, no variant construction).
# Default 'fused_emit' post-A2c (q3i/PERFORMANCE.md §2 H4); pass
# coli_walker_variant=baseline to reproduce the regression A/B.
coli_walker_variant ?= fused_emit
# A3-Linux re-A/B: -1=use Backend trait (default), 0=force off, 1=force on.
use_seek_skip ?= -1

# A one‑off check we always do before building any binary
.PHONY: check_perf_event_paranoid
ifeq ($(UNAME),Darwin)
check_perf_event_paranoid:
	@echo "Skipping perf_event_paranoid check on macOS"
else
check_perf_event_paranoid:
	@ perf_par=$(shell sysctl -n kernel.perf_event_paranoid); \
	  if [ $$perf_par -gt 0 ]; then \
	    echo "Error: perf_event_paranoid is $$perf_par. Must be 0."; \
	    echo "Hint: sudo sysctl -w kernel.perf_event_paranoid=0"; \
	    exit 1; \
	  fi
endif

# When generate_targets.py changes, rebuild targets.mk
targets.mk: generate_targets.py
	python3 generate_targets.py > $@

# Include all of the repetitive rules
include targets.mk

temp_btree:
	-$(MAKE) geo_btree scale=15

temp_lsm:
	-$(MAKE) geo_lsm scale=40

temp:
	-$(MAKE) geo_btree_2 dram=0.1
	-$(MAKE) geo_btree_4 dram=0.1

# One-shot relocation of pre-versioning image dirs into v0/. Run once
# after upgrading to a Makefile that uses $(format_version) so existing
# images become reachable at the new versioned path. Idempotent: skips
# anything already moved. Lists v0 contents at the end.
.PHONY: migrate-format-v0
migrate-format-v0:
	@for exec in geo_btree geo_lsm q12_btree q12_lsm q3_btree q3_lsm q3i_btree q3i_lsm; do \
	    base=$(data_disk)/$$exec; \
	    [ -d $$base ] || continue; \
	    mkdir -p $$base/v0 $$base/v0/build $$base/v0/build-debug; \
	    for f in $$base/*.image $$base/*.image_temp; do \
	        [ -e "$$f" ] && mv "$$f" $$base/v0/ 2>/dev/null || true; \
	    done; \
	    for d in $$base/[0-9]*; do \
	        [ -d "$$d" ] || continue; \
	        mv "$$d" $$base/v0/ 2>/dev/null || true; \
	    done; \
	    for b in build build-debug; do \
	        for f in $$base/$$b/*.json; do \
	            [ -e "$$f" ] && mv "$$f" $$base/v0/$$b/ 2>/dev/null || true; \
	        done; \
	    done; \
	    echo "[migrated] $$base/v0 ->"; ls -la $$base/v0 2>/dev/null | tail -n +2 | head -10; \
	done

tmux:
	tmux new-session -s s1 || tmux attach-session -t s1

list-proc:
	@ps aux | grep "make"
