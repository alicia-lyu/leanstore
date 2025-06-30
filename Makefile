# Makefile
# ——————————————————————————————————————————————————————————————————
# Compiler / Build flags
CMAKE_DEBUG         := cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO:= cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
CMAKE_OPTIONS       := -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} \
                       -DCMAKE_C_COMPILER=/usr/bin/clang \
                       -DCMAKE_CXX_COMPILER=/usr/bin/clang++
NUMJOBS             ?= $(shell nproc)

# Build directories and executables
BUILD_DIR           := build
BUILD_DIR_DEBUG     := $(BUILD_DIR)-debug
BUILD_DIRS          := $(BUILD_DIR) $(BUILD_DIR_DEBUG)
EXEC_NAMES          := basic_join basic_group basic_group_variant

# Experiment flags
dram                := 1

# A one‑off check we always do before building any binary
.PHONY: check_perf_event_paranoid
check_perf_event_paranoid:
	@ perf_par=$(shell sysctl -n kernel.perf_event_paranoid); \
	  if [ $$perf_par -gt 0 ]; then \
	    echo "Error: perf_event_paranoid is $$perf_par. Must be 0."; \
	    echo "Hint: sudo sysctl -w kernel.perf_event_paranoid=0"; \
	    exit 1; \
	  fi

# When generate_targets.py changes, rebuild targets.mk
targets.mk: generate_targets.py
	python3 generate_targets.py > $@

# Include all of the repetitive rules
include targets.mk

temp:
	-$(MAKE) geo_lsm dram=2
	-$(MAKE) geo_leanstore dram=0.6
	-$(MAKE) geo_lsm dram=1

tmux:
	tmux new-session -s s1 || tmux attach-session -t s1