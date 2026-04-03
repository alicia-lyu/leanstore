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

# Experiment flags
dram                	:= 0.1
scale 			    	:= 15
tentative_skip_bytes	:= 0 # do no tentative skip bytes
bgw_pct 		  		:= 0 # background write percentage

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

tmux:
	tmux new-session -s s1 || tmux attach-session -t s1

list-proc:
	@ps aux | grep "make"
