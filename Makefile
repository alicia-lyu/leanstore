# ----------------- VARIABLES -----------------
default_read := 2
default_scan := 0
default_write := 98
default_dram := 1
default_target := 4
default_update_size := 5
default_selectivity := 100
included_columns ?= 1

CMAKE_DEBUG := cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO := cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
CMAKE_RELEASE := cmake -DCMAKE_BUILD_TYPE=Release ..

CMAKE_OPTIONS := -DCMAKE_CXX_FLAGS="${CMAKE_CXX_FLAGS} -DINCLUDE_COLUMNS=${included_columns}"

# ----------------- TARGETS -----------------
BUILD_DIR := ./build-$(included_columns)
BUILD_RELEASE_DIR := ./build-release-$(included_columns)
JOIN_EXEC := /frontend/join_tpcc
MERGED_EXEC := /frontend/merged_tpcc
BUILD_DIRS := $(BUILD_DIR) $(BUILD_RELEASE_DIR)
EXECS := $(JOIN_EXEC) $(MERGED_EXEC)

# Create Cartesian product for targets
TARGETS := $(foreach dir, $(BUILD_DIRS), $(foreach exec, $(EXECS), $(dir)$(exec)))

$(foreach dir, $(BUILD_DIRS), \
  $(foreach exec, $(EXECS), \
    $(eval $(dir)$(exec): DIR := $(dir)) \
    $(eval $(dir)$(exec): EXEC := $(exec)) \
    $(eval $(dir)$(exec): CMAKE := $(if $(findstring debug,$(dir)),$(CMAKE_DEBUG),$(if $(findstring release,$(dir)),$(CMAKE_RELEASE),$(CMAKE_RELWITHDEBINFO))) \
  ) \
))

PERF_PARANOID := $(shell sysctl -n kernel.perf_event_paranoid)

check_perf_event_paranoid:
	@if [ $(PERF_PARANOID) -gt 0 ]; then \
		echo "Error: kernel.perf_event_paranoid is set to $(PERF_PARANOID). Must be 0."; \
		echo "Hint: sudo sysctl -w kernel.perf_event_paranoid=0"; \
		exit 1; \
	fi

.PHONY: check_perf_event_paranoid

$(TARGETS): check_perf_event_paranoid
	mkdir -p $(DIR) && cd $(DIR) && $(CMAKE) $(CMAKE_OPTIONS) && $(MAKE) -j

executables: $(TARGETS)
.PHONY: executables

# ----------------- DEBUG -----------------
SSD_PATH := /home/alicia.w.lyu/tmp/image
CSV_PATH := ./build/log
lldb_flags := --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH) --tpcc_warehouse_count=2 --read_percentage=$(default_read) --scan_percentage=$(default_scan) --write_percentage=$(default_write) --order_size=10 --semijoin_selectivity=50

join-lldb: $(BUILD_DIR)$(JOIN_EXEC)
	lldb -- ./build/frontend/join_tpcc $(lldb_flags)

merged-lldb: $(BUILD_DIR)$(MERGED_EXEC)
	lldb -- ./build/frontend/merged_tpcc $(lldb_flags)

.PHONY: join-lldb

# ----------------- EXPERIMENTS -----------------
local_dram ?= $(default_dram)
local_target ?= $(default_target)
local_read ?= $(default_read)
local_scan ?= $(default_scan)
local_write ?= $(default_write)
local_update_size ?= $(default_update_size)
local_selectivity ?= $(default_selectivity)

both: $(BUILD_RELEASE_DIR)$(JOIN_EXEC) $(BUILD_RELEASE_DIR)$(MERGED_EXEC)
	./experiment.sh $(BUILD_RELEASE_DIR)$(JOIN_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns)
	./experiment.sh $(BUILD_RELEASE_DIR)$(MERGED_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns)

join: $(BUILD_RELEASE_DIR)$(JOIN_EXEC)
	./experiment.sh $(BUILD_RELEASE_DIR)$(JOIN_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns)

merged: $(BUILD_RELEASE_DIR)$(MERGED_EXEC)
	./experiment.sh $(BUILD_RELEASE_DIR)$(MERGED_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns)

read: 
	$(MAKE) both local_read=100 local_scan=0 local_write=0

scan:
	$(MAKE) both local_read=0 local_scan=100 local_write=0

write:
	$(MAKE) both local_read=0 local_scan=0 local_write=100

all-tx-types: read scan write

update-size:
# $(MAKE) write local_update_size=5 # refer to write expriments
	$(MAKE) write local_update_size=10
	$(MAKE) write local_update_size=20

selectivity:
# Affects read, scan, and write
# for selectivity=100, refer to all-tx-types experiments
	$(MAKE) all-tx-types local_selectivity=50
	$(MAKE) all-tx-types local_selectivity=10

no-columns:
	$(MAKE) all-tx-types included_columns=0

.PHONY: both read scan write all-tx-types update-size selectivity