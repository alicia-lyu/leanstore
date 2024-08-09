# ----------------- VARIABLES -----------------
default_read := 98
default_scan := 2
default_write := 0
default_dram := 1
default_target := 4
default_update_size := 5
default_selectivity := 100
included_columns ?= 1

CMAKE_DEBUG := cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO := cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..

CMAKE_OPTIONS := -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DINCLUDED_COLUMNS=${included_columns}

# ----------------- TARGETS -----------------
BUILD_DIR := ./build
BUILD_DIR_DEBUG := $(BUILD_DIR)-debug
BUILD_DIRS := $(BUILD_DIR) $(BUILD_DIR_DEBUG)

JOIN_EXEC := /frontend/join_tpcc
MERGED_EXEC := /frontend/merged_tpcc
ROCKSDB_JOIN_EXEC := /frontend/rocksdb_join_tpcc

EXECS := $(JOIN_EXEC) $(MERGED_EXEC) $(ROCKSDB_JOIN_EXEC)

# Create Cartesian product for targets
TARGETS := $(foreach dir, $(BUILD_DIRS), $(foreach exec, $(EXECS), $(dir)$(exec)))

$(foreach dir, $(BUILD_DIRS), \
  $(foreach exec, $(EXECS), \
    $(eval $(dir)$(exec): DIR := $(dir)) \
    $(eval $(dir)$(exec): EXEC := $(exec)) \
    $(eval $(dir)$(exec): CMAKE := $(if $(findstring debug,$(dir)),$(CMAKE_DEBUG),$(CMAKE_RELWITHDEBINFO)) \
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
SSD_DIR := /home/alicia.w.lyu/tmp/image_dir
lldb_flags := --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --tpcc_warehouse_count=2 --read_percentage=98 --scan_percentage=0 --write_percentage=2 --order_size=10 --semijoin_selectivity=50 --csv_truncate=true --worker_threads=2

join-lldb: $(BUILD_DIR_DEBUG)$(JOIN_EXEC)
	lldb -- ./build-debug/frontend/join_tpcc $(lldb_flags) --ssd_path=$(SSD_PATH) --csv_path=$(BUILD_DIR_DEBUG)/join-lldb

merged-lldb: $(BUILD_DIR_DEBUG)$(MERGED_EXEC)
	lldb -- ./build-debug/frontend/merged_tpcc $(lldb_flags) --ssd_path=$(SSD_PATH)
	--csv_path=$(BUILD_DIR_DEBUG)/merged-lldb

rocksdb-join-lldb: $(BUILD_DIR_DEBUG)$(ROCKSDB_JOIN_EXEC)
	lldb -- ./build-debug/frontend/rocksdb_join_tpcc $(lldb_flags) --ssd_path=$(SSD_DIR) --csv_path=$(BUILD_DIR_DEBUG)/rocksdb-join-lldb

rocksdb-merged-lldb: $(BUILD_DIR_DEBUG)$(ROCKSDB_JOIN_EXEC)
	lldb -- ./build-debug/frontend/rocksdb_merged_tpcc $(lldb_flags) --ssd_path=$(SSD_DIR) --csv_path=$(BUILD_DIR_DEBUG)/rocksdb-merged-lldb

.PHONY: join-lldb

# ----------------- EXPERIMENTS -----------------
local_dram ?= $(default_dram)
local_target ?= $(default_target)
local_read ?= $(default_read)
local_scan ?= $(default_scan)
local_write ?= $(default_write)
local_update_size ?= $(default_update_size)
local_selectivity ?= $(default_selectivity)
extra_args ?= ""

both: $(BUILD_DIR)$(JOIN_EXEC) $(BUILD_DIR)$(MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)$(JOIN_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns) $(extra_args)
	python3 experiment.py $(BUILD_DIR)$(MERGED_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns) $(extra_args)

join: $(BUILD_DIR)$(JOIN_EXEC)
	python3 experiment.py $(BUILD_DIR)$(JOIN_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns) $(extra_args)

merged: $(BUILD_DIR)$(MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)$(MERGED_EXEC) $(local_dram) $(local_target) $(local_read) $(local_scan) $(local_write) $(local_update_size) $(local_selectivity) $(included_columns) $(extra_args)

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

table-size:
	find . -regextype posix-extended -regex './build/(merged|join)-target$(local_target)g.*\.json' -exec rm {} \;
	rm -f "~/logs/join_size.csv"
	rm -f "~/logs/merged_size.csv"
	@for col in 0 1; do \
		for sel in 100 50 20 10; do \
			$(MAKE) both local_dram=12 local_selectivity=$$sel included_columns=$$col extra_args=1; \
		done \
	done

.PHONY: both read scan write all-tx-types update-size selectivity

# ----------------- CLEAN -----------------
clean:
	rm -rf $(BUILD_DIRS)