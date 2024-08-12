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
ROCKSDB_MERGED_EXEC := /frontend/rocksdb_merged_tpcc

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
	lldb -- ./build-debug/frontend/merged_tpcc $(lldb_flags) --ssd_path=$(SSD_PATH) --csv_path=$(BUILD_DIR_DEBUG)/merged-lldb

rocksdb-join-lldb: $(BUILD_DIR_DEBUG)$(ROCKSDB_JOIN_EXEC)
	lldb -- ./build-debug/frontend/rocksdb_join_tpcc $(lldb_flags) --ssd_path=$(SSD_DIR) --csv_path=$(BUILD_DIR_DEBUG)/rocksdb-join-lldb

rocksdb-merged-lldb: $(BUILD_DIR_DEBUG)$(ROCKSDB_MERGED_EXEC)
	lldb -- ./build-debug/frontend/rocksdb_merged_tpcc $(lldb_flags) --ssd_path=$(SSD_DIR) --csv_path=$(BUILD_DIR_DEBUG)/rocksdb-merged-lldb

.PHONY: join-lldb

# ----------------- EXPERIMENTS -----------------
dram ?= $(default_dram)
target ?= $(default_target)
read ?= $(default_read)
scan ?= $(default_scan)
write ?= $(default_write)
update_size ?= $(default_update_size)
selectivity ?= $(default_selectivity)
duration ?= 0
locality_read ?= False

both: $(BUILD_DIR)$(JOIN_EXEC) $(BUILD_DIR)$(MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)$(MERGED_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)
	python3 experiment.py $(BUILD_DIR)$(JOIN_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)

rocksdb-both: $(BUILD_DIR)$(ROCKSDB_JOIN_EXEC) $(BUILD_DIR)$(ROCKSDB_MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)$(ROCKSDB_MERGED_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)
	python3 experiment.py $(BUILD_DIR)$(ROCKSDB_JOIN_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)

join: $(BUILD_DIR)$(JOIN_EXEC)
	python3 experiment.py $(BUILD_DIR)$(JOIN_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)

rocksdb-join: $(BUILD_DIR)$(ROCKSDB_JOIN_EXEC)
	python3 experiment.py $(BUILD_DIR)$(ROCKSDB_JOIN_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)

merged: $(BUILD_DIR)$(MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)$(MERGED_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)

rocksdb-merged: $(BUILD_DIR)$(ROCKSDB_MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)$(ROCKSDB_MERGED_EXEC) $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read)

%-read:
	$(MAKE) $* read=100 scan=0 write=0

%-locality-read:
	$(MAKE) $* read=100 scan=0 write=0 locality_read=True

%-scan:
	$(MAKE) $* read=0 scan=100 write=0

%-write:
	$(MAKE) $* read=0 scan=0 write=100
	
%-all-tx-types: %-read %-scan %-write
	@echo "Completed all transaction types for $*"

read: join-read merged-read

locality-read: join-locality-read merged-locality-read

scan: join-scan merged-scan

write: join-write merged-write

all-tx-types: read scan write locality-read

rocksdb-all-tx-types: rocksdb-join-all-tx-types rocksdb-merged-all-tx-types

all-locality-read: join-locality-read merged-locality-read rocksdb-join-locality-read rocksdb-merged-locality-read

update-size:
# $(MAKE) write update_size=5 # refer to write expriments
	$(MAKE) write update_size=10
	$(MAKE) write update_size=20

selectivity:
# Affects read, scan, and write
# for selectivity=100, refer to all-tx-types experiments
	$(MAKE) all-tx-types selectivity=50
	$(MAKE) all-tx-types selectivity=20
	$(MAKE) all-tx-types selectivity=10

no-columns:
	$(MAKE) all-tx-types included_columns=0

table-size:
	find . -regextype posix-extended -regex './build/(merged|join)-target$(target)g.*\.json' -exec rm {} \;
	rm -f "~/logs/join_size.csv"
	rm -f "~/logs/merged_size.csv"
	@for col in 0 1; do \
		for sel in 100 50 19; do \
			$(MAKE) both dram=16 selectivity=$$sel included_columns=$$col duration=1; \
		done \
	done

.PHONY: both read scan write all-tx-types update-size selectivity no-columns table-size

# ----------------- CLEAN -----------------
clean:
	rm -rf $(BUILD_DIRS)