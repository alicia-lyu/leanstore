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

CMAKE_OPTIONS := -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DINCLUDED_COLUMNS=${included_columns} -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++

# ----------------- TARGETS -----------------
BUILD_DIR := ./build
BUILD_DIR_DEBUG := $(BUILD_DIR)-debug
BUILD_DIRS := $(BUILD_DIR) $(BUILD_DIR_DEBUG)

JOIN_EXEC := join_tpcc
MERGED_EXEC := merged_tpcc
BASE_EXEC := base_tpcc
ROCKSDB_JOIN_EXEC := rocksdb_join_tpcc
ROCKSDB_MERGED_EXEC := rocksdb_merged_tpcc
ROCKSDB_BASE_EXEC := rocksdb_base_tpcc

EXECS := $(JOIN_EXEC) $(MERGED_EXEC) $(ROCKSDB_JOIN_EXEC) $(ROCKSDB_MERGED_EXEC) $(BASE_EXEC) $(ROCKSDB_BASE_EXEC)

# Create Cartesian product for targets
TARGETS := $(foreach dir, $(BUILD_DIRS), $(foreach exec, $(EXECS), $(dir)/frontend/$(exec)))

$(foreach dir, $(BUILD_DIRS), \
  $(foreach exec, $(EXECS), \
    $(eval $(dir)/frontend/$(exec): DIR := $(dir)) \
    $(eval $(dir)/frontend/$(exec): EXEC := $(exec)) \
    $(eval $(dir)/frontend/$(exec): CMAKE := $(if $(findstring debug,$(dir)),$(CMAKE_DEBUG),$(CMAKE_RELWITHDEBINFO)) \
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
	mkdir -p $(DIR) && cd $(DIR) && $(CMAKE) $(CMAKE_OPTIONS) && $(MAKE) $(EXEC) -j

all-executables: $(TARGETS)

executables: $(TARGETS)
.PHONY: executables

# ----------------- DEBUG -----------------
SSD_PATH := /home/alicia.w.lyu/tmp/image
SSD_DIR := /home/alicia.w.lyu/tmp/image_dir
lldb_flags := --dram_gib=1 --vi=false --mv=false --isolation_level=ser --optimistic_scan=false --tpcc_warehouse_count=4 --read_percentage=98 --scan_percentage=0 --write_percentage=2 --pp_threads=2 --order_size=10 --semijoin_selectivity=19 --csv_truncate=true --worker_threads=4 --locality_read=true --trunc=true --ssd_path=$(SSD_PATH)

LLDB_TARGETS := $(foreach exec, $(EXECS), $(patsubst %_tpcc,%-lldb,$(exec)))

lldb ?= true

$(foreach lldb_target, $(LLDB_TARGETS), \
	$(eval $(lldb_target): EXEC := $(patsubst %-lldb,%_tpcc,$(lldb_target))) \
  	$(eval $(lldb_target): CSV := $(BUILD_DIR_DEBUG)/$(lldb_target)) \
)

ifeq ($(lldb), true)
$(LLDB_TARGETS):
	lldb -- $(BUILD_DIR_DEBUG)/frontend/$(EXEC) $(lldb_flags) --csv_path=$(CSV)
else
$(LLDB_TARGETS):
	$(BUILD_DIR_DEBUG)/frontend/$(EXEC) $(lldb_flags) --csv_path=$(CSV)
endif

run-plain-targets:
	$(MAKE) -j1 $(LLDB_TARGETS) lldb=false

.PHONY: $(LLDB_TARGETS) run-plain-targets

# ----------------- EXPERIMENTS -----------------
dram ?= $(default_dram)
target ?= $(default_target)
read ?= $(default_read)
scan ?= $(default_scan)
write ?= $(default_write)
update_size ?= $(default_update_size)
selectivity ?= $(default_selectivity)
duration ?= 0
locality_read ?= ""
outer_join ?= ""

PY_FLAGS := $(dram) $(target) $(read) $(scan) $(write) $(update_size) $(selectivity) $(included_columns) $(duration) $(locality_read) $(outer_join)

leanstore: join merged base

rocksdb: rocksdb-merged rocksdb-base rocksdb-join 

join: $(BUILD_DIR)/frontend/$(JOIN_EXEC)
	python3 experiment.py $(BUILD_DIR)/frontend/$(JOIN_EXEC) $(PY_FLAGS)

rocksdb-join: $(BUILD_DIR)/frontend/$(ROCKSDB_JOIN_EXEC)
	python3 experiment.py $(BUILD_DIR)/frontend/$(ROCKSDB_JOIN_EXEC) $(PY_FLAGS)

merged: $(BUILD_DIR)/frontend/$(MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)/frontend/$(MERGED_EXEC) $(PY_FLAGS)

rocksdb-merged: $(BUILD_DIR)/frontend/$(ROCKSDB_MERGED_EXEC)
	python3 experiment.py $(BUILD_DIR)/frontend/$(ROCKSDB_MERGED_EXEC) $(PY_FLAGS)

base: $(BUILD_DIR)/frontend/$(BASE_EXEC)
	python3 experiment.py $(BUILD_DIR)/frontend/$(BASE_EXEC) $(PY_FLAGS)

rocksdb-base: $(BUILD_DIR)/frontend/$(ROCKSDB_BASE_EXEC)
	python3 experiment.py $(BUILD_DIR)/frontend/$(ROCKSDB_BASE_EXEC) $(PY_FLAGS)

%-read:
	- $(MAKE) $* read=100 scan=0 write=0

%-locality:
	- $(MAKE) $* read=100 scan=0 write=0 locality_read=1

%-scan:
	- $(MAKE) $* read=0 scan=100 write=0

%-write:
	- $(MAKE) $* read=0 scan=0 write=100
	
%-all-tx: %-read %-locality %-scan %-write 
	@echo "Completed all transaction types for $*"

update-size:
# - $(MAKE) write update_size=5 # refer to write expriments
	- $(MAKE) write update_size=10
	- $(MAKE) write update_size=20

%-selectivity:
	- $(MAKE) $*-all-tx selectivity=100
	- $(MAKE) $*-all-tx selectivity=50
	- $(MAKE) $*-all-tx selectivity=19
	- $(MAKE) $*-all-tx selectivity=5

%-size: # Change back
	- @for col in 2 0; do \
		for sel in 5 19 50 100; do \
			$(MAKE) $* dram=16 selectivity=$$sel included_columns=$$col duration=1 || echo "Failed for $$col columns and selectivity $$sel"; \
		done \
	done

.PHONY: both read scan write all-tx update-size selectivity no-columns table-size

# ----------------- CLEAN -----------------
clean:
	rm -rf $(BUILD_DIRS)