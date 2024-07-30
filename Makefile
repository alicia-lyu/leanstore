# Default values
default_read := 2
default_scan := 0
default_write := 98
default_dram := 1
target_gib ?= 2

# Paths
SSD_PATH := /home/alicia.w.lyu/tmp/image
CSV_PATH := ./build/log

# Directories
BUILD_DEBUG_DIR := ./build-debug
BUILD_RELEASE_DIR := ./build-release
BUILD_DIR := ./build

# Executables
JOIN_EXEC := /frontend/join_tpcc
MERGED_EXEC := /frontend/merged_tpcc

# Compilation commands
CMAKE_DEBUG := cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO := cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
CMAKE_RELEASE := cmake -DCMAKE_BUILD_TYPE=Release ..

BUILD_DIRS := $(BUILD_DEBUG_DIR) $(BUILD_RELEASE_DIR) $(BUILD_DIR)
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

$(TARGETS): check_perf_event_paranoid
	mkdir -p $(DIR) && cd $(DIR) && $(CMAKE) && make -j

join-lldb: $(BUILD_DIR)$(JOIN_EXEC)
	lldb -- "$(BUILD_DIR)$(JOIN_EXEC)" --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH) --tpcc_warehouse_count=2

.PHONY: $(TARGETS)

# Experiment rules
join-exp-disk: $(BUILD_RELEASE_DIR)$(JOIN_EXEC)
	./experiment.sh join 1 1 $(default_read) $(default_scan) $(default_write)
	./experiment.sh join 1 2 $(default_read) $(default_scan) $(default_write)
	./experiment.sh join 1 4 $(default_read) $(default_scan) $(default_write)

join-exp-rsw: $(BUILD_RELEASE_DIR)$(JOIN_EXEC)
	./experiment.sh join $(default_dram) $(target_gib) 100 0 0
	./experiment.sh join $(default_dram) $(target_gib) 0 100 0
	./experiment.sh join $(default_dram) $(target_gib) 0 0 100

join-exp: join-exp-disk join-exp-rsw

join-recover: $(BUILD_RELEASE_DIR)$(JOIN_EXEC)
	./experiment.sh join $(default_dram) $(target_gib) $(default_read) $(default_scan) $(default_write) # first run
	./experiment.sh join $(default_dram) $(target_gib) $(default_read) $(default_scan) $(default_write) # recover run

merged-exp-disk: $(BUILD_RELEASE_DIR)$(MERGED_EXEC)
	./experiment.sh merged 1 1 $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged 1 2 $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged 1 4 $(default_read) $(default_scan) $(default_write)

merged-exp-rsw: $(BUILD_RELEASE_DIR)$(MERGED_EXEC)
	./experiment.sh merged $(default_dram) $(target_gib) 100 0 0
	./experiment.sh merged $(default_dram) $(target_gib) 0 100 0
	./experiment.sh merged $(default_dram) $(target_gib) 0 0 100

merged-exp: merged-exp-disk merged-exp-rsw

exp: join-exp merged-exp

exp-rsw: $(BUILD_RELEASE_DIR)$(MERGED_EXEC) $(BUILD_RELEASE_DIR)$(JOIN_EXEC)
	./experiment.sh join $(default_dram) $(target_gib) 100 0 0
	./experiment.sh merged $(default_dram) $(target_gib) 100 0 0

	./experiment.sh join $(default_dram) $(target_gib) 0 0 100
	./experiment.sh merged $(default_dram) $(target_gib) 0 0 100

	./experiment.sh join $(default_dram) $(target_gib) 0 100 0
	./experiment.sh merged $(default_dram) $(target_gib) 0 100 0

scan: $(BUILD_RELEASE_DIR)$(JOIN_EXEC) $(BUILD_RELEASE_DIR)$(MERGED_EXEC)
	./experiment.sh join 1 1 0 100 0
	./experiment.sh merged 1 1 0 100 0