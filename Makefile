# -----------------------------------------------------------
# ----------------------executables--------------------------
CMAKE_DEBUG := cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO := cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..

CMAKE_OPTIONS := -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++

BUILD_DIR := build
BUILD_DIR_DEBUG := $(BUILD_DIR)-debug
BUILD_DIRS := $(BUILD_DIR) $(BUILD_DIR_DEBUG)

EXEC_NAMES := basic_join basic_group basic_group_variant

dram := 1
scale := 10

PERF_PARANOID := $(shell sysctl -n kernel.perf_event_paranoid)

check_perf_event_paranoid:
	@if [ $(PERF_PARANOID) -gt 0 ]; then \
		echo "Error: kernel.perf_event_paranoid is set to $(PERF_PARANOID). Must be 0."; \
		echo "Hint: sudo sysctl -w kernel.perf_event_paranoid=0"; \
		exit 1; \
	fi

executables := $(foreach dir, $(BUILD_DIRS), $(foreach exec, $(EXECS), $(dir)/frontend/$(exec)))

$(foreach exec, $(EXEC_NAMES), \
	$(foreach dir, $(BUILD_DIRS),
		$(eval $(dir)/frontend/$(exec): check_perf_event_paranoid) \
		$(eval define $(dir)_$(exec)_recipe
@echo "Building $@"
mkdir -p $(dir)/frontend
cd $(dir)/frontend && $(CMAKE) $(if $(findstring debug,$(dir)),$(CMAKE_DEBUG),$(CMAKE_RELWITHDEBINFO))
cd $(dir)/frontend && $(MAKE) $(exec) -j$(NUMJOBS)
endef) \
		$(eval $(dir)/frontend/$(exec): ; $(dir)_$(exec)_recipe) \
	)
)

.PHONY: check_perf_event_paranoid

# ---------------------------------------------------------
# ----------------------experiments------------------------
IMAGE_FILE := /mnt/hdd/tmp/test_image

leanstore_flags := --dram_gib=$(dram) --vi=false --mv=false --isolation_level=ser --optimistic_scan=false --pp_threads=1 --csv_truncate=false --worker_threads=2 --trunc=true --ssd_path=$(IMAGE_FILE) --tpch_scale_factor=$(scale)

# Targets for restore files
$(foreach dir, $(BUILD_DIRS), \
  $(foreach exec, $(EXEC_NAMES), \
    $(eval $(dir)_$(exec)_restore_path := $(dir)/frontend/$(exec)/$(scale).json) \
    $(eval $(dir)_$(exec)_restore_path: $(dir)/frontend/$(exec)) \
    $(eval define $(dir)_$(exec)_restore_path_recipe
@echo "Persisting data to $@"
$(dir)/frontend/$(exec) $(leanstore_flags) --csv_path=$(CSV) --persist_file=$@ 2>$(CSV)/stderr.txt
endef) \
    $(eval $(dir)_$(exec)_restore_path: ; $($(dir)_$(exec)_restore_path_recipe)) \
  ) \
)

# Targets for running the executables
TARGETS := $(EXEC_NAMES)

$(foreach target, $(EXEC_NAMES), \
	$(eval executable := $(BUILD_DIR)/frontend/$(target)) \
	$(eval $(target) : $(executable) $(BUILD_DIR)/frontend/$(exec)/$(scale).json)\
	$(eval define $(target)_recipe
@echo "Running $@"
mkdir -p $(CSV)
script -q -c 'bash -c "$(executable) $(leanstore_flags) --csv_path=$(CSV) --restore_file=$(dir)/frontend/$(exec)/$(scale).json 2>$(CSV)/stderr.txt"' $(CSV)/log
endef) \
	$(eval $(target): ; $($(target)_recipe)) \
)

# Targets for running the executables with LLDB
LLDB_TARGETS := $(foreach exec, $(EXEC_NAMES), $(exec)_lldb)

$(foreach exec, $(EXEC_NAMES), \
	$(eval $(exec)_lldb: $(BUILD_DIR_DEBUG)/frontend/$(exec) $(BUILD_DIR)/frontend/$(exec)/$(scale).json) \
	$(eval define $(exec)_lldb_recipe
@echo "Running $@"
mkdir -p $(CSV)
lldb --source .lldbinit -- $(BUILD_DIR_DEBUG)/frontend/$(exec) $(leanstore_flags) --csv_path=$(CSV) --restore_file=$(BUILD_DIR)/frontend/$(exec)/$(scale).json
endef) \
	$(eval $(exec)_lldb: ; $($(exec)_lldb_recipe)) \
)

.PHONY: $(LLDB_TARGETS) $(TARGETS)