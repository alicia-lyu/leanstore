CMAKE_DEBUG := cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO := cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..

CMAKE_OPTIONS := -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++

BUILD_DIR := build
BUILD_DIR_DEBUG := $(BUILD_DIR)-debug
BUILD_DIRS := $(BUILD_DIR) $(BUILD_DIR_DEBUG)

EXECS := basic_join basic_group basic_group_variant

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

$(TARGETS): check_perf_event_paranoid
	mkdir -p $(DIR)
	cd $(DIR) && $(CMAKE) $(CMAKE_OPTIONS)
	cd $(DIR) && $(MAKE) $(EXEC) -j$(NUMJOBS)

IMAGE_FILE := /mnt/hdd/tmp/test_image

lldb ?= true

LLDB_TARGETS := $(foreach exec, $(EXECS), $(exec)_lldb)

$(foreach exec, $(EXECS), \
	$(eval $(exec)_lldb: EXEC := $(exec)) \
	$(eval $(exec)_lldb: CSV := $(BUILD_DIR_DEBUG)/$(exec)) \
) # Variable match work well for an array of targets

dram := 1
scale := 0.01

leanstore_flags := --dram_gib=$(dram) --vi=false --mv=false --isolation_level=ser --optimistic_scan=false --pp_threads=1 --csv_truncate=false --worker_threads=2 --trunc=true --ssd_path=$(IMAGE_FILE) --tpch_scale_factor=$(scale)

# TODO: persist and recover

$(LLDB_TARGETS):
# Depedency match does not work well for an array of targets
	$(MAKE) $(BUILD_DIR_DEBUG)/frontend/$(EXEC)
	lldb -- $(BUILD_DIR_DEBUG)/frontend/$(EXEC) $(leanstore_flags) --csv_path=$(CSV)

$(EXECS):
	$(MAKE) $(BUILD_DIR)/frontend/$@
	mkdir -p $(BUILD_DIR)/$@/$(scale)-in-$(dram)
	script -q -c '$(BUILD_DIR)/frontend/$@ $(leanstore_flags) --csv_path=$(BUILD_DIR)/$@/$(scale)-in-$(dram)' $(BUILD_DIR)/$@/$(scale)-in-$(dram)/log

temp:
	$(MAKE) $(EXECS) dram=4
	$(MAKE) basic_join
	$(MAKE) basic_group dram=0.2
	$(MAKE) basic_group_variant dram=0.2

.PHONY: check_perf_event_paranoid build build_debug $(LLDB_TARGETS)