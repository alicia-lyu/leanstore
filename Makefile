# Default values
default_read = 2
default_scan = 0
default_write = 98
default_dram = 1
target_gib ?= 1

# Directories
BUILD_DEBUG_DIR = build-debug
BUILD_RELEASE_DIR = build-release
BUILD_DIR = build

# Paths
SSD_PATH = /home/alicia.w.lyu/tmp/image
CSV_PATH = ./build/log

# Executables
JOIN_EXEC = ./$(BUILD_RELEASE_DIR)/frontend/join_tpcc
MERGED_EXEC = ./$(BUILD_RELEASE_DIR)/frontend/merged_tpcc

# Compilation commands
CMAKE_DEBUG = cmake -DCMAKE_BUILD_TYPE=Debug ..
CMAKE_RELWITHDEBINFO = cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
CMAKE_RELEASE = cmake -DCMAKE_BUILD_TYPE=Release ..

# Common targets
.PHONY: all join-debug join-lldb join-rel merged-debug merged-lldb merged-rel \
        join-exp-disk join-exp-rsw join-exp join-recover merged-exp-disk merged-exp-rsw merged-exp exp exp-min tpcc

all: exp

# Join targets
join-debug: 
	mkdir -p $(BUILD_DEBUG_DIR) && cd $(BUILD_DEBUG_DIR) && $(CMAKE_DEBUG) && make -j
	./$(JOIN_EXEC) --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)

join-lldb:
	cd $(BUILD_DIR) && $(CMAKE_RELWITHDEBINFO) && make -j
	lldb -- $(JOIN_EXEC) --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)

join-rel:
	mkdir -p $(BUILD_RELEASE_DIR) && cd $(BUILD_RELEASE_DIR) && $(CMAKE_RELEASE) && make -j
	./$(JOIN_EXEC) --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)

join-exp-disk: join-rel
	./experiment.sh join 1 1 $(default_read) $(default_scan) $(default_write)
	./experiment.sh join 1 2 $(default_read) $(default_scan) $(default_write)
	./experiment.sh join 1 4 $(default_read) $(default_scan) $(default_write)
# Later: Takes too long
#	./experiment.sh join 1 8 $(default_read) $(default_scan) $(default_write)
#	./experiment.sh join 1 16 $(default_read) $(default_scan) $(default_write)
#	./experiment.sh join 1 32 $(default_read) $(default_scan) $(default_write)
#	./experiment.sh join 1 64 $(default_read) $(default_scan) $(default_write)

join-exp-rsw: join-rel
# read only
	./experiment.sh join $(default_dram) $(target_gib) 100 0 0
# scan only
	./experiment.sh join $(default_dram) $(target_gib) 0 100 0
# write only
	./experiment.sh join $(default_dram) $(target_gib) 0 0 100

join-exp: join-exp-disk join-exp-rsw

join-recover: join-rel
	./experiment.sh join $(default_dram) $(target_gib) $(default_read) $(default_scan) $(default_write) # first run
	./experiment.sh join $(default_dram) $(target_gib) $(default_read) $(default_scan) $(default_write) # recover run

# Merged targets
merged-debug:
	mkdir -p $(BUILD_DEBUG_DIR) && cd $(BUILD_DEBUG_DIR) && $(CMAKE_DEBUG) && make -j
	./$(BUILD_DEBUG_DIR)/frontend/merged_tpcc --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)

merged-lldb:
	cd $(BUILD_DIR) && $(CMAKE_RELWITHDEBINFO) && make -j
	lldb -- $(BUILD_RELEASE_DIR)/frontend/merged_tpcc --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)

merged-rel:
	mkdir -p $(BUILD_RELEASE_DIR) && cd $(BUILD_RELEASE_DIR) && $(CMAKE_RELEASE) && make -j
	./$(BUILD_RELEASE_DIR)/frontend/merged_tpcc --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)

merged-exp-disk: merged-rel
	./experiment.sh merged 1 1 $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged 1 2 $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged 1 4 $(default_read) $(default_scan) $(default_write)
# Later: Takes too long
#	./experiment.sh merged 1 8 $(default_read) $(default_scan) $(default_write)
#	./experiment.sh merged 1 16 $(default_read) $(default_scan) $(default_write)
#	./experiment.sh merged 1 32 $(default_read) $(default_scan) $(default_write)
#	./experiment.sh merged 1 64 $(default_read) $(default_scan) $(default_write)

merged-exp-rsw: merged-rel
# read only
	./experiment.sh merged $(default_dram) $(target_gib) 100 0 0
# scan only
	./experiment.sh merged $(default_dram) $(target_gib) 0 100 0
# write only
	./experiment.sh merged $(default_dram) $(target_gib) 0 0 100

merged-exp: merged-exp-disk merged-exp-rsw

exp: join-exp merged-exp

exp-rsw:
	make join-exp-rsw
	make merged-exp-rsw

tpcc:
	cd $(BUILD_DIR) && $(CMAKE_RELWITHDEBINFO) && make -j
	lldb -- ./$(BUILD_RELEASE_DIR)/frontend/tpcc --ssd_path=$(SSD_PATH) --dram_gib=$(default_dram) --vi=false --mv=false --isolation_level=ser --csv_path=$(CSV_PATH)
