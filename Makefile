join-debug: # -fsanitize=address
	mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build-debug/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-rel:
	mkdir -p build-release && cd build-release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

default_read = 2
default_scan = 0
default_write = 98

join-exp-disk: # on-disk ratio: 1:8, 1:16, 1:32, 1:64
	make join-rel
	./experiment.sh join 1 1 $(default_read) $(default_scan) $(default_write)
	./experiment.sh join 1 2 $(default_read) $(default_scan) $(default_write)
	./experiment.sh join 1 4 $(default_read) $(default_scan) $(default_write)
# Later: Takes too long
	# ./experiment.sh join 1 8 $(default_read) $(default_scan) $(default_write)
	# ./experiment.sh join 1 16 $(default_read) $(default_scan) $(default_write)
	# ./experiment.sh join 1 32 $(default_read) $(default_scan) $(default_write)
	# ./experiment.sh join 1 64 $(default_read) $(default_scan) $(default_write)

default_dram = 1
default_target = 2

join-exp-rsw: # read/scan/write ratio
	make join-rel
# read only
	./experiment.sh join $(default_dram) $(default_target) 100 0 0
# scan only
	./experiment.sh join $(default_dram) $(default_target) 0 100 0
# write only
	./experiment.sh join $(default_dram) $(default_target) 0 0 100

join-exp:
	make join-exp-disk
	make join-exp-rsw

join-recover:
	make join-rel
	./experiment.sh join $(default_dram) $(default_target) $(default_read) $(default_scan) $(default_write) # first run
	./experiment.sh join $(default_dram) $(default_target) $(default_read) $(default_scan) $(default_write) # recover run

merged-debug:
	mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build-debug/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log 

merged-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log 

merged-rel:
	mkdir -p build-release && cd build-release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build-release/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

merged-exp-disk: # on-disk ratio: 1:2, 1:4, 1:8
	make merged-rel
	./experiment.sh merged 1 1 $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged 1 2 $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged 1 4 $(default_read) $(default_scan) $(default_write)
# Later: Takes too long
	# ./experiment.sh merged 1 8 $(default_read) $(default_scan) $(default_write)
	# ./experiment.sh merged 1 16 $(default_read) $(default_scan) $(default_write)
	# ./experiment.sh merged 1 32 $(default_read) $(default_scan) $(default_write)
	# ./experiment.sh merged 1 64 $(default_read) $(default_scan) $(default_write)

merged-exp-rsw: # read/scan/write ratio
	make merged-rel
# read only
	./experiment.sh merged $(default_dram) $(default_target) 100 0 0
# scan only
	./experiment.sh merged $(default_dram) $(default_target) 0 100 0
# write only
	./experiment.sh merged $(default_dram) $(default_target) 0 0 100

merged-exp:
	make merged-exp-disk
	make merged-exp-rsw

exp:
	make join-exp
	make merged-exp

make exp-min:
	mkdir -p build-release && cd build-release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./experiment.sh join $(default_dram) $(default_target) $(default_read) $(default_scan) $(default_write)
	./experiment.sh merged $(default_dram) $(default_target) $(default_read) $(default_scan) $(default_write)
	make join-exp-rsw
	make merged-exp-rsw

tpcc:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log