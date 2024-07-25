join-debug: # -fsanitize=address
	mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build-debug/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-rel:
	mkdir -p build-release && cd build-release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

default_read = 1
default_scan = 1
default_write = 98

join-exp-disk: # on-disk ratio: 1:8, 1:16, 1:32, 1:64
	make join-rel
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
# write heavy included in join-exp-disk
# read heavy
	./experiment.sh join $(default_dram) $(default_target) 70 15 15
# scan heavy
	./experiment.sh join $(default_dram) $(default_target) 15 70 15

join-exp:
	make join-exp-disk > ~/logs/join-exp-disk.log
	make join-exp-rsw > ~/logs/join-exp-rsw.log

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
# write heavy included in merged-exp-disk
# read heavy
	./experiment.sh merged $(default_dram) $(default_target) 70 15 15
# scan heavy
	./experiment.sh merged $(default_dram) $(default_target) 15 70 15

merged-exp:
	make merged-exp-disk > ~/logs/merged-exp-disk.log
	make merged-exp-rsw > ~/logs/merged-exp-rsw.log

exp:
	make join-exp
	make merged-exp

tpcc:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log