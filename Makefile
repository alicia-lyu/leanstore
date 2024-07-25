join-debug: # -fsanitize=address
	mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build-debug/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-rel:
	mkdir -p build-release && cd build-release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log

join-exp-disk: # on-disk ratio: 1:8, 1:16, 1:32, 1:64
	make join-rel
	./experiment.sh join 1 8 1 1 98
	./experiment.sh join 1 16 1 1 98
	./experiment.sh join 1 32 1 1 98
	./experiment.sh join 1 64 1 1 98

join-exp-rsw: # read/scan/write ratio
	make join-rel
# read only
	./experiment.sh join 1 32 100 0 0
# scan only
	./experiment.sh join 1 32 0 100 0
# write only
	./experiment.sh join 1 32 0 0 100
# write heavy included in join-exp-disk
# read heavy
	./experiment.sh join 1 32 70 15 15
# scan heavy
	./experiment.sh join 1 32 15 70 15

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

merged-exp-disk: # on-disk ratio: 1:8, 1:16, 1:32, 1:64
	make merged-rel
	./experiment.sh merged 1 8 1 1 98
	./experiment.sh merged 1 16 1 1 98
	./experiment.sh merged 1 32 1 1 98
	./experiment.sh merged 1 64 1 1 98

merged-exp-rsw: # read/scan/write ratio
	make merged-rel
# read only
	./experiment.sh merged 1 32 100 0 0
# scan only
	./experiment.sh merged 1 32 0 100 0
# write only
	./experiment.sh merged 1 32 0 0 100
# write heavy included in merged-exp-disk
# read heavy
	./experiment.sh merged 1 32 70 15 15
# scan heavy
	./experiment.sh merged 1 32 15 70 15

merged-exp:
	make merged-exp-disk > ~/logs/merged-exp-disk.log
	make merged-exp-rsw > ~/logs/merged-exp-rsw.log

exp:
	make join-exp
	make merged-exp

tpcc:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log