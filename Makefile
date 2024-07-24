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

	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=8 --read_percentage=1 --scan_percentage=1 --write_percentage=98
	mkdir -p ~/logs/log-1-8-1-1-98
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-8-1-1-98 \;

	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=16 --read_percentage=1 --scan_percentage=1 --write_percentage=98
	mkdir -p ~/logs/log-1-16-1-1-98
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-16-1-1-98 \;

	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=32 --read_percentage=1 --scan_percentage=1 --write_percentage=98
	mkdir -p ~/logs/log-1-32-1-1-98
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-32-1-1-98 \;

	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=64 --read_percentage=1 --scan_percentage=1 --write_percentage=98
	mkdir -p ~/logs/log-1-64-1-1-98
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-64-1-1-98 \;

join-exp-rsw: # read/scan/write ratio
	make join-rel
# Write heavy
	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=32 --read_percentage=1 --scan_percentage=1 --write_percentage=98
	mkdir -p ~/logs/log-1-32-1-1-98
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-32-1-1-98 \;
# Scan heavy
	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=32 --read_percentage=40 --scan_percentage=50 --write_percentage=10
	mkdir -p ~/logs/log-1-32-40-50-10
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-32-40-50-10 \;
# Read heavy
	./build-release/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --vi=false --mv=false --isolation_level=ser --csv_path=./build/log --dram_gib=1  --target_gib=32 --read_percentage=98 --scan_percentage=1 --write_percentage=1
	mkdir -p ~/logs/log-1-32-98-1-1
	find ./build-release -type f -name "log*" -exec mv {} ~/logs/log-1-32-98-1-1 \;

merged-debug:
	mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build-debug/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log 

merged-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log 

merged-rel:
	mkdir -p build-release && cd build-release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build-release/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log



tpcc:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=ser --csv_path=./build/log