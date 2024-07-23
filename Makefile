join:
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log

join-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log

join-rel:
	cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log

merged:
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j
	./build/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log 

merged-lldb:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log 

merged-rel:
	cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
	./build/frontend/merged_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log

tpcc:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc --csv_path=./build/log