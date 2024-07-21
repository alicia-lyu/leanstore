join:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./build/frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc csv_path=./build/log