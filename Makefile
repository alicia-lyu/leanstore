join:
	cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j
	lldb -- ./frontend/join_tpcc --ssd_path=/home/alicia.w.lyu/tmp/image --dram_gib=8 --vi=false --mv=false --isolation_level=rc