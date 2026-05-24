# DBToaster refresh_sales baseline — codegen → build → run → memory-pressure sweep.
# Copy-adapted from geodb-dbtoaster. Requires Phase 0 (DBToaster 2.3 + dbgen);
# see ../LINUX_SETUP.md.
#
# Pipeline:
#   1. `make data SF=<sf>`     — dbgen base + RF1/RF2 refresh, build stream files
#   2. `make refresh_sales.hpp`— DBToaster codegen (SQL -> C++ header)
#   3. `make build`            — cmake + compile the harness
#   4. `make run`              — single run (unlimited memory)
#   5. `make experiment`       — memory-pressure ladder (5L / 5H paper cells)

SF ?= 1
DATA := data_files

# ---------------------------------------------------------------------------
# Data generation. dbgen must be on PATH (Phase 0). Override DBGEN_DIR if its
# dists.dss / templates live elsewhere.
# Produces base *.tbl + RF1 (orders.tbl.u1, lineitem.tbl.u1) + RF2 (delete.1),
# then concatenates base+RF1 into the stream files the SQL reads.
# Generate base + refresh, then concatenate base + RF1 inserts into the ORDERS
# and LINEITEM stream files (CUSTOMER/NATION/REGION have no refresh and are read
# straight from .tbl). dbgen's pipe delimiter is handled via `delimiter := '|'`
# in the SQL; its trailing '|' is harmless (the CSV adaptor stops at end of
# schema), so no stripping is needed.
data:
	cd $(DATA) && dbgen -s $(SF) -f && dbgen -s $(SF) -U 1 -f
	cd $(DATA) && cat orders.tbl   orders.tbl.u1   > ORDERS.csv
	cd $(DATA) && cat lineitem.tbl lineitem.tbl.u1 > LINEITEM.csv
	@echo "stream rows:"; wc -l $(DATA)/customer.tbl $(DATA)/ORDERS.csv $(DATA)/LINEITEM.csv
	@echo "RF1 inserts:"; wc -l $(DATA)/orders.tbl.u1 $(DATA)/lineitem.tbl.u1
	@echo "RF2 deletes:"; wc -l $(DATA)/delete.1

# ---------------------------------------------------------------------------
# DBToaster codegen: SQL -> C++ header consumed by main.cpp.
refresh_sales.hpp: refresh_sales.sql
	dbtoaster refresh_sales.sql -l cpp -o refresh_sales.hpp

build: refresh_sales.hpp main.cpp CMakeLists.txt
	mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && $(MAKE) refresh_sales

run: build
	./build/refresh_sales

# Unlimited-memory run. DBToaster is pure in-memory (no SSD spill), so we do NOT
# sweep `ulimit -v` (a cap below the working set just OOMs). Instead we report
# the unlimited-memory footprint: VmRSS after warmup ~= the maintained-view
# working set (a loose lower bound on RAM), and peak RSS, which runs to a
# multiple of that. Pin SF so the working set ~= 5 GiB (paper anchor); see
# ../results/README.md. The in-memory footprint vs LeanStore S2 serving a 5 GiB
# SSD-backed secondary from a 0.4-1.0 GiB buffer pool is the headline contrast.
experiment: build
	bash ./entrypoint.sh

clean:
	rm -rf build refresh_sales.hpp

.PHONY: data build run experiment clean
