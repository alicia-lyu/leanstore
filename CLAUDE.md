# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a fork of [LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf), a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. It serves as the **execution engine** in a [Calcite ↔ LeanStore integration](https://github.com/alicia-lyu/calcite/blob/main/CALCITE_LEANSTORE_INTEGRATION.md), where Apache Calcite acts as the query optimizer (cost-based join ordering, merged-index substitution) and LeanStore executes the resulting plans against B-tree/LSM merged indexes.

### Integration Architecture

The integration follows a **Plan Export + C++ Runtime Interpreter** approach:
1. **Calcite** (Java, [separate repo](https://github.com/alicia-lyu/calcite)) parses SQL, optimizes with merged-index substitution rules, and exports the plan as JSON
2. **C++ Plan Interpreter** (in this repo, to be built) parses the JSON plan, builds an operator tree, and dispatches to pre-instantiated LeanStore templates
3. **LeanStore Storage** provides merged B-tree/LSM indexes with pre-sorted, interleaved multi-table data

The goal is to replace manually-coded C++ template queries with optimizer-generated plans, scaling to full TPC-H and JOB (Join Order Benchmark) workloads.

### TPC-H Q12 Implementation (In Progress)

The first Calcite-planned query being manually coded as a proof-of-concept, translating optimizer-generated plans from `calcite-integration-info/test-plans/q12/` into C++.

**Two merged indexes:**
- **MI[0]** (Pipeline 0): ORDERS + LINEITEM interleaved by orderkey. Trivial — direct insertion.
- **Root pipeline MI** (Pipeline 1): Indexed view storing join→filter→project results keyed by `(l_shipmode, o_orderkey, l_linenumber)`.

**Key infrastructure change:** Adopting Calcite's tagged row format (`[domainTag][keyVal]...[indexTag][sourceId][payload]`) in merged adapters/scanners, replacing the current fold-length heuristic for record type discrimination.

**Files:** `frontend/tpch/q12/` — views, workload, query, maintenance, executables.

**Storage structure variants (--storage_structure 1-4):**
1. Traditional indexes + hash join
2. Fully materialized view
3. MI[0] only (merged index, query-time join)
4. MI[0] + root pipeline MI (full Calcite plan)

### Key Execution Components

- **Merged Index Adapter** (`LeanStoreMergedAdapter<Records...>`): variadic template storing heterogeneous records in one B-tree with lexicographic key folding
- **Merged Scanner** (`LeanStoreMergedScanner`): iterator returning `std::variant<Record1, Record2, ...>` with seek/next navigation
- **PremergedJoin** (`premerged_join.hpp`): single-pass multi-table join assembly over one merged scanner, with adaptive seek strategy and join state machine
- **TPC-H Support** (`tpch_tables.hpp`, `tpch_workload.hpp`): all 8 TPC-H tables defined as C++ record types with proper key structures

### Geo Benchmark

The fork also includes an experimental workload ("geo") that benchmarks different indexing strategies (traditional indexes, materialized views, merged indexes) for multi-table joins over a geographic hierarchy (Nation → States → County → City → Customer) using both B-tree (LeanStore native) and LSM-tree (RocksDB) storage backends.

## Build Commands

### Prerequisites (Ubuntu 22.04 — full build)
```
sudo apt-get install cmake clang libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev libwiredtiger-dev liburing-dev
```

### Prerequisites (macOS ARM — RocksDB-only build)
```
brew install cmake rocksdb gflags snappy lz4 zstd
```

### Build (RelWithDebInfo)
```
# Linux: builds both geo_btree and geo_lsm
mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && cd frontend && make geo_btree geo_lsm -j$(nproc)

# macOS: builds geo_lsm only (RocksDB backend)
mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && cd frontend && make geo_lsm -j$(sysctl -n hw.ncpu)
```

### Build (Debug)
```
mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && cd frontend && make geo_btree geo_lsm -j$(nproc)
```

### Using the Makefile (requires Linux with perf_event_paranoid=0)
```
make geo_btree scale=15          # Build + run B-tree experiments (structures 1-4)
make geo_lsm scale=40            # Build + run LSM experiments (structures 1-4)
make geo_btree_2 dram=0.1        # Run a single storage structure variant
make geo_btree_lldb_1             # Debug a single structure with LLDB
```

Key Makefile variables: `dram` (GiB, default 0.1), `scale` (TPC-H scale factor), `tentative_skip_bytes`, `bgw_pct`.

### Docker (self-contained, no host mounts needed)
```
docker build -t geodb .
docker run geodb              # Runs all experiments
docker run -it geodb bash     # Interactive shell
```

### Regenerate Makefile targets
```
python3 generate_targets.py > targets.mk
```
This also regenerates `.vscode/launch.json` with LLDB debug configurations.

## Architecture

### Three-layer structure

- **`backend/`** — The LeanStore storage engine library. Core components:
  - `storage/btree/` — B-tree implementation with buffer management
  - `storage/buffer-manager/` — Lightweight buffer manager with pointer swizzling
  - `concurrency-recovery/` — MVCC, snapshot isolation (SI), distributed logging
  - `KVInterface.hpp` — Abstract key/value interface (lookup, insert, scan, remove) that both LeanStore and RocksDB adapters implement

- **`frontend/`** — Experiment executables and shared adapter code:
  - `geo/` — The geographic join benchmark. Two entry points: `executable_leanstore.cpp` (B-tree via LeanStore) and `executable_rocksdb.cpp` (LSM via RocksDB)
  - `shared/` — Storage adapters (`LeanStoreAdapter`, `RocksDB`), merge-join utilities, logging, and schema helpers

- **`shared-headers/`** — Cross-cutting headers (units, perf events, CRC, exceptions)

### Storage structure variants (--storage_structure flag)

The `storage_structure` flag (1-4) selects the indexing strategy being benchmarked:
1. Traditional indexes with hash join (`trad_idx_hj`)
2. Materialized views
3. Merged indexes (single merged index)
4. Two merged indexes

Structure 0 forces a data reload.

### Adapter pattern

The code is templated on adapter types (`LeanStoreAdapter` / `RocksDB`) so the same workload logic runs against both storage backends. Scanners (`LeanStoreScanner` / `LeanStoreMergedScanner`) provide iteration. The `GeoJoin` template composes these into workload classes (`BaseGeoJoin`, `ViewGeoJoin`, `MergedGeoJoin`, `HashGeoJoin`).

### Experiment data flow

1. **Load phase**: Generates synthetic TPC-H-like data, writes to SSD image, persists a recovery JSON file
2. **Run phase**: Recovers from the JSON file, runs queries (join, count aggregation, distinct count) and updates for a configurable duration, outputs CSV metrics

Runtime flags are configured via gflags. Key flags: `--ssd_path`, `--dram_gib`, `--tpch_scale_factor`, `--isolation_level` (ser/si/rc/ru), `--worker_threads`, `--pp_threads`.

## CMake options

- `-DSANI=ON` — Enable AddressSanitizer
- `-DPARANOID=ON` — Enable sanity checks in release builds
- `-DCOUNTERS_LEVEL=all` — Control performance counter instrumentation
- `-DCHECKS_LEVEL=default|debug|release|benchmark` — Control assertion level

## Compiler

The project requires Clang (`/usr/bin/clang` and `/usr/bin/clang++`), set in CMakeLists.txt. C++20 standard.
