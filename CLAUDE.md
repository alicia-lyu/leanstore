# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow Rules

- **Do not run end-to-end executables on macOS.** macOS builds
  (RocksDB-only) exist for compile-checks and unit/load tests, not for
  full per-query binaries (`q3i_lsm`, `q12_lsm`, `q3_lsm`, etc.).
  Performance numbers and long-running TX loops belong on Linux. On
  macOS: build the target to verify it compiles, then stop. Use
  `test_query_q*_lsm` and `test_load_*_lsm` for correctness checks.
- **Do not run end-to-end executables until correctness is verified
  by shorter tests first.** Production per-query binaries
  (`q*_lsm`, `q*_btree`) run multi-second TX loops and produce
  performance numbers; they are not a debugging tool. Before invoking
  any `q*_lsm` / `q*_btree`: (1) build clean, (2) run the
  corresponding `test_load_*_lsm` to confirm secondaries populate,
  (3) run the corresponding `test_query_*_lsm` to confirm
  cross-structure parity. Only after all three are green should an
  end-to-end executable run. Performance numbers from a binary that
  hasn't passed parity are noise.
- **Reload eagerly on Linux when load-path files change.** Each
  `q*_{lsm,btree}_iso_N` Makefile target depends on the persisted
  `$(data_disk)/<query>_iso/iso_N/build/$(scale).json`, which in
  turn depends on `frontend/tpch/tpch_workload.hpp`,
  `frontend/tpch/tpchi_family/tpchi_workload.hpp`, and the per-query
  `frontend/tpch/<q>/load.tpp`. Any commit (yours or an upstream
  merge) that bumps the mtime of those files invalidates the
  persisted image, and the *next* `make q*_iso_N` invocation pays
  full reload cost (minutes at SF=15+, dominating the 15s TX run).
  After editing or merging any of those load-path files — and
  *before* you next need a clean perf number — kick off a
  background reload by running each affected target once with
  `--load_only_structure=N` (or just `make q3i_lsm_iso scale=15
  dram=0.1` to refresh all structures in one pass). Subsequent
  perf runs then reuse the fresh .json and pay only the 15s query
  cost. If you skip this step, the first perf run after a merge
  will be deceptively long and the slowdown will look like a
  regression in the binary itself rather than scheduled reload.
- **Push commits aggressively on Linux.** Linux experiment machines expire and wipe disk content without warning. After every commit on Linux — including intermediate "build passes" or "load complete" checkpoints — immediately run `git push`. Do not accumulate unpushed commits; a machine wipe will lose them permanently.
- When developing on macOS, add tasks pending on Linux/Leanstore to [LINUX_PENDING.md](LINUX_PENDING.md). Reference equivalent files implemented for LSM-tree. Resolved items are rotated out to [LINUX_HISTORY.md](LINUX_HISTORY.md) so the pending file stays a worklist, not an archive.
- **Append a `RUNS.md` entry after every perf run.** Each query
  directory under `frontend/tpch/<q>/` and `frontend/geo/` has a
  `RUNS.md`. After a `make q*_{lsm,btree}` (or `geo_*`) sweep
  produces a `TPut.csv` (or finishes without one), append an entry
  with: date+time, commit SHA + branch, path to the `TPut.csv` (or
  why it wasn't produced), config (SF / DRAM / structures / host),
  and **one sentence** stating whether the run supports the query's
  paper claim and how. If the run is correctness-only (no perf
  numbers), still log it — say so explicitly. Don't backfill old
  runs. Keep entries terse: this is a ledger, not a writeup.

## Doc Maintenance

Completed/Done log entries belong in `frontend/tpch/HISTORY.md`; rotate
to `HISTORY-YYYYHN.md` quarterly. Reference docs (`CLAUDE.md`,
`CONVENTIONS.md`, `OPERATORS.md`, `STATUS.md` as a table) describe
current state only — append-only history goes in `HISTORY.md`.

## Project Overview

This is a fork of [LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf), a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. It serves as the **execution engine** in a [Calcite ↔ LeanStore integration](https://github.com/alicia-lyu/calcite/blob/main/CALCITE_LEANSTORE_INTEGRATION.md), where Apache Calcite acts as the query optimizer (cost-based join ordering, merged-index substitution) and LeanStore executes the resulting plans against B-tree/LSM merged indexes.

### Integration Architecture

The integration follows a **Plan Export + C++ Runtime Interpreter** approach:
1. **Calcite** (Java, [separate repo](https://github.com/alicia-lyu/calcite)) parses SQL, optimizes with merged-index substitution rules, and exports the plan as JSON
2. **C++ Plan Interpreter** (in this repo, to be built) parses the JSON plan, builds an operator tree, and dispatches to pre-instantiated LeanStore templates
3. **LeanStore Storage** provides merged B-tree/LSM indexes with pre-sorted, interleaved multi-table data

The goal is to replace manually-coded C++ template queries with optimizer-generated plans, scaling to full TPC-H and JOB (Join Order Benchmark) workloads.

### COLI MI + Invoice-Extended Queries (Active Showcase)

The COLI 4-table merged index (`customer_coli_t`, `orders_coli_t`,
`lineitem_coli_t`, `invoice_coli_t`) is now implemented and load-tested
(`test_load_coli_lsm` passes at SF=1). Three invoice-extended queries — Q3I,
Q5I, Q10I — are the active showcase for the §3.1.2 sibling sub-aggregate
pattern: Invoice attaches under Customer as a sibling of Orders, and the COLI
MI co-locates all four record types per `custkey` so a single `PremergedJoin`
pass computes per-customer invoice aggregates alongside the O×L join. **Q3I's
paper-reported axis is S1–S4** — S1 (custkey-sorted split indexes), S2
(pipeline view), S3 (COLI MI), S4 (hash baseline). The pitch is **S3 ≥ S2 >
S1/S4**: raw co-location matches full materialisation without paying its
storage / maintenance cost, while comfortably beating split-merge-join (S1)
and hash (S4) baselines. S5 (aCOLI MI with `pre_open_due` baked at load time)
is implemented and parity-verified but **deferred from the paper sweep**
across all queries — see `frontend/tpch/PLAYBOOK.md §S5` for rationale (S3 >
S5 anomaly traced to S5 lacking the hand-tuned COLI walker; closing it is
infrastructure work, not per-query). Production `q3i_lsm` / `q3i_btree`
targets are wired into CMake and `generate_targets.py`. Q5I and Q10I are
design-doc only. See `frontend/tpch/q3i/CLAUDE.md §Implementation Phases` for
full detail.

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

## Repo Markdown Index

Every non-`CLAUDE.md` Markdown file at this level (or in `archive/`,
which has no `CLAUDE.md`). Each entry includes when to read it.

- [`LINUX_SETUP.md`](./LINUX_SETUP.md) — read when bringing up a fresh
  Linux node (perf_event_paranoid sysctl, NVMe partitioning + ext4
  mount, smoke-test commands). Authoritative; reproduce per-machine.
- [`LINUX_PENDING.md`](./LINUX_PENDING.md) — active worklist of
  macOS-developed items awaiting Linux follow-up (perf sweeps,
  LeanStore B-tree validation). Read at the start of every Linux
  session; append new items from macOS sessions.
- [`LINUX_HISTORY.md`](./LINUX_HISTORY.md) — append-only archive of
  resolved Linux/LeanStore bring-up items rotated out of
  `LINUX_PENDING.md`. Read only when reconstructing a prior fix.
- [`TPCH_experiments.md`](./TPCH_experiments.md) — read for the TPC-H
  experiment matrix: query status, storage-structure conventions,
  memory-pressure design, reviewer-concern mapping. Use this before
  drilling into a per-query `frontend/tpch/<q>/CLAUDE.md`.
- [`TRASH-FILES.md`](./TRASH-FILES.md) — audit trail of files moved to
  `TRASH/`. Read only when an agent encounters a referenced file that
  no longer exists / has been renamed and needs to confirm intent.
- `archive/` (no `CLAUDE.md` at that level — referenced from here):
  - [`archive/README.md`](./archive/README.md) — original geo benchmark
    schema and data-generation spec. Read when reconstructing the
    pre-TPC-H geo workload, otherwise skip.
  - [`archive/DOCKER_MEMORY.md`](./archive/DOCKER_MEMORY.md) — docker
    container session notes. Read when working on the Docker image or
    paper-submission packaging, otherwise skip.
  - [`archive/SESSION_PROGRESS.md`](./archive/SESSION_PROGRESS.md) —
    historical TPC-H Tier-1 progress snapshot. Superseded by per-query
    CLAUDE.md status sections; read only when reconstructing
    historical context.
  - [`archive/UPSTREAM_README.md`](./archive/UPSTREAM_README.md) —
    upstream LeanStore README. Read when investigating upstream
    behaviour or contributing back.

## Build Commands

### Prerequisites (Ubuntu 22.04 — full build)
```
sudo apt-get install cmake clang libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev libwiredtiger-dev liburing-dev
```

For full Linux bring-up on a fresh node (perf_event_paranoid sysctl,
SSD partition + mount, smoke-test commands), see
[`LINUX_SETUP.md`](./LINUX_SETUP.md).

### Prerequisites (macOS ARM — RocksDB-only build)
```
brew install cmake rocksdb gflags snappy lz4 zstd
```

### Build (RelWithDebInfo)
```
# Linux: builds both B-tree and LSM targets
mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && cd frontend && make geo_btree geo_lsm q12_btree q12_lsm -j$(nproc)

# macOS: builds LSM targets only (RocksDB backend)
mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && cd frontend && make geo_lsm q12_lsm -j$(sysctl -n hw.ncpu)
```

### Build (Debug)
```
mkdir -p build-debug && cd build-debug && cmake -DCMAKE_BUILD_TYPE=Debug .. && cd frontend && make geo_btree geo_lsm q12_btree q12_lsm -j$(nproc)
```

### Using the Makefile (requires Linux with perf_event_paranoid=0)
```
# Geo benchmark
make geo_btree scale=15          # Build + run B-tree experiments (structures 1-4)
make geo_lsm scale=40            # Build + run LSM experiments (structures 1-4)
make geo_btree_2 dram=0.1        # Run a single storage structure variant
make geo_btree_lldb_1             # Debug a single structure with LLDB

# TPC-H Q12
make q12_lsm scale=15            # Build + run LSM experiments (structures 1-4)
make q12_btree scale=15           # Build + run B-tree experiments (structures 1-4)
make q12_lsm_3 dram=0.1          # Run a single storage structure variant
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

### TPC-H unit tests and load tests

See [`frontend/tpch/CLAUDE.md §Tests`](frontend/tpch/CLAUDE.md#tests) for
the full index of TPC-H tests and their build/run commands. Per-query
specifics (e.g. Q12 view load-test) live alongside the per-query
directory: [`frontend/tpch/q12/CLAUDE.md §Tests`](frontend/tpch/q12/CLAUDE.md#tests).

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

The `storage_structure` flag (1-4) selects the indexing strategy being benchmarked.

**Geo benchmark:**

1. Traditional indexes with hash join
2. Materialized views
3. Merged indexes (single merged index)
4. Two merged indexes

**TPC-H Q12:**

1. Traditional indexes + merge join
2. Intermediate pipeline view (materialized ORDERS x LINEITEM)
3. MI[0] only (PremergedJoin over merged ORDERS + LINEITEM)
4. Traditional indexes + hash join

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
