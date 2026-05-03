# Docker Container Session Notes

## What was done

### Goal
Create a reproducible Docker container for paper submission that builds and
runs the geo_btree and geo_lsm experiments without any dependency on host
machine paths.

### Files created / modified

| File | Change |
|------|--------|
| `Dockerfile` | New: ubuntu:22.04 base, installs deps, copies source, builds geo_btree + geo_lsm, creates /data and /results inside the container |
| `run_experiments.sh` | New: calls geo_btree and geo_lsm executables directly (bypasses make's check_perf_event_paranoid gate), loads data if not present, runs structures 1–4 for each engine |
| `.dockerignore` | New: excludes build/ and build-debug/ so host CMakeCache.txt doesn't leak in |
| `CMakeLists.txt` | Two fixes: (1) moved set(CMAKE_C/CXX_COMPILER) before project() to stop infinite cmake re-configure loop; (2) fixed broken elseif() that was silently injecting -fsanitize=address into all non-Debug builds |

### Key design decisions

- **No host mounts**: /data and /results are directories inside the container.
  Reviewers run `docker run leanstore-geo` with no -v flags needed.
- **Perf gate bypassed**: make targets require kernel.perf_event_paranoid=0
  (impossible in an unprivileged container). run_experiments.sh calls
  executables directly. TPut (TX/s) column is unaffected; CPU-cycle columns
  will be absent/zero.
- **Cache-friendly Dockerfile**: source files (CMakeLists.txt, libs/,
  backend/, frontend/) are copied before the cmake/make build step. Scripts
  (run_experiments.sh) are copied after. Editing only the script rebuilds in
  ~1 second; the ~20-minute RocksDB build stays cached.
- **ASan removed**: the elseif() fix removes unintended AddressSanitizer
  overhead (~2x slowdown) from RelWithDebInfo builds.

### Build & run

```bash
# Build (needs network for git clone of vendored deps)
docker build -t geodb .

# Run (everything self-contained inside the container)
docker run --name geodb-run geodb

# Check results inside the container after it finishes
docker exec geodb-run cat /results/geo_btree_TPut.csv
docker exec geodb-run cat /results/geo_lsm_TPut.csv
```

### Parameters used (matching generate_targets.py / Makefile defaults)
- geo_btree: scale=15, dram=0.1, structures 1–4
- geo_lsm:   scale=40, dram=0.1, structures 1–4
- Shared: --vi=false --mv=false --isolation_level=ser --optimistic_scan=false
          --pp_threads=1 --worker_threads=2 --tentative_skip_bytes=0 --bgw_pct=0

### System requirements for reviewers
- Docker Engine installed
- x86_64 CPU with AVX2 (any post-2013 Intel/AMD)
- ~10 GiB free RAM (load step uses dram_gib=8)
- ~10 GiB free disk (image files: ~2.5 GiB btree + ~1.5 GiB LSM)
- Network access during `docker build` (downloads vendored deps from GitHub)
