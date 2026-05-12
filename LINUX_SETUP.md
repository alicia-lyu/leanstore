# LINUX_SETUP.md

End-to-end bring-up for a fresh Linux node when the goal is the **full
LeanStore + RocksDB build** (all `_btree` and `_lsm` targets). Use this when
running on bare CloudLab / EC2 / any disposable Ubuntu node. For
container-based runs see `DOCKER_MEMORY.md`; for macOS see `CLAUDE.md
§Build Commands` (RocksDB-only).

The macOS dev flow exercises only the `_lsm` (RocksDB) targets. The
`_btree` targets — `geo_btree`, `q12_btree`, `q3i_btree` and the
matching LeanStore parity tests — only build on Linux because the
`backend/` LeanStore library is gated by `if (NOT APPLE)` in
`CMakeLists.txt`. This document is what you run on a fresh Linux node
to make those targets buildable and runnable.

## Prerequisites

- Ubuntu 22.04+ (jammy or later). Earlier releases lack `liburing-dev`
  in the default repos. `lsb_release -a` to confirm.
- x86_64 with AVX2 + cx16 (`grep -E 'avx2|cx16' /proc/cpuinfo`). The
  build adds `-mavx2 -mcx16` unconditionally on x86 (`CMakeLists.txt:24`).
- Passwordless `sudo` (or willingness to type a password 3–4 times).
- Internet access during the first `cmake` + `make` — vendored deps
  (RocksDB, gflags, tabulate, rapidjson) are git-cloned as
  `ExternalProject` targets. Once built they're cached under
  `build/vendor/`.
- ~15 GiB disk for the build (vendored RocksDB source + objects),
  plus whatever you need for benchmark data (see Step 3).

## Step 1 — Install packages

The authoritative package list is `Dockerfile:7-11`. Re-run after every
new node provisioning:

```
sudo apt-get update
sudo apt-get install -y \
    cmake clang git make python3 \
    libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev \
    libbz2-dev liblz4-dev libzstd-dev liburing-dev \
    librocksdb-dev libwiredtiger-dev
```

CMake hard-codes `/usr/bin/clang` / `/usr/bin/clang++` (`CMakeLists.txt:7-9`)
— GCC will be ignored even if installed. The Ubuntu 22.04 `clang`
package installs there at version 14, which is C++20-capable and
sufficient.

`librocksdb-dev` is installed because the system header dir is on the
default include path; the actual library used by the build is the
vendored RocksDB built from source by `libs/rocksdb.cmake`. The
upstream README also lists `liblmdb-dev` (`README.md:141`) but the
Dockerfile omits it — not needed for the active workloads.

Verify:

```
clang --version    # expect Ubuntu clang version 14.x
cmake --version    # expect 3.22+
for pkg in cmake clang libtbb2-dev libaio-dev liburing-dev \
           librocksdb-dev libwiredtiger-dev libsnappy-dev; do
  dpkg -l "$pkg" >/dev/null 2>&1 && echo "OK $pkg" || echo "MISSING $pkg"
done
```

## Step 2 — Configure perf_event_paranoid

The Makefile gate (`Makefile:43-49`) refuses to build any target unless
`kernel.perf_event_paranoid == 0`. The default on most distros is 2 or
4, so this is a real blocker every time.

```
sudo sysctl -w kernel.perf_event_paranoid=0
echo 'kernel.perf_event_paranoid=0' | sudo tee /etc/sysctl.d/99-leanstore.conf
cat /proc/sys/kernel/perf_event_paranoid    # expect 0
```

The `/etc/sysctl.d/` entry survives reboot (CloudLab nodes get
reprovisioned, but if the node persists across a reboot you don't
want to redo this).

If you need to defer this (e.g. you only want to compile, not run via
`make geo_btree`), invoke binaries directly: `./build/frontend/geo_btree
--ssd_path=...` skips the Makefile gate entirely. The binary itself
will run with `perf_event_paranoid > 0`; only the perf-counter columns
in the output CSV will be blank.

## Step 3 — Provision the SSD path

The default `--ssd_path` / Makefile `data_disk` is `/mnt/ssd`
(`Makefile:18-23`). Most fresh nodes don't have anything mounted there.
Three options, in descending order of preference:

### 3a. Partition unallocated NVMe space (preferred when available)

CloudLab and many cloud nodes ship with the root partition occupying
only part of the disk, OR with one or more entire NVMe devices left
unallocated. Check:

```
lsblk
sudo parted /dev/nvme0n1 unit MiB print free
sudo wipefs -n /dev/nvme0n1                 # confirm the device is empty
```

Three sub-cases:

**(i) Entire NVMe device is blank** (`lsblk` shows no `nvmeXnYp*`
partitions for the device, and `wipefs -n` is empty). This is the
common CloudLab case where the node has multiple unused NVMe disks
alongside an `sda` root. Skip `parted` entirely — format the whole
device:

```
sudo mkfs.ext4 -F -L leanstore-ssd /dev/nvme0n1
```

**(ii) Existing partition table with a free-space row** (root takes
only part of the disk). Carve a partition out of the free range:

```
# Replace 65794MiB / 236006MiB with the actual free range from `print free`.
sudo parted -s /dev/nvme0n1 mkpart leanstore-ssd ext4 65794MiB 236006MiB
sudo partprobe /dev/nvme0n1
sudo parted /dev/nvme0n1 unit MiB print     # confirm new partition number

# Format (substitute pN with the new partition number, usually p4):
sudo mkfs.ext4 -F -L leanstore-ssd /dev/nvme0n1p4
```

**(iii) No free space available** — go to §3b or §3c.

Then mount (same for all three sub-cases — `LABEL=leanstore-ssd`
makes fstab device-name agnostic):

```
sudo mkdir -p /mnt/ssd
grep -q "leanstore-ssd" /etc/fstab \
  || echo 'LABEL=leanstore-ssd /mnt/ssd ext4 defaults,noatime 0 2' \
       | sudo tee -a /etc/fstab
sudo mount /mnt/ssd
sudo chown $USER:$(id -gn) /mnt/ssd        # use $(id -gn), NOT $USER:$USER
                                            # (primary group name often differs)
df -h /mnt/ssd                              # confirm size + free space
touch /mnt/ssd/probe && rm /mnt/ssd/probe  # confirm unprivileged write
```

`noatime` skips access-time updates on a benchmarking volume.
`LABEL=leanstore-ssd` in fstab keeps the entry valid through reformats.

### 3b. Use the root partition

If the node has no spare disk and the root partition has sufficient
free space (`df -h /` to check):

```
sudo mkdir -p /mnt/ssd
sudo chown $USER:$(id -gn) /mnt/ssd
```

Watch out: TPC-H scale factor 15 needs ~30 GiB; SF=40 needs ~80 GiB.

### 3c. Use any path you have write access to

Skip `/mnt/ssd` entirely and pass the path on every invocation:

```
mkdir -p /users/$USER/ssd_data
make geo_btree data_disk=/users/$USER/ssd_data scale=15
./build/frontend/test_query_q12_btree --ssd_path=/users/$USER/ssd_data ...
```

## Step 4 — Build

```
cd /path/to/leanstore           # the repo root
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
cd frontend
make geo_btree geo_lsm \
     q12_btree q12_lsm \
     q3i_btree q3i_lsm \
     test_load_merged_btree test_load_merged_lsm \
     test_load_q12_btree    test_load_q12_lsm \
     test_query_q12_btree   test_query_q12_lsm \
                            test_query_q3i_lsm test_query_q3i_btree \
     -j$(nproc)
```

Expected first-build behavior:

- `cmake ..` is fast (seconds). It only writes the build files;
  vendored deps are not yet cloned.
- `make` clones `vendor/rocksdb`, `vendor/gflags`, `vendor/tabluate`,
  `vendor/rapidjson` from GitHub on demand and builds them. **RocksDB
  takes the longest** — single-digit minutes on a 16-core box, longer
  on smaller machines.
- Subsequent rebuilds are fast — the vendored libs are cached under
  `build/vendor/`.

If the build fails on a `_btree` target with a missing-include or
other small compile error, that is most likely a regression introduced
on the macOS-only flow (the `_btree` targets aren't compiled on
macOS). Fix in place and re-run; `_btree` is gated by
`#ifndef ROCKSDB_ONLY` so the same fix usually doesn't affect
macOS.

For a Debug build use `mkdir -p build-debug && cd build-debug && cmake
-DCMAKE_BUILD_TYPE=Debug ..` and the same `make` command.

## Step 5 — Smoke-test parity

Each invocation needs a distinct `--ssd_path` (LeanStore writes to a
single file at the path; RocksDB writes a directory tree — colliding
paths cause data corruption / load failures). The Q3I harness wipes
its `--ssd_path` defensively at the start.

### LeanStore-specific runtime requirements

`_btree` (LeanStore) binaries refuse to start unless:

1. `--wal=true` is passed (or `--vi=false` is set). The default
   `--vi=true` enables in-place version SI, which mandates WAL
   (`backend/leanstore/LeanStore.cpp:53`). Without `--wal`:
   `terminate called: You have to enable WAL`.
2. The path passed to `--ssd_path` already exists as a regular file
   (LeanStore mmaps a single backing file, it does not auto-create
   it). The Makefile creates this file via `truncate` before
   invoking the binary; when running binaries directly, `touch
   /mnt/ssd/<dir>/db.image` first. Without it: `posix error: No
   such file or directory; Could not open the file or the SSD block
   device`.

`_lsm` (RocksDB) binaries auto-create their `--ssd_path` directory
and do not need `--wal`.

### 5a. Load tests (4 binaries, 2 backends each)

RocksDB:

```
cd build/frontend
./test_load_merged_lsm   --ssd_path=/mnt/ssd/lm_lsm   --tpch_scale_factor=1
./test_load_q12_lsm      --ssd_path=/mnt/ssd/lq_lsm   --tpch_scale_factor=1
```

LeanStore (note `--wal=true` and pre-touched file):

```
mkdir -p /mnt/ssd/lm_btree && touch /mnt/ssd/lm_btree/db.image
./test_load_merged_btree --ssd_path=/mnt/ssd/lm_btree/db.image \
    --tpch_scale_factor=1 --wal=true

mkdir -p /mnt/ssd/lq_btree && touch /mnt/ssd/lq_btree/db.image
./test_load_q12_btree    --ssd_path=/mnt/ssd/lq_btree/db.image \
    --tpch_scale_factor=1 --wal=true
```

Each should print `[OK]` lines for every check and exit 0.

> **Status as of 2026-05-08:** the historical
> `loadInvoiceAndLinkLineitem` `ensure(false)` and the SEGV during
> orders loading are both resolved. The SEGV root cause was
> `LeanStoreAdapter::getScanner()` / `LeanStoreMergedAdapter` casting
> to `BTreeVI*` when `FLAGS_vi=true` (default), even though the
> adapter ctor has always registered a `BTreeLL`; the dynamic_cast
> returned null and the first iterator op deref'd `btree.dt_id`.
> Fix lives in `frontend/shared/adapter-scanner/`. `_btree` smoke
> tests now run with the same flags as `_lsm` — no `--vi=false`
> workaround needed.
>
> **Currently passing at SF=1**: `test_load_merged_btree`,
> `test_load_q12_btree`, `test_load_col_btree`,
> `test_query_q12_btree`, `test_query_q3_btree` — all `[OK]`
> with `--wal=true` and the default flags.
>
> **Still failing**: `test_query_q3i_btree` aborts with a COLI
> tagged-key variant-dispatch assertion in
> `views_coli.hpp:110` during `coli_group_walk`. Different bug —
> tracked in `LINUX_PENDING.md`. Q3I LSM is unaffected.

> **Pitfall when running smoke tests:** `cmd | tail` makes `$?`
> report `tail`'s exit code, not the binary's, so a SEGV looks like
> success. Either redirect to a file (`cmd > /tmp/x.log 2>&1; echo
> $?`) or use `${PIPESTATUS[0]}`. Several `_btree` SEGVs in the
> 2026-05-08 bring-up were initially missed because of this.

### 5b. Query parity (Q12 + Q3I, both backends)

```
./test_query_q12_lsm   --ssd_path=/mnt/ssd/q12_lsm   --tpch_scale_factor=1
./test_query_q12_btree --ssd_path=/mnt/ssd/q12_btree --tpch_scale_factor=1
./test_query_q3i_lsm   --ssd_path=/mnt/ssd/q3i_lsm   --tpch_scale_factor=1
./test_query_q3i_btree --ssd_path=/mnt/ssd/q3i_btree --tpch_scale_factor=1
```

Expect: identical `Q12Stats` / `Q3IStats` digests within each backend
across all paper-reported storage structures (S1–S4 for Q12 and Q3I;
Q3I S5 is implemented but deferred from the paper sweep — see
`frontend/tpch/PLAYBOOK.md §S5`), and
identical per-storage-structure digests across the two backends at
the same SF.

### 5c. End-to-end Makefile sanity

Confirms the full Makefile path including `targets.mk` generation and
the `check_perf_event_paranoid` gate:

```
cd /path/to/leanstore
make geo_btree scale=15 dram=0.1
make q12_btree scale=15 dram=0.1
make q3i_btree scale=1  dram=0.1
```

Each builds the binary and runs storage-structure variants 1–4
(generated from `generate_targets.py:STRUCTURE_OPTIONS`).

## Troubleshooting

- **`Error: perf_event_paranoid is N. Must be 0.`** — Step 2 was not
  run, or `/etc/sysctl.d/` entry didn't take effect after reboot. Re-run
  `sudo sysctl -p /etc/sysctl.d/99-leanstore.conf`.
- **Build fails fetching a vendored repo.** The `ExternalProject_Add`
  rules in `libs/*.cmake` use `TIMEOUT 10` (`libs/rocksdb.cmake:14`).
  Slow networks can hit the timeout during git-clone. Bump the
  `TIMEOUT` line and re-run `make`.
- **`fatal error: 'leanstore/LeanStore.hpp' file not found`** when
  building a `_btree` target. The `backend/` dir wasn't included in
  CMake — confirm `add_subdirectory("backend")` ran (see
  `CMakeLists.txt:103`) and you are NOT on macOS.
- **A `_btree` test runs cleanly but a `_btree` benchmark hangs/crashes
  during heavy concurrent inserts.** Known LeanStore background-insert
  bug, documented in `SESSION_PROGRESS.md:115`. Currently deferred —
  RocksDB-only experiments are unaffected.
- **`chown: invalid group: 'alicial:alicial'`** in Step 3a. The user's
  primary group name is not the same as the username on CloudLab.
  Use `chown $USER:$(id -gn) /mnt/ssd`.

## When NOT to use this doc

- For Docker-based runs see `DOCKER_MEMORY.md` — the `Dockerfile`
  bundles all of the above into a self-contained image and bypasses
  the `perf_event_paranoid` gate by invoking binaries directly.
- For macOS dev see `CLAUDE.md §Build Commands` — only the `_lsm`
  targets compile.
