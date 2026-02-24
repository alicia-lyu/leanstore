FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# ── System packages (mirrors current development VM on Ubuntu 22.04) ──────────
RUN apt-get update && apt-get install -y \
        cmake clang git make python3 \
        libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev \
        libbz2-dev liblz4-dev libzstd-dev liburing-dev \
        librocksdb-dev libwiredtiger-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Source & build ────────────────────────────────────────────────────────────
# Copy only build-relevant source. Changing scripts (run_experiments.sh, etc.)
# will not invalidate this layer or the expensive cmake/make layer below.
WORKDIR /leanstore
COPY CMakeLists.txt .
COPY libs/ libs/
COPY shared-headers/ shared-headers/
COPY backend/ backend/
COPY frontend/ frontend/

# CMake downloads gflags, tabluate, rapidjson, rocksdb from GitHub on first run.
# Requires network access during docker build.
RUN mkdir -p build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. \
    && cd frontend \
    && make geo_btree geo_lsm -j"$(nproc)"

# ── Runtime scripts (copy after build so edits don't bust the build cache) ────
COPY run_experiments.sh .
RUN chmod +x run_experiments.sh

# ── Data & results volumes ────────────────────────────────────────────────────
# /data    — database image files and recover (.json) files
#            bind-mount a host directory: docker run -v /path/on/host:/data ...
# /results — TPut.csv output files
#            bind-mount a host directory: docker run -v /path/on/host:/results ...
RUN mkdir -p /data /results

# Default: run all experiments. Override with: docker run -it <image> bash
ENTRYPOINT ["/leanstore/run_experiments.sh"]
