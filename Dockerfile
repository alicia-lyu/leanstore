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
WORKDIR /leanstore
COPY . .

# CMake downloads gflags, tabluate, rapidjson, rocksdb from GitHub on first run.
# Requires network access during docker build.
RUN mkdir -p build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. \
    && cd frontend \
    && make geo_btree geo_lsm -j"$(nproc)"

# ── Data & results volumes ────────────────────────────────────────────────────
# /data    — database image files and recover (.json) files
#            bind-mount a host directory: docker run -v /path/on/host:/data ...
# /results — TPut.csv output files
#            bind-mount a host directory: docker run -v /path/on/host:/results ...
RUN mkdir -p /data /results

COPY run_experiments.sh /leanstore/run_experiments.sh
RUN chmod +x /leanstore/run_experiments.sh

# Default: run all experiments. Override with: docker run -it <image> bash
ENTRYPOINT ["/leanstore/run_experiments.sh"]
