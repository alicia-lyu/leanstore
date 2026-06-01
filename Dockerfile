FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# ── System packages ───────────────────────────────────────────────────────────
# LeanStore build deps + DBToaster build deps (Java, Boost) + Python plotting.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang git make python3 python3-pip \
        libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev \
        libbz2-dev liblz4-dev libzstd-dev liburing-dev \
        librocksdb-dev libwiredtiger-dev \
        openjdk-11-jre-headless \
        g++ wget tar ca-certificates \
        libboost-dev libboost-serialization-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Python plotting dependencies ──────────────────────────────────────────────
RUN pip3 install --no-cache-dir \
        pandas numpy matplotlib seaborn Jinja2 PyYAML

# ── DBToaster 2.3 prebuilt distribution ──────────────────────────────────────
RUN wget -q https://dbtoaster.github.io/dist/dbtoaster_2.3_linux.tgz \
        -O /tmp/dbt.tgz \
    && tar -xzf /tmp/dbt.tgz -C /opt \
    && rm /tmp/dbt.tgz
ENV PATH="/opt/dbtoaster/bin:${PATH}"

# ── TPC-H dbgen (used by DBToaster data generation) ──────────────────────────
RUN git clone --depth 1 https://github.com/electrum/tpch-dbgen.git /opt/tpch-dbgen \
    && make -C /opt/tpch-dbgen CC=gcc DATABASE=DB2 MACHINE=LINUX WORKLOAD=TPCH
ENV PATH="/opt/tpch-dbgen:${PATH}" \
    DSS_CONFIG="/opt/tpch-dbgen" \
    DSS_PATH="/dbtoaster/data_files"

# ── LeanStore source & build ──────────────────────────────────────────────────
# Copy only build-relevant source so script/config edits don't bust this layer.
WORKDIR /leanstore
COPY CMakeLists.txt .
COPY libs/ libs/
COPY shared-headers/ shared-headers/
COPY backend/ backend/
COPY frontend/ frontend/

# -march=haswell: AVX2 floor, no AVX-512. CloudLab c220g2 has Haswell-EP CPUs;
# this image must not bake AVX-512 instructions that would SIGILL on that ISA.
# CMake downloads gflags, tabluate, rapidjson from GitHub on first run.
RUN mkdir -p build \
    && cd build \
    && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
             -DCMAKE_C_FLAGS="-march=haswell" \
             -DCMAKE_CXX_FLAGS="-march=haswell" \
             .. \
    && cd frontend \
    && make \
        q3_lsm q3_btree \
        q3i_lsm q3i_btree \
        q5_lsm q5_btree \
        q5i_lsm q5i_btree \
        q10_lsm q10_btree \
        q10i_lsm q10i_btree \
        refresh_sales_lsm refresh_sales_btree \
        -j"$(nproc)"

# ── DBToaster refresh_sales binary ────────────────────────────────────────────
# Built from the in-tree dbtoaster/ directory using the DBToaster codegen +
# prebuilt distribution already installed above.
COPY dbtoaster/ /leanstore/dbtoaster/
WORKDIR /leanstore/dbtoaster
# rm -rf build: never reuse a host-side cmake cache that may have been copied
# in (CMakeCache.txt bakes an absolute source path and aborts on mismatch).
RUN rm -rf build \
    && mkdir -p data_files \
    && make refresh_sales.hpp \
    && make build

# ── Runtime scripts ───────────────────────────────────────────────────────────
WORKDIR /leanstore
COPY experiments/ experiments/
COPY paper-data/ paper-data/
RUN chmod +x experiments/docker_entrypoint.sh

# ── Runtime directories ───────────────────────────────────────────────────────
# /mnt/ssd  — bind-mount point for SSD image files (host provides; ROTA=0).
# /mnt/hdd  — bind-mount point for HDD image files (tpch-headline-hdd cell).
# /results  — bind-mount point for output CSVs and paper-ready PDFs.
RUN mkdir -p /mnt/ssd /mnt/hdd /results

ENTRYPOINT ["/leanstore/experiments/docker_entrypoint.sh"]
