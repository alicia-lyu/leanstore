# DBToaster refresh_sales baseline — self-contained build.
# Copy-adapted from geodb-dbtoaster/Dockerfile (2-stage: builder -> runner).
#
#   docker build -t refresh-sales-dbt --build-arg SF=1 .
#   docker run --rm refresh-sales-dbt              # runs the memory-pressure ladder
#
# SF is the TPC-H scale factor for the baked dataset. For the ~5 GB working-set
# run (paper anchor) pin SF empirically first (see ../results/README.md); the
# default SF=1 is for a correctness / smoke build.

# ---------------------------------------------------------------------------
FROM ubuntu:22.04 AS builder
ARG SF=1
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-11-jre-headless g++ cmake make wget tar git ca-certificates \
        libboost-dev libboost-serialization-dev \
    && rm -rf /var/lib/apt/lists/*

# DBToaster 2.3 prebuilt distribution -> /opt/dbtoaster (bin/dbtoaster, lib/dbt_c++).
RUN wget -q https://dbtoaster.github.io/dist/dbtoaster_2.3_linux.tgz -O /tmp/dbt.tgz \
    && tar -xzf /tmp/dbt.tgz -C /opt && rm /tmp/dbt.tgz
ENV PATH="/opt/dbtoaster/bin:${PATH}"

# TPC-H dbgen (standard tools-kit mirror).
RUN git clone --depth 1 https://github.com/electrum/tpch-dbgen.git /opt/tpch-dbgen \
    && make -C /opt/tpch-dbgen CC=gcc DATABASE=DB2 MACHINE=LINUX WORKLOAD=TPCH
ENV PATH="/opt/tpch-dbgen:${PATH}" DSS_CONFIG="/opt/tpch-dbgen" DSS_PATH="/app/data_files"

WORKDIR /app
COPY . /app

# Generate data (base + RF1/RF2), concat stream files, codegen, build.
RUN mkdir -p data_files \
    && (cd /opt/tpch-dbgen && ./dbgen -b dists.dss -s ${SF} -f -O /app/data_files || true) \
    && SF=${SF} make data || true \
    && make refresh_sales.hpp \
    && make build

# ---------------------------------------------------------------------------
FROM ubuntu:22.04 AS runner
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        libstdc++6 libboost-serialization-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/build/refresh_sales /app/build/refresh_sales
COPY --from=builder /app/data_files /app/data_files
COPY --from=builder /app/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
