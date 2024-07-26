#!/bin/bash

# Ensure the script is called with the correct number of parameters
if [ "$#" -ne 6 ] || { [ "$1" != "join" ] && [ "$1" != "merged" ]; }; then
    echo "Usage: $0 <join|merged> <dram_gib> <target_gib> <read_percentage> <scan_percentage> <write_percentage>"
    exit 1
fi

METHOD=$1
DRAM_GIB=$2
TARGET_GIB=$3
READ_PERCENTAGE=$4
SCAN_PERCENTAGE=$5
WRITE_PERCENTAGE=$6

RECOVERY_FILE="./build-release/${METHOD}-target${TARGET_GIB}g.json"
IMAGE_FILE="/home/alicia.w.lyu/tmp/${METHOD}-target${TARGET_GIB}g.image"
LOG_DIR="/home/alicia.w.lyu/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}"

mkdir -p "${LOG_DIR}"
touch "${IMAGE_FILE}"

if [ "$WRITE_PERCENTAGE" -gt 0 ]; then
    PERSIST_FILE="./leanstore.json" # Do not persist
else
    PERSIST_FILE="${RECOVERY_FILE}"
fi

if [ -e "${RECOVERY_FILE}" ]; then
    RUN_FOR_SECONDS=120
    RERUN=false
else 
    RUN_FOR_SECONDS=10
    RERUN=true
fi

echo "************************************************************ NEW EXPERIMENT ************************************************************"

CMD="./build-release/frontend/${METHOD}_tpcc \
--ssd_path=${IMAGE_FILE} --persist_file=${PERSIST_FILE} --recover_file=${RECOVERY_FILE} \
--csv_path=${LOG_DIR} --csv_truncate=true \
--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
--run_for_seconds=${RUN_FOR_SECONDS} --pp_threads=2 \
--dram_gib=${DRAM_GIB} --target_gib=${TARGET_GIB} \
--read_percentage=${READ_PERCENTAGE} --scan_percentage=${SCAN_PERCENTAGE} --write_percentage=${WRITE_PERCENTAGE} \
>> ${LOG_DIR}/output.log"

echo "Running command ${CMD}"

TIME=$(date -u +"%m-%dT%H:%M:%SZ")

echo "${TIME}. Running experiment with method: ${METHOD}, DRAM: ${DRAM_GIB} GiB, target: ${TARGET_GIB} GiB, read: ${READ_PERCENTAGE}%, scan: ${SCAN_PERCENTAGE}%, write: ${WRITE_PERCENTAGE}%" > "${LOG_DIR}/output.log"

eval "${CMD}"

if [ "$RERUN" = true ]; then
    echo "Rerunning experiment after loading..."
    RUN_FOR_SECONDS=120
    eval "${CMD}"
fi

if [ $? -ne 0 ]; then
    echo "Experiment failed"
    rm -f "${PERSIST_FILE}" # Must load next time
    exit 1
fi
