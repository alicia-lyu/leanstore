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
    WRITE_IMAGE_FILE="/home/alicia.w.lyu/tmp/${METHOD}-target${TARGET_GIB}g-write.image" # Do not overwrite the original image
    cp "${IMAGE_FILE}" "${WRITE_IMAGE_FILE}"
    IMAGE_FILE="${WRITE_IMAGE_FILE}"
else
    PERSIST_FILE="${RECOVERY_FILE}"
fi

if [ -e "${RECOVERY_FILE}" ] && [ "$SCAN_PERCENTAGE" -lt 100 ]; then
    TERM_COND="--run_for_seconds=60"
    TRUNC=false
elif [ ! -e "${RECOVERY_FILE}" ]; then
    TERM_COND="--run_for_seconds=90"
    TRUNC=true
else
    TERM_COND="--run_until_tx=240"
    TRUNC=false
fi

echo "************************************************************ NEW EXPERIMENT ************************************************************"

make "./build-release/frontend/${METHOD}_tpcc"

CMD="./build-release/frontend/${METHOD}_tpcc \
--ssd_path=${IMAGE_FILE} --persist_file=${PERSIST_FILE} --recover_file=${RECOVERY_FILE} \
--csv_path=${LOG_DIR}/log --csv_truncate=true --trunc=${TRUNC} \
--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
${TERM_COND} --pp_threads=2 \
--dram_gib=${DRAM_GIB} --target_gib=${TARGET_GIB} --tpcc_warehouse_count=${TARGET_GIB} \
--read_percentage=${READ_PERCENTAGE} --scan_percentage=${SCAN_PERCENTAGE} --write_percentage=${WRITE_PERCENTAGE} \
>> ${LOG_DIR}/output.log"

echo "Running command ${CMD}"

TIME=$(date -u +"%m-%dT%H:%M:%SZ")

echo "${TIME}. Running experiment with method: ${METHOD}, DRAM: ${DRAM_GIB} GiB, target: ${TARGET_GIB} GiB, read: ${READ_PERCENTAGE}%, scan: ${SCAN_PERCENTAGE}%, write: ${WRITE_PERCENTAGE}%" > "${LOG_DIR}/output.log"

eval "${CMD}"

if [ $? -ne 0 ]; then
    echo "Experiment failed, removing persisted json file."
    rm -f "${PERSIST_FILE}" # Must load next time
    exit 1
fi

if [ "$WRITE_PERCENTAGE" -gt 0 ]; then
    rm -f "${WRITE_IMAGE_FILE}"
fi