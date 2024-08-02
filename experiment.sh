#!/bin/bash

# Ensure the script is called with the correct number of parameters
if [ "$#" -ne 9 ] || [ ! -e $1 ] ; then
    echo "Usage: $0 <executable> <dram_gib> <target_gib> <read_percentage> <scan_percentage> <write_percentage> <order_size> <selectivity> <included_columns>"
    exit 1
fi

EXECUTABLE_PATH=$1
# Extract the method name using basename and parameter expansion
METHOD=$(basename "$EXECUTABLE_PATH"_tpcc)
# Extract the build directory using parameter expansion
BUILD_DIR=$(dirname "$(dirname "$EXECUTABLE_PATH")")
echo "Method: $METHOD"
echo "Build Directory: $BUILD_DIR"

DRAM_GIB=$2
TARGET_GIB=$3
READ_PERCENTAGE=$4
SCAN_PERCENTAGE=$5
WRITE_PERCENTAGE=$6
ORDER_SIZE=$7
SELECTIVITY=$8
INCLUDED_COLUMNS=$9

if [ "$READ_PERCENTAGE" -eq 100 ] && [ "$SCAN_PERCENTAGE" -eq 0 ] && [ "$WRITE_PERCENTAGE" -eq 0 ]; then
  LOG_DIR="/home/alicia.w.lyu/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-read"
elif [ "$READ_PERCENTAGE" -eq 0 ] && [ "$SCAN_PERCENTAGE" -eq 100 ] && [ "$WRITE_PERCENTAGE" -eq 0 ]; then
  LOG_DIR="/home/alicia.w.lyu/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-scan"
elif [ "$READ_PERCENTAGE" -eq 0 ] && [ "$SCAN_PERCENTAGE" -eq 0 ] && [ "$WRITE_PERCENTAGE" -eq 100 ]; then
  LOG_DIR="/home/alicia.w.lyu/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-write"
else
  LOG_DIR="/home/alicia.w.lyu/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}"
fi

if [ "$ORDER_SIZE" -ne 5 ]; then
  LOG_DIR="${LOG_DIR}-size${ORDER_SIZE}"
fi

add_suffix_before_extension() {
    local original_path="$1"
    local suffix="$2"
    local dir=$(dirname "$original_path")
    local filename=$(basename "$original_path" .${original_path##*.})
    local extension="${original_path##*.}"
    local new_path="${dir}/${filename}${suffix}.${extension}"
    echo "$new_path"
}

RECOVERY_FILE="./$BUILD_DIR/${METHOD}-target${TARGET_GIB}g.json"
IMAGE_FILE="/home/alicia.w.lyu/tmp/${METHOD}-target${TARGET_GIB}g.image"

if [ "$SELECTIVITY" -ne 100 ]; then
  RECOVERY_FILE=$(add_suffix_before_extension "$RECOVERY_FILE" "-sel${SELECTIVITY}")
  IMAGE_FILE=$(add_suffix_before_extension "$IMAGE_FILE" "-sel${SELECTIVITY}")
  LOG_DIR="${LOG_DIR}-sel${SELECTIVITY}"
fi

if [ "$INCLUDED_COLUMNS" -ne 1 ]; then
  # RECOVERY_FILE=$(add_suffix_before_extension "$RECOVERY_FILE" "-col${INCLUDED_COLUMNS}") # No need, because build dir is different
  IMAGE_FILE=$(add_suffix_before_extension "$IMAGE_FILE" "-col${INCLUDED_COLUMNS}")
  LOG_DIR="${LOG_DIR}-col${INCLUDED_COLUMNS}"
fi

mkdir -p "${LOG_DIR}"
touch "${IMAGE_FILE}"

if [ "$WRITE_PERCENTAGE" -gt 0 ]; then
    PERSIST_FILE="./leanstore.json" # Do not persist
    WRITE_IMAGE_FILE="${IMAGE_FILE}-write" # Do not overwrite the original image
    cp "${IMAGE_FILE}" "${WRITE_IMAGE_FILE}"
    IMAGE_FILE="${WRITE_IMAGE_FILE}"
else
    PERSIST_FILE="${RECOVERY_FILE}"
fi

if [ -e "${RECOVERY_FILE}" ]; then
    TERM_COND="--run_for_seconds=360"
    TRUNC=false
elif [ ! -e "${RECOVERY_FILE}" ]; then
    TERM_COND="--run_for_seconds=480"
    TRUNC=true
fi

echo "************************************************************ NEW EXPERIMENT ************************************************************"

# make "./$BUILD_DIR/frontend/${METHOD}_tpcc"

CMD="${EXECUTABLE_PATH} \
--ssd_path=${IMAGE_FILE} --persist_file=${PERSIST_FILE} --recover_file=${RECOVERY_FILE} \
--csv_path=${LOG_DIR}/log --csv_truncate=true --trunc=${TRUNC} \
--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
${TERM_COND} --pp_threads=2 \
--dram_gib=${DRAM_GIB} --target_gib=${TARGET_GIB} --tpcc_warehouse_count=${TARGET_GIB} \
--read_percentage=${READ_PERCENTAGE} --scan_percentage=${SCAN_PERCENTAGE} --write_percentage=${WRITE_PERCENTAGE} \
--order_size=${ORDER_SIZE} --semijoin_selectivity=${SELECTIVITY} \
>> ${LOG_DIR}/output.log"

echo "Running command ${CMD}"

TIME=$(date -u +"%m-%dT%H:%M:%SZ")

echo "${TIME}. Running experiment with method: ${METHOD}, DRAM: ${DRAM_GIB} GiB, target: ${TARGET_GIB} GiB, read: ${READ_PERCENTAGE}%, scan: ${SCAN_PERCENTAGE}%, write: ${WRITE_PERCENTAGE}%" > "${LOG_DIR}/output.log"

eval "${CMD}"

if [ $? -ne 0 ]; then
    echo "Experiment failed, you need to remove the persisted json file."
    # rm -f "${PERSIST_FILE}" # Must load next time
    exit 1
fi

if [ "$WRITE_PERCENTAGE" -gt 0 ]; then
    rm -f "${WRITE_IMAGE_FILE}"
fi