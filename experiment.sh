#!/bin/bash

# Ensure the script is called with the correct number of parameters
if [ "$#" -ne 6 ] || [ "$1" != "join" ] && [ "$1" != "merged" ]; then
    echo "Usage: $0 <join|merged> <dram_gib> <target_gib> <read_percentage> <scan_percentage> <write_percentage>"
    exit 1
fi

METHOD=$1

DRAM_GIB=$2
TARGET_GIB=$3
READ_PERCENTAGE=$4
SCAN_PERCENTAGE=$5
WRITE_PERCENTAGE=$6

echo "************************************************************ NEW EXPERIMENT ************************************************************"
echo "./build-release/frontend/${METHOD}_tpcc \
--ssd_path=/home/alicia.w.lyu/tmp/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.image \
--persist_file=./build-release/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.json \
--recover_file=./build-release/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.json \
--csv_path=./build-release/log --csv_truncate=true \
--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
--run_for_seconds=120 --pp_threads=2 \
--dram_gib=${DRAM_GIB} --target_gib=${TARGET_GIB} \
--read_percentage=${READ_PERCENTAGE} --scan_percentage=${SCAN_PERCENTAGE} --write_percentage=${WRITE_PERCENTAGE} \
>> ~/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.log"


touch ~/tmp/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.image

TIME=$(date -u +"%m-%dT%H:%M:%SZ")

echo "${TIME}. Running experiment with method: ${METHOD}, DRAM: ${DRAM_GIB} GiB, target: ${TARGET_GIB} GiB, read: ${READ_PERCENTAGE}%, scan: ${SCAN_PERCENTAGE}%, write: ${WRITE_PERCENTAGE}%" > ~/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.log

./build-release/frontend/${METHOD}_tpcc \
--ssd_path=/home/alicia.w.lyu/tmp/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.image \
--persist_file=./build-release/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.json \
--recover_file=./build-release/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.json \
--csv_path=./build-release/log --csv_truncate=true --profile_latency=true \
--vi=false --mv=false --isolation_level=ser --optimistic_scan=false \
--run_for_seconds=120 --pp_threads=2 \
--dram_gib=${DRAM_GIB} --target_gib=${TARGET_GIB} \
--read_percentage=${READ_PERCENTAGE} --scan_percentage=${SCAN_PERCENTAGE} --write_percentage=${WRITE_PERCENTAGE} \
>> ~/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.log

if [ $? -ne 0 ]; then
    echo "Experiment failed"
    rm -f "./build-release/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}.json" # Must load next time
    exit 1
fi

mkdir -p ~/logs

move_log_file() {
    local log_file=$1
    suffix=$(basename "$log_file" | sed "s/log//")
    new_log_file=~/logs/${METHOD}-${DRAM_GIB}-${TARGET_GIB}-${READ_PERCENTAGE}-${SCAN_PERCENTAGE}-${WRITE_PERCENTAGE}$suffix
    echo "Moving $log_file to $new_log_file"
    mv -f ${log_file} ${new_log_file}
}

# Find and move log files
find ./build-release -type f -name "log*" | while read -r log_file; do
    move_log_file $log_file
done
