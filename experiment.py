import os
import sys
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

def add_suffix_before_extension(original_path, suffix):
    path = Path(original_path)
    return path.with_name(f"{path.stem}{suffix}{path.suffix}")

def get_tx_type(read_percentage, scan_percentage, write_percentage):
    if read_percentage == 100 and scan_percentage == 0 and write_percentage == 0:
        return "read"
    elif read_percentage == 0 and scan_percentage == 100 and write_percentage == 0:
        return "scan"
    elif read_percentage == 0 and scan_percentage == 0 and write_percentage == 100:
        return "write"
    else:
        return f"mixed-{read_percentage}-{scan_percentage}-{write_percentage}"

def get_log_dir(method, dram_gib, target_gib, read_percentage, scan_percentage, write_percentage, order_size, selectivity, included_columns):
    log_dir = "/home/alicia.w.lyu/logs/" + f"{method}-{dram_gib}-{target_gib}-" + get_tx_type(read_percentage, scan_percentage, write_percentage)
    
    if order_size != 5:
        log_dir += f"-size{order_size}"
    if selectivity != 100:
        log_dir += f"-sel{selectivity}"
    if included_columns != 1:
        log_dir += f"-col{included_columns}"

    os.makedirs(log_dir, exist_ok=True)
    return log_dir

def get_recovery_file(method, target_gib, selectivity, included_columns):
    recovery_file = f"{method}-target{target_gib}g.json"
    if selectivity != 100:
        recovery_file = add_suffix_before_extension(recovery_file, f"-sel{selectivity}")
    if included_columns != 1:
        recovery_file = add_suffix_before_extension(recovery_file, f"-col{included_columns}")
    return recovery_file

def get_image(method, target_gib, selectivity, included_columns):
    def get_prefix():
        prefix = f"/home/alicia.w.lyu/tmp/{method}-target{target_gib}g"
        if selectivity != 100:
            prefix = add_suffix_before_extension(prefix, f"-sel{selectivity}")
        if included_columns != 1:
            prefix = add_suffix_before_extension(prefix, f"-col{included_columns}")
        return prefix
            
    if "rocksdb" not in method:
        image_file = f"{get_prefix()}.image"
        Path(image_file).touch()
        return image_file
    else:
        image_dir = f"{get_prefix()}"
        os.makedirs(image_dir, exist_ok=True)
        return image_dir

def main():
    if len(sys.argv) < 10 or not os.path.exists(sys.argv[1]):
        print("Usage: <executable> <dram_gib> <target_gib> <read_percentage> <scan_percentage> <write_percentage> <order_size> <selectivity> <included_columns>")
        sys.exit(1)

    executable_path = Path(sys.argv[1])
    method = executable_path.stem.replace('_tpcc', '')
    build_dir = executable_path.parents[1]
    
    dram_gib = float(sys.argv[2])
    
    target_gib, \
        read_percentage, scan_percentage, write_percentage, \
        order_size, selectivity, included_columns = map(int, sys.argv[3:10])
    
    if len(sys.argv) == 10 or sys.argv[10] == '':
        duration = 240
    else:
        duration = int(sys.argv[10])

    print(f"Method: {method}, Build Directory: {build_dir}, run for seconds: {duration}")
    
    print(f"DRAM: {dram_gib} GiB, target: {target_gib} GiB, read: {read_percentage}%, scan: {scan_percentage}%, write: {write_percentage}%")
    
    print(f"Order Size: {order_size}, Selectivity: {selectivity}, Included Columns: {included_columns}")

    log_dir = get_log_dir(
        method, dram_gib, target_gib, 
        read_percentage, scan_percentage, write_percentage, 
        order_size, selectivity, included_columns)
    
    recovery_file = build_dir / get_recovery_file(method, target_gib, selectivity, included_columns)
    
    image = get_image(method, target_gib, selectivity, included_columns)

    if write_percentage > 0:
        persist_file = Path(f"{build_dir}/leanstore.json")
        write_image_file = add_suffix_before_extension(image, "-write")
        subprocess.run(["cp", image, write_image_file])
        image = write_image_file
    else:
        persist_file = recovery_file

    print(f"Log Directory: {log_dir}, Recovery File: {recovery_file}, Persist File: {persist_file}, Image: {image}")
    
    trunc = not recovery_file.exists()
    
    utc_offset_seconds = -time.timezone if not time.localtime().tm_isdst else -time.altzone
    utc_offset = timedelta(seconds=utc_offset_seconds)

    local_timezone = timezone(utc_offset)
    timestamp = datetime.now(tz=local_timezone).strftime("%m-%d-%H-%M")

    print("************************************************************ NEW EXPERIMENT ************************************************************")

    cmd = [
        executable_path,
        f"--ssd_path={image}", f"--persist_file={persist_file}", f"--recover_file={recovery_file}",
        f"--csv_path={log_dir}/{timestamp}", "--csv_truncate=true", f"--trunc={str(trunc).lower()}",
        "--vi=false", "--mv=false", "--isolation_level=ser", "--optimistic_scan=false",
        f"--run_for_seconds={duration}", "--pp_threads=2",
        f"--dram_gib={dram_gib}", f"--target_gib={target_gib}", f"--tpcc_warehouse_count={target_gib}",
        f"--read_percentage={read_percentage}", f"--scan_percentage={scan_percentage}", f"--write_percentage={write_percentage}",
        f"--order_size={order_size}", f"--semijoin_selectivity={selectivity}",
    ]

    print(f"Running command {' '.join(map(str, cmd))}")
    
    stdout_log_path = f"{log_dir}/{timestamp}.log"

    with open(stdout_log_path, 'w') as log_file:
        log_file.write(f"{timestamp}. Running experiment with method: {method}, DRAM: {dram_gib} GiB, target: {target_gib} GiB, read: {read_percentage}%, scan: {scan_percentage}%, write: {write_percentage}%\n")
    
    with open(stdout_log_path, 'a') as log_file:
        result = subprocess.run(cmd, stdout=log_file, stderr=None)

    if result.returncode != 0:
        print("Experiment failed, you need to remove the persisted json file.")
        sys.exit(1)

    if write_percentage > 0:
        os.remove(write_image_file)

if __name__ == "__main__":
    main()
