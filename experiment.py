import os
import sys
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import argparse

# Path utilities
def add_suffix_before_extension(original_path, suffix):
    path = Path(original_path)
    return path.with_name(f"{path.stem}{suffix}{path.suffix}")

# TX Type determination
def get_tx_type(read_percentage, scan_percentage, write_percentage, locality_read):
    if read_percentage == 100 and scan_percentage == 0 and write_percentage == 0:
        return "read-locality" if locality_read else "read"
    elif read_percentage == 0 and scan_percentage == 100 and write_percentage == 0:
        return "scan"
    elif read_percentage == 0 and scan_percentage == 0 and write_percentage == 100:
        return "write"
    else:
        return f"mixed-{read_percentage}-{scan_percentage}-{write_percentage}"

# Log directory creation
def get_log_dir(method, dram_gib, target_gib, read_percentage, scan_percentage, write_percentage, order_size, selectivity, included_columns, locality_read, outer_join):
    log_dir = f"/home/alicia.w.lyu/logs/{method}/{dram_gib}-{target_gib}-{get_tx_type(read_percentage, scan_percentage, write_percentage, locality_read)}"
    if outer_join:
        log_dir += "-outer"
    if order_size != 5:
        log_dir += f"-size{order_size}"
    if selectivity != 100:
        log_dir += f"-sel{selectivity}"
    if included_columns != 1:
        log_dir += f"-col{included_columns}"
    os.makedirs(log_dir, exist_ok=True)
    print(f"Log Directory: {log_dir}")
    return log_dir

# Recovery file creation
def get_recovery_file(method, target_gib, selectivity, included_columns, rocksdb, outer_join):
    recovery_file = f"{method}-target{target_gib}g.json"
    if outer_join:
        recovery_file = add_suffix_before_extension(recovery_file, "-outer")
    if selectivity != 100:
        recovery_file = add_suffix_before_extension(recovery_file, f"-sel{selectivity}")
    if included_columns != 1:
        recovery_file = add_suffix_before_extension(recovery_file, f"-col{included_columns}")
    if rocksdb:
        recovery_file = add_suffix_before_extension(recovery_file, "-rocksdb")
    print(f"Recovery File: {recovery_file}")
    return recovery_file

# Image file/directory creation
def get_image(method, target_gib, selectivity, included_columns, outer_join, write_percentage):
    def get_prefix():
        prefix = f"/home/alicia.w.lyu/tmp/{method}-target{target_gib}g"
        if outer_join:
            prefix = add_suffix_before_extension(prefix, "-outer")
        if selectivity != 100:
            prefix = add_suffix_before_extension(prefix, f"-sel{selectivity}")
        if included_columns != 1:
            prefix = add_suffix_before_extension(prefix, f"-col{included_columns}")
        return prefix

    prefix = get_prefix()
    if "rocksdb" not in method:
        image_file = f"{prefix}.image"
        Path(image_file).touch()
        print(f"Image File: {image_file}")
        if write_percentage > 0:
            write_image_file = add_suffix_before_extension(image, "-write")
            print(f"Write Image File: {write_image_file}")
            subprocess.run(["cp", "-f", image, write_image_file])
            image = write_image_file
        return image_file
    else:
        image_dir = f"{prefix}"
        os.makedirs(image_dir, exist_ok=True)
        print(f"Image Directory: {image_dir}")
        if write_percentage > 0:
            write_image_dir = f"{image}-write"
            print(f"Write Image Directory: {write_image_dir}")
            subprocess.run(["cp", "-f", "-r", image, write_image_dir])
            image = write_image_dir
        return image_dir

# Command runner
def run_experiment_cmd(args, method, dram_temp, duration_temp, build_dir):
    log_dir = get_log_dir(
        method, dram_temp, args.target_gib, 
        args.read_percentage, args.scan_percentage, args.write_percentage, 
        args.order_size, args.selectivity, args.included_columns, args.locality_read, args.outer_join)
    
    recovery_file, image, trunc, persist_file = get_recovery_et_al(
        build_dir, method, args.target_gib, args.selectivity, args.included_columns, args.outer_join, args.write_percentage, dram_temp)
    
    utc_offset_seconds = -time.timezone if not time.localtime().tm_isdst else -time.altzone
    utc_offset = timedelta(seconds=utc_offset_seconds)
    local_timezone = timezone(utc_offset)
    timestamp = datetime.now(tz=local_timezone).strftime("%m-%d-%H-%M")

    print("************************************************************ NEW EXPERIMENT ************************************************************")
    
    cmd = [
        args.executable,
        f"--ssd_path={image}", f"--persist_file={persist_file}", f"--recover_file={recovery_file}",
        f"--csv_path={log_dir}/{timestamp}", "--csv_truncate=true", f"--trunc={str(trunc).lower()}",
        "--vi=false", "--mv=false", "--isolation_level=ser", "--optimistic_scan=false",
        f"--run_for_seconds={duration_temp}", "--pp_threads=2",
        f"--dram_gib={dram_temp}", f"--target_gib={args.target_gib}", f"--tpcc_warehouse_count={args.target_gib}",
        f"--read_percentage={args.read_percentage}", f"--scan_percentage={args.scan_percentage}", f"--write_percentage={args.write_percentage}", f"--locality_read={str(args.locality_read).lower()}",
        f"--order_size={args.order_size}", f"--semijoin_selectivity={args.selectivity}",
        f"--outer_join={str(args.outer_join).lower()}"
    ]

    stdout_log_path = f"{log_dir}/{timestamp}.log"
    print(f"Running command {' '.join(map(str, cmd))}\n")
    
    with open(stdout_log_path, 'w') as log_file:
        log_file.write(f"{timestamp}. Running experiment with method: {method}, DRAM: {dram_temp} GiB, target: {args.target_gib} GiB, read: {args.read_percentage}%, scan: {args.scan_percentage}%, write: {args.write_percentage}%\n")
        log_file.write(f"Running command {' '.join(map(str, cmd))}")
    
    with open(stdout_log_path, 'a') as log_file:
        result = subprocess.run(cmd, stdout=log_file, stderr=None)

    if result.returncode != 0:
        print("Experiment failed, you need to remove the persisted json file.")
        sys.exit(1)

    if "rocksdb" in method:
        shutil.rmtree(image)

    if args.write_percentage > 0 and "rocksdb" not in method:
        with open(stdout_log_path, 'a') as log_file:
            log_file.write(f"Size of {image}: {os.path.getsize(image) / (1024**3)} GiB.\n")
            os.remove(image)

def get_recovery_et_al(build_dir, method, target_gib, selectivity, included_columns, outer_join, write_percentage, dram_gib):
    image = get_image(method, target_gib, selectivity, included_columns, outer_join, write_percentage)
    
    if dram_gib >= target_gib * 2: # Start over and persist---as a chance to refresh stored data
        recovery_file = "./leanstore.json" 
        trunc = True
        if write_percentage > 0:
            persist_file = recovery_file
        else:
            persist_file = build_dir / get_recovery_file(method, target_gib, selectivity, included_columns, "rocksdb" in method, outer_join)
        return recovery_file, image, trunc, persist_file
    else: # Only recover in beyond-memory workload and do not persist
        recovery_file = build_dir / get_recovery_file(method, target_gib, selectivity, included_columns, "rocksdb" in method, outer_join)
        assert(recovery_file.exists())
        trunc = False
        persist_file = "./leanstore.json"

    return recovery_file, image, trunc, persist_file

def main():
    parser = argparse.ArgumentParser(description="Run experiment with specified parameters.")
    parser.add_argument('executable', type=Path, help="Path to the executable.")
    parser.add_argument('dram_gib', type=float, help="Amount of DRAM in GiB.")
    parser.add_argument('target_gib', type=int, help="Target GiB.")
    parser.add_argument('read_percentage', type=int, help="Percentage of read operations.")
    parser.add_argument('scan_percentage', type=int, help="Percentage of scan operations.")
    parser.add_argument('write_percentage', type=int, help="Percentage of write operations.")
    parser.add_argument('order_size', type=int, help="Order size.")
    parser.add_argument('selectivity', type=int, help="Selectivity percentage.")
    parser.add_argument('included_columns', type=int, help="Number of included columns.")
    parser.add_argument('duration', type=int, nargs='?', default=240, help="Duration to run the experiment (optional, default is 240 seconds).")
    parser.add_argument('locality_read', type=bool, nargs='?', default=False, help="Locality read (optional, default is False).")
    parser.add_argument('outer_join', type=bool, nargs='?', default=False, help="Outer join (optional, default is False).")
    
    args = parser.parse_args()

    if not args.executable.exists():
        parser.error(f"The executable {args.executable} does not exist.")
    
    method = args.executable.stem.replace('_tpcc', '')
    print(f"Executable: {args.executable}, Method: {method}")
    build_dir = args.executable.parents[1]

    # Set the actual experiment duration
    duration = min(args.duration, 180) if args.dram_gib >= args.target_gib * 2 else args.duration or 240
    print(f"Method: {method}, Build Directory: {build_dir}, run for seconds: {duration}")
    
    print(f"DRAM: {args.dram_gib} GiB, target: {args.target_gib} GiB, read: {args.read_percentage}%, scan: {args.scan_percentage}%, write: {args.write_percentage}%")
    
    print(f"Order Size: {args.order_size}, Selectivity: {args.selectivity}, Included Columns: {args.included_columns}")

    # If memory is smaller than target_gib, run the in-memory workload first
    if args.dram_gib < args.target_gib * 2 and "rocksdb" not in method:
        recovery_file = build_dir / get_recovery_file(method, args.target_gib, args.selectivity, args.included_columns, "rocksdb" in method, args.outer_join)
        if not recovery_file.exists():
            print(f"Recovery file {recovery_file} does not exist. Run the in-memory workload first.")
            run_experiment_cmd(args, method, 16, 1, build_dir)

    # Run the actual experiment
    print("Running the actual experiment...")
    run_experiment_cmd(args, method, args.dram_gib, duration, build_dir)
    
if __name__ == "__main__":
    main()