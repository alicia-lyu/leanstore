import os
import sys
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
import argparse

def add_suffix_before_extension(original_path, suffix):
    path = Path(original_path)
    return path.with_name(f"{path.stem}{suffix}{path.suffix}")

def get_tx_type(read_percentage, scan_percentage, write_percentage, locality_read):
    if read_percentage == 100 and scan_percentage == 0 and write_percentage == 0:
        if locality_read:
            return "read-locality"
        else:
            return "read"
    elif read_percentage == 0 and scan_percentage == 100 and write_percentage == 0:
        return "scan"
    elif read_percentage == 0 and scan_percentage == 0 and write_percentage == 100:
        return "write"
    else:
        return f"mixed-{read_percentage}-{scan_percentage}-{write_percentage}"

def get_log_dir(method, dram_gib, target_gib, read_percentage, scan_percentage, write_percentage, order_size, selectivity, included_columns, locality_read):
    log_dir = "/home/alicia.w.lyu/logs/" + f"{method}-{dram_gib}-{target_gib}-" + get_tx_type(read_percentage, scan_percentage, write_percentage, locality_read)
    
    if order_size != 5:
        log_dir += f"-size{order_size}"
    if selectivity != 100:
        log_dir += f"-sel{selectivity}"
    if included_columns != 1:
        log_dir += f"-col{included_columns}"

    os.makedirs(log_dir, exist_ok=True)
    print(f"Log Directory: {log_dir}")
    return log_dir

def get_recovery_file(method, target_gib, selectivity, included_columns):
    recovery_file = f"{method}-target{target_gib}g.json"
    if selectivity != 100:
        recovery_file = add_suffix_before_extension(recovery_file, f"-sel{selectivity}")
    if included_columns != 1:
        recovery_file = add_suffix_before_extension(recovery_file, f"-col{included_columns}")
    print(f"Recovery File: {recovery_file}")
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
        print(f"Image File: {image_file}")
        return image_file
    else:
        image_dir = f"{get_prefix()}"
        os.makedirs(image_dir, exist_ok=True)
        print(f"Image Directory: {image_dir}")
        return image_dir

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
    
    args = parser.parse_args()

    if not args.executable.exists():
        parser.error(f"The executable {args.executable} does not exist.")
    
    method = args.executable.stem.replace('_tpcc', '')
    print(f"Executable: {args.executable}, Method: {method}")
    build_dir = args.executable.parents[1]

    if args.dram_gib >= args.target_gib * 2:
        duration = min(args.duration, 180)
    elif args.duration == 0:
        duration = 240
    else:
        duration = args.duration
    
    print(f"Method: {method}, Build Directory: {build_dir}, run for seconds: {duration}")
    
    print(f"DRAM: {args.dram_gib} GiB, target: {args.target_gib} GiB, read: {args.read_percentage}%, scan: {args.scan_percentage}%, write: {args.write_percentage}%")
    
    print(f"Order Size: {args.order_size}, Selectivity: {args.selectivity}, Included Columns: {args.included_columns}")

    log_dir = get_log_dir(
        method, args.dram_gib, args.target_gib, 
        args.read_percentage, args.scan_percentage, args.write_percentage, 
        args.order_size, args.selectivity, args.included_columns, args.locality_read)
    
    recovery_file = build_dir / get_recovery_file(method, args.target_gib, args.selectivity, args.included_columns)
    
    trunc = not recovery_file.exists()
    
    image = get_image(method, args.target_gib, args.selectivity, args.included_columns)

    if args.write_percentage > 0:
        persist_file = f"./leanstore.json"
        write_image_file = add_suffix_before_extension(image, "-write")
        
        if args.dram_gib >= args.target_gib * 2: # Force load instead of recovery
            trunc = True
            recovery_file = "./leanstore.json"
            assert('rocksdb' not in method)
            Path(write_image_file).touch()
        else:
            subprocess.run(["cp", "-f", "-r", image, write_image_file]) # Force overwrite
            
        image = write_image_file
    else:
        persist_file = recovery_file
    
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
        f"--run_for_seconds={duration}", "--pp_threads=2",
        f"--dram_gib={args.dram_gib}", f"--target_gib={args.target_gib}", f"--tpcc_warehouse_count={args.target_gib}",
        f"--read_percentage={args.read_percentage}", f"--scan_percentage={args.scan_percentage}", f"--write_percentage={args.write_percentage}", f"--locality_read={args.locality_read}",
        f"--order_size={args.order_size}", f"--semijoin_selectivity={args.selectivity}",
    ]

    print(f"Running command {' '.join(map(str, cmd))}")
    
    stdout_log_path = f"{log_dir}/{timestamp}.log"

    with open(stdout_log_path, 'w') as log_file:
        log_file.write(f"{timestamp}. Running experiment with method: {method}, DRAM: {args.dram_gib} GiB, target: {args.target_gib} GiB, read: {args.read_percentage}%, scan: {args.scan_percentage}%, write: {args.write_percentage}%\n")
        log_file.write(f"Running command {' '.join(map(str, cmd))}")
    
    with open(stdout_log_path, 'a') as log_file:
        result = subprocess.run(cmd, stdout=log_file, stderr=None)

    if result.returncode != 0:
        print("Experiment failed, you need to remove the persisted json file.")
        sys.exit(1)

    if args.write_percentage > 0:
        if os.path.isdir(write_image_file):
            shutil.rmtree(write_image_file)
        else:
            with open(stdout_log_path, 'a') as log_file:
                log_file.write(f"Size of {write_image_file}: {os.path.getsize(write_image_file) / (1024**3)} GiB.\n")
            os.remove(write_image_file)

if __name__ == "__main__":
    main()