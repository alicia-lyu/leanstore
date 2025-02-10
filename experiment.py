import argparse, os, time
from pathlib import Path
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo
    
parser = argparse.ArgumentParser(description="Run experiment with specified parameters.")
parser.add_argument('executable', type=Path, help="Path to the executable.")
parser.add_argument('dram_gib', type=float, help="Amount of DRAM in GiB.")
parser.add_argument('target_gib', type=int, help="Target GiB.")
parser.add_argument('read_percentage', type=int, help="Percentage of read operations.")
parser.add_argument('scan_percentage', type=int, help="Percentage of scan operations.")
parser.add_argument('write_percentage', type=int, help="Percentage of write operations.")
# parser.add_argument('order_size', type=int, help="Order size.")
parser.add_argument('selectivity', type=int, help="Selectivity percentage.")
parser.add_argument('included_columns', type=int, help="Number of included columns.")
parser.add_argument('duration', type=int, nargs='?', default=240, help="Duration to run the experiment (optional, default is 240 seconds).")
# parser.add_argument('locality_read', type=bool, nargs='?', default=False, help="Locality read (optional, default is False).")
parser.add_argument('outer_join', type=bool, nargs='?', default=False, help="Outer join (optional, default is False).")

args = parser.parse_args()

class Experiment:
    def __init__(self, executable, dram_gib, target_gib, read_percentage, scan_percentage, write_percentage, selectivity, included_columns, duration, outer_join):
        # Shorten duration for in-memory workloads
        self.duration = min(duration or 180, 180) if dram_gib >= target_gib * 2 else (duration or 240)
        self.dram_gib = dram_gib
        self.target_gib = target_gib
        self.read_percentage = read_percentage
        self.scan_percentage = scan_percentage
        self.write_percentage = write_percentage
        self.selectivity = selectivity
        self.included_columns = included_columns
        self.outer_join = outer_join
        # try-except block to handle path errors
        try:
            self.method = executable.stem.replace('_tpcc', '')
            self.build_dir = executable.parents[1]
            current_dir = Path(__file__).parent.resolve() if "__file__" in globals() else Path.cwd()
            self.home_dir = current_dir.parent.resolve()
            self.recovery_path, self.persist_path, self.trunc = self.get_recovery_persist()
            self.log_dir = self.get_log_dir()
            self.image_path, self.archive_image = self.get_image_path()
        except Exception as e:
            print(f"An error occurred: {e}")
    
    def get_tx_type(self):
        if self.read_percentage == 100 and self.scan_percentage == 0 and self.write_percentage == 0:
            return "read"
        elif self.read_percentage == 0 and self.scan_percentage == 100 and self.write_percentage == 0:
            return "scan"
        elif self.read_percentage == 0 and self.scan_percentage == 0 and self.write_percentage == 100:
            return "write"
        else:
            return f"mixed-{self.read_percentage}-{self.scan_percentage}-{self.write_percentage}"
    
    def summarize_config(self):
        sum_str = f"{self.method}/{self.target_gib}"
        sum_str += "-outer" if self.outer_join else ""
        sum_str += f"-sel{self.selectivity}" if self.selectivity != 100 else ""
        sum_str += f"-col{self.included_columns}" if self.included_columns != 1 else ""
        return sum_str
        
    def summarize_runtime_config(self):
        return f"{self.get_tx_type()}-{self.dram_gib}"
            
    def get_log_dir(self) -> Path:
        log_pdir = self.home_dir / "logs"
        log_pdir.mkdir(exist_ok=True)
        log_str = self.summarize_config() + self.summarize_runtime_config()
        log_dir = self.log_pdir / log_str
        log_dir.mkdir(exist_ok=True)
        return log_dir
    
    def get_recovery_persist(self):
        recovery_path = self.build_dir / f"{self.summarize_config()}.json"
        # A necessary condition for recovery but subject to existence of image
        if recovery_path.exists():
            trunc = False # Do not truncate the image
            persist_path = self.build_dir / "leanstore.json" # Do not persist as the image exists already
        else:
            trunc = True
            persist_path = recovery_path
            
        return recovery_path, persist_path, trunc
    
    def get_image_path(self) -> Path:
        self.image_pdir = self.home_dir / "tmp"
        self.image_archive = Path("/mnt/hdd/merged-index-images")
        self.image_pdir.mkdir(exist_ok=True)
        self.image_archive.mkdir(exist_ok=True)
        
        image_prefix = self.summarize_config()
        is_dir = "rocksdb" in self.method
        target_path = self.image_pdir / image_prefix if is_dir else self.image_pdir / f"{image_prefix}.image"
        archive_image = self.image_archive / image_prefix if is_dir else self.image_archive / f"{image_prefix}.image"

        if archive_image.exists():
            t1 = time.time()
            shutil.copy(archive_image, target_path)
            t2 = time.time()
            print(f"Copying image took {t2 - t1} seconds.")
        else:
            target_path.mkdir(exist_ok=True) if is_dir else target_path.touch()

        return target_path, archive_image
    
    def __str__(self):
        return f"Experiment: {self.method}, DRAM: {self.dram_gib} GiB, Target: {self.target_gib} GiB, Read: {self.read_percentage}%, Scan: {self.scan_percentage}%, Write: {self.write_percentage}%, Selectivity: {self.selectivity}, Included Columns: {self.included_columns}, Duration: {self.duration}, Outer Join: {self.outer_join}"

    def __call__(self):
        # Get the current time in the system's local timezone
        timestamp = datetime.now(ZoneInfo("US/Chicago")).strftime("%m-%d-%H-%M")
        print(self)
        
        
        
        # Delete image if write_percentage > 0
        # Otherwise, dump image to HDD: "/mnt/hdd/merged-index-images" if size is different from the existing one
        
        
        
