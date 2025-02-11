import argparse, time, subprocess, shutil
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

class Experiment:
    def __init__(self, executable, dram_gib, target_gib, read_percentage, scan_percentage, write_percentage, selectivity, included_columns, duration, outer_join):
        self.executable = executable
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
        self.timestamp = datetime.now(ZoneInfo("US/Chicago")).strftime("%m-%d-%H-%M")
        self.persist = False
        self.recovery_path = self.build_dir / f"{self.summarize_config()}.json"
        # try-except block to handle path errors
        try:
            self.method = executable.stem.replace('_tpcc', '')
            self.build_dir = executable.parents[1]
            current_dir = Path(__file__).parent.resolve() if "__file__" in globals() else Path.cwd()
            self.home_dir = current_dir.parent.resolve()
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
        log_dir = log_pdir / log_str
        log_dir.mkdir(exist_ok=True)
        return log_dir
    
    def get_image_path(self) -> Path:
        self.image_pdir = self.home_dir / "tmp"
        self.image_archive = Path("/mnt/hdd/merged-index-images")
        self.image_pdir.mkdir(exist_ok=True)
        self.image_archive.mkdir(exist_ok=True)
        
        image_prefix = self.summarize_config()
        is_dir = "rocksdb" in self.method
        target_path = self.image_pdir / image_prefix
        archive_image = self.image_archive / image_prefix

        if archive_image.exists():
            t1 = time.time()
            shutil.copy(archive_image, target_path)
            t2 = time.time()
            print(f"Copying image took {t2 - t1} seconds.")
            if not self.recovery_path.exists() and self.write_percentage == 0:
                self.persist = True
        else:
            target_path.mkdir(exist_ok=True) if is_dir else target_path.touch()
            if self.write_percentage == 0:
                self.persist = True

        return target_path, archive_image
    
    def __str__(self):
        return f"Experiment: {self.method}, DRAM: {self.dram_gib} GiB, Target: {self.target_gib} GiB, Read: {self.read_percentage}%, Scan: {self.scan_percentage}%, Write: {self.write_percentage}%, Selectivity: {self.selectivity}, Included Columns: {self.included_columns}, Duration: {self.duration}, Outer Join: {self.outer_join}"
    
    def to_csv_row(self, dram_gib=None):
        dram_gib = dram_gib or self.dram_gib
        return [
            self.method, self.get_tx_type(), 
            self.selectivity, self.included_columns, self.outer_join, 
            dram_gib, self.target_gib, 
            self.timestamp, self.log_dir.relative_to(self.log_pdir) / self.timestamp
        ]
        
    @classmethod
    def to_csv_header(cls):
        return ["method", "tx", "selectivity", "included_columns", "join", "dram_gib", "target_gib", "timestamp", "file_path"]

    def get_cmd_prefix(self):
        return [
            self.executable,
            f"--ssd_path={self.image_path}", f"--recover_file={self.recovery_path}",
            f"--csv_path={self.log_dir}/{self.timestamp}", "--csv_truncate=true",
            "--vi=false", "--mv=false", "--isolation_level=ser", "--optimistic_scan=false",
            f"--run_for_seconds={self.duration}", "--pp_threads=1", "--worker_threads=2",
            f"--target_gib={self.target_gib}", f"--tpcc_warehouse_count={self.target_gib}",
            f"--semijoin_selectivity={self.selectivity}", f"--outer_join={str(self.outer_join).lower()}"
        ]
    
    def __call__(self):
        print(self)
        
        # For beyond-memory workload without existing image (notable by persist_file), run in-memory and persist it first
        if self.persist and self.dram_gib < self.target_gib * 2:
            # Create DB in memory and persist it
            self.run_command([
                f"--persist_file={self.recovery_path}", f"--dram_gib={self.target_gib * 2}",
                f"--read_percentage=100", f"--scan_percentage=0", f"--write_percentage=0"
            ])
            # Run the experiment with the persisted image
            self.persist = False
            self.run_command([
                f"--persist_file={self.build_dir / "leanstore.json"}", f"--dram_gib={self.dram_gib}",
                f"--read_percentage={self.read_percentage}", f"--scan_percentage={self.scan_percentage}", f"--write_percentage={self.write_percentage}"
            ])
            return
        
        extra_flags = [
            f"--persist_file={self.recovery_path if self.persist else self.build_dir / "leanstore.json"}", 
            f"--dram_gib={self.dram_gib}",
            f"--read_percentage={self.read_percentage}", f"--scan_percentage={self.scan_percentage}", f"--write_percentage={self.write_percentage}"
        ]
        self.run_command(extra_flags)
        
    def run_command(self, extra_flags):
        with open(self.log_dir / "config.csv", "w") as config_f:
            config_f.write(",".join(self.to_csv_header()) + "\n")
            config_f.write(",".join(map(str, self.to_csv_row())) + "\n")
            
        cmd = self.get_cmd_prefix() + extra_flags
        
        stdout_log_path = f"{self.log_dir}/{self.timestamp}.log"
        
        with open(stdout_log_path, 'a') as log_file:
            result = subprocess.run(cmd, stdout=log_file, stderr=None)
            
        if self.archive_image.exists() and cmd.index("--write_percentage=0") != -1 and result.returncode == 0:
            # Compare size to see if the image is different
            if self.image_path.is_dir():
                image_size = sum(f.stat().st_size for f in self.image_path.glob('**/*') if f.is_file())
                archive_size = sum(f.stat().st_size for f in self.archive_image.glob('**/*') if f.is_file())
            else:
                image_size = self.image_path.stat().st_size
                archive_size = self.archive_image.stat().st_size

            if image_size != archive_size:
                print(f"Warning: image size mismatch between {self.image_path} and {self.archive_image}.")

        # Not touching recovery path, it will become meaningless
        # Only change it if any of the b-trees are modified (e.g. schema)
        if not self.persist or result.returncode != 0:
            if self.image_path.is_dir():
                shutil.rmtree(self.image_path)
            else:
                self.image_path.unlink()
        else:
            shutil.copy(self.image_path, self.archive_image)
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run experiment with specified parameters.")
    parser.add_argument('executable', type=Path, help="Path to the executable.")
    parser.add_argument('dram_gib', type=float, help="Amount of DRAM in GiB.")
    parser.add_argument('target_gib', type=int, help="Target GiB.")
    parser.add_argument('read_percentage', type=int, help="Percentage of read operations.")
    parser.add_argument('scan_percentage', type=int, help="Percentage of scan operations.")
    parser.add_argument('write_percentage', type=int, help="Percentage of write operations.")
    parser.add_argument('selectivity', type=int, help="Selectivity percentage.")
    parser.add_argument('included_columns', type=int, help="Number of included columns.")
    parser.add_argument('duration', type=int, nargs='?', default=240, help="Duration to run the experiment (optional, default is 240 seconds).")
    parser.add_argument('outer_join', type=bool, nargs='?', default=False, help="Outer join (optional, default is False).")

    args = parser.parse_args()
    experiment = Experiment(**vars(args))
    experiment()
