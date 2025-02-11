import argparse, time, subprocess, shutil
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

class DBConfig:
    def __init__(self, executable: Path, target_gib, selectivity, included_columns, outer_join):
        self.executable = executable
        self.target_gib = target_gib
        self.selectivity = selectivity
        self.included_columns = included_columns
        self.outer_join = outer_join
        self.recovery_path = self.build_dir / f"{self.summarize()}.json"
        
        try:
            self.method = executable.stem.replace('_tpcc', '')
            self.build_dir = executable.parents[1]
            current_dir = Path(__file__).parent.resolve() if "__file__" in globals() else Path.cwd()
            self.home_dir = current_dir.parent.resolve()
            self.image_path, self.archive_image = self.get_image_path()
        except Exception as e:
            print(f"An error occurred: {e}")
        
    def summarize(self):
        sum_str = f"{self.method}/{self.target_gib}"
        sum_str += "-outer" if self.outer_join else ""
        sum_str += f"-sel{self.selectivity}" if self.selectivity != 100 else ""
        sum_str += f"-col{self.included_columns}" if self.included_columns != 1 else ""
        return sum_str
        
    def __str__(self):
        return f"********** DB Config **********\n" + f"Method: {self.method}, Target: {self.target_gib} GiB, Selectivity: {self.selectivity}, Included Columns: {self.included_columns}, Outer Join: {self.outer_join}"
    
    def get_image_path(self) -> Path:
        self.image_pdir = self.home_dir / "tmp"
        self.image_archive = Path("/mnt/hdd/merged-index-images")
        self.image_pdir.mkdir(exist_ok=True)
        self.image_archive.mkdir(exist_ok=True)
        
        image_prefix = self.summarize()
        is_dir = "rocksdb" in self.method
        target_path = self.image_pdir / image_prefix
        archive_image = self.image_archive / image_prefix

        if archive_image.exists():
            t1 = time.time()
            shutil.copy(archive_image, target_path)
            t2 = time.time()
            print(f"Copying image took {t2 - t1} seconds.")
        else:
            target_path.mkdir(exist_ok=True) if is_dir else target_path.touch()

        return target_path, archive_image
    
    def get_flags(self):
        return [
        f"--ssd_path={self.image_path}", f"--recover_file={self.recovery_path}",
        "--vi=false", "--mv=false", "--isolation_level=ser", "--optimistic_scan=false",
        f"--target_gib={self.target_gib}", f"--tpcc_warehouse_count={self.target_gib}",
        f"--semijoin_selectivity={self.selectivity}", f"--outer_join={str(self.outer_join).lower()}"
    ]
    
    @classmethod
    def to_csv_header(cls):
        return ["method", "selectivity", "included_columns", "join", "target_gib"]
    
    def to_csv_row(self, dram_gib=None):
        return [
            self.method, 
            self.selectivity, self.included_columns, self.outer_join, 
            self.target_gib
        ]

class RuntimeConfig:
    def __init__(self, dbconfig, dram_gib, read_percentage, scan_percentage, write_percentage, duration):
        self.dbconfig: DBConfig = dbconfig
        self.dram_gib = dram_gib
        self.read_percentage = read_percentage
        self.scan_percentage = scan_percentage
        self.write_percentage = write_percentage
        self.duration = min(duration or 180, 180) if dram_gib >= dbconfig.target_gib * 2 else (duration or 240)
        self.timestamp = datetime.now(ZoneInfo("US/Chicago")).strftime("%m-%d-%H-%M")
        
        self.log_dir = self.get_log_dir()
        
        if self.write_percentage == 0 and (not self.dbconfig.archive_image.exists() or not self.dbconfig.recovery_path.exists()):
            self.persist = True
        else:
            self.persist = False
        
    def summarize(self):
        return f"{self.get_tx_type()}-{self.dram_gib}"
    
    def __str__(self):
        return f"********** Runtime Config **********\n" + f"DRAM: {self.dram_gib} GiB, Read: {self.read_percentage}%, Scan: {self.scan_percentage}%, Write: {self.write_percentage}%, Duration: {self.duration}"
    
    def get_tx_type(self):
        if self.read_percentage == 100 and self.scan_percentage == 0 and self.write_percentage == 0:
            return "read"
        elif self.read_percentage == 0 and self.scan_percentage == 100 and self.write_percentage == 0:
            return "scan"
        elif self.read_percentage == 0 and self.scan_percentage == 0 and self.write_percentage == 100:
            return "write"
        else:
            return f"mixed-{self.read_percentage}-{self.scan_percentage}-{self.write_percentage}"
    
    def get_csv_prefix(self):
        return f"{self.log_dir}/{self.timestamp}"
    
    @classmethod
    def to_csv_header(cls):
        return ["tx", "dram_gib", "timestamp", "file_path"]
    
    def to_csv_row(self, dram_gib=None):
        dram_gib = dram_gib or self.dram_gib
        return [self.get_tx_type(), dram_gib, self.timestamp, self.dbconfig.log_dir.relative_to(self.dbconfig.log_pdir) / self.timestamp]
    
    def get_log_dir(self) -> Path:
        log_pdir = self.dbconfig.home_dir / "logs"
        log_pdir.mkdir(exist_ok=True)
        log_str = self.dbconfig.summarize() + self.summarize()
        log_dir = log_pdir / log_str
        log_dir.mkdir(exist_ok=True)
        return log_dir
    
    def get_flags(self):
        return [
            f"--csv_path={self.get_csv_prefix()}", "--csv_truncate=true", "--pp_threads=1", "--worker_threads=2",
        ], [
            f"--persist_file={self.dbconfig.recovery_path if self.persist else self.build_dir / "leanstore.json"}", 
            f"--dram_gib={self.dram_gib}",
            f"--read_percentage={self.read_percentage}", f"--scan_percentage={self.scan_percentage}", f"--write_percentage={self.write_percentage}", f"--run_for_seconds={self.duration}"
        ]

class Experiment:
    def __init__(self, executable, dram_gib, target_gib, read_percentage, scan_percentage, write_percentage, selectivity, included_columns, duration, outer_join):
        self.dbconfig = DBConfig(executable, target_gib, selectivity, included_columns, outer_join)
        self.runtimeconfig = RuntimeConfig(self.dbconfig, dram_gib, read_percentage, scan_percentage, write_percentage, duration)
    
    def __str__(self):
        return str(self.dbconfig) + "\n" + str(self.runtimeconfig)
    
    def two_phase(self):
        return self.runtimeconfig.persist and self.runtimeconfig.dram_gib < self.dbconfig.target_gib * 2
        
    def get_2p_flags(self):
        return [
            f"--persist_file={self.dbconfig.recovery_path}", f"--dram_gib={self.dbconfig.target_gib * 2}",
            f"--read_percentage=100", f"--scan_percentage=0", f"--write_percentage=0", f"--run_for_seconds=1"
        ], [
            f"--persist_file={self.dbconfig.build_dir / "leanstore.json"}", f"--dram_gib={self.runtimeconfig.dram_gib}",
            f"--read_percentage={self.runtimeconfig.read_percentage}", f"--scan_percentage={self.runtimeconfig.scan_percentage}", f"--write_percentage={self.runtimeconfig.write_percentage}", f"--run_for_seconds={self.runtimeconfig.duration}"
        ]
    
    def __call__(self):
        print(self)
        # For beyond-memory workload without existing image (notable by persist_file), run in-memory and persist it first
        if self.runtimeconfig.two_phase():
            # Create DB in memory and persist it
            flags1, flags2 = self.runtimeconfig.get_2p_flags()
            self.run_command(flags1)
            # Run the experiment with the persisted image
            self.runtimeconfig.persist = False
            self.run_command(flags2)
            return
        
        self.run_command()
    
    @classmethod
    def to_csv_header(cls):
        return DBConfig.to_csv_header() + RuntimeConfig.to_csv_header()
    
    def to_csv_row(self):
        return [*self.dbconfig.to_csv_row(), *self.runtimeconfig.to_csv_row()]
    
    def check_image_size(self, write_enabled, returncode):
        image_path = self.dbconfig.image_path
        archive_image = self.dbconfig.archive_image
        
        if archive_image.exists() and not write_enabled and returncode == 0:
        # Compare size to see if the image is different
            if image_path.is_dir():
                image_size = sum(f.stat().st_size for f in image_path.glob('**/*') if f.is_file())
                archive_size = sum(f.stat().st_size for f in archive_image.glob('**/*') if f.is_file())
            else:
                image_size = image_path.stat().st_size
                archive_size = archive_image.stat().st_size

            if image_size != archive_size:
                print(f"Warning: image size mismatch between {self.image_path} and {self.archive_image}.")
                
    def cleanup_image(self):
        image_path = self.dbconfig.image_path
        if image_path.is_dir():
            shutil.rmtree(image_path)
        else:
            image_path.unlink()
    
    def copy2archive(self):
        shutil.copy(self.dbconfig.image_path, self.dbconfig.archive_image)
        
    def run_command(self, extra_flags=None):
        db_flags = self.dbconfig.get_flags()
        runtime_flags, extra_flags_temp = self.runtimeconfig.get_flags()
        extra_flags = extra_flags or extra_flags_temp
        
        with open(self.runtimeconfig.log_dir / "config.csv", "w") as config_f:
            config_f.write(",".join(self.to_csv_header()) + "\n")
            config_f.write(",".join(map(str, self.to_csv_row())) + "\n")
            
        cmd = [self.dbconfig.executable, *db_flags, *runtime_flags, *extra_flags]
        
        stdout_log_path = f"{self.runtimeconfig.get_csv_prefix()}.log"
        
        with open(stdout_log_path, 'a') as log_file:
            result = subprocess.run(cmd, stdout=log_file, stderr=None)
            
        self.dbconfig.check_image_size(cmd.index("--write_percentage=0") != -1, result.returncode)

        # Not touching recovery path, it will become meaningless
        # Only change it if any of the b-trees are modified (e.g. schema)
        if not self.runtimeconfig.persist or result.returncode != 0:
            self.cleanup_image()
        else:
            self.copy2archive()
        
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
