#!/usr/bin/env python3
"""
generate_targets.py

Auto-generates a Makefile fragment with build, csv-dir, image, recovery and LLDB rules.
"""

from dataclasses import dataclass
from pathlib import Path
import json
from typing import List

vscode_launch_obj = {
    "version": "0.2.0",
    "configurations": []
}

@dataclass(frozen=True)
class Config:
    build_dirs = ["build", "build-debug"]
    exec_names = ["basic_join", "basic_group", "basic_group_variant", "basic_join_group", "geo_join", "geo_lsm"]
    numjobs: str = "$(NUMJOBS)"
    cmake_options: str = "$(CMAKE_OPTIONS)"
    cmake_debug: str = "$(CMAKE_DEBUG)"
    cmake_relwithdebinfo: str = "$(CMAKE_RELWITHDEBINFO)"
    leanstore_flags: str = "--vi=false --mv=false \
                       --isolation_level=ser --optimistic_scan=false \
                       --pp_threads=1 --csv_truncate=false --worker_threads=2 \
                       --tpch_scale_factor=$(scale)"
    dram: str = "$(dram)"
    scale: str = "$(scale)"


cfg = Config()

def print_section(title: str) -> None:
    """Prints a styled section header."""
    sep = "=" * 20
    print(f"# {sep} {title} {sep}")
    
def get_cmake_cmd(build_dir: str) -> str:
    """Selects the correct CMake invocation based on directory name."""
    return cfg.cmake_debug if "debug" in build_dir else cfg.cmake_relwithdebinfo

def get_executable_path(build_dir: str, exe: str) -> Path:
    """Returns the path to the compiled executable."""
    return Path(build_dir) / "frontend" / exe

def get_runtime_dir(build_dir: str, exe: str) -> Path:
    """
    Returns a dedicated runtime directory for CSV/log output,
    separate from 'frontend' so it's not confused with the binary.
    """
    return Path(build_dir) / exe / f"{cfg.scale}-in-{cfg.dram}"


def get_image_file(build_dir:str, exe: str) -> Path:
    """Returns the path to the SSD image file for a given executable."""
    if "lsm" in exe:
        # LSM executables use a different image directory
        p =  Path("/mnt/hdd/rocksdb_images") / build_dir / exe / f"{cfg.scale}"
        return p, f"mkdir -p {p}", f"cp -R -f {p} {p}_temp"
    else:
        p =  Path("/mnt/hdd/leanstore_images") / build_dir / exe / f"{cfg.scale}.image"
        return p, f"mkdir -p {p.parent} && touch {p}", f"cp -f {p} {p}_temp"

executables = []
runtime_dirs = []

class Experiment:
    build_dir: str
    exec_fname: str
    cmake_cmd: str
    exec_path: Path
    runtime_dir: Path
    image_file: Path
    create_image_cmd: str
    copy_image_cmd: str
    loading_files: List[str]
    recover_file: str
    separate_runs: List[str]
    
    sep = "-" * 20

    def __init__(self, build_dir: str, exec_fname: str):
        self.build_dir = build_dir
        self.exec_fname = exec_fname
        self.cmake_cmd = get_cmake_cmd(build_dir)
        self.exec_path = get_executable_path(build_dir, exec_fname)
        executables.append(self.exec_path)
        self.runtime_dir = get_runtime_dir(build_dir, exec_fname)
        runtime_dirs.append(self.runtime_dir)
        self.image_file, self.create_image_cmd, self.copy_image_cmd = get_image_file(build_dir, exec_fname)
        src_dir = DIFF_DIRS[exec_fname] if exec_fname in DIFF_DIRS else exec_fname
        file_base = f"./frontend/tpc-h/{src_dir}/load"
        self.loading_files = []
        for ext in ['tpp', 'hpp']:
            file = f"{file_base}.{ext}"
            if Path(file).exists():
                self.loading_files.append(file)
        self.recover_file = Path(build_dir) / exec_fname / f"{cfg.scale}.json"
        self.separate_runs = [f"{self.exec_fname}_{str(i)}" for i in STRUCTURE_OPTIONS[self.exec_fname]]
    
    def generate_all_targets(self) -> None:
        """Generates all Makefile targets for this experiment."""
        print_section(f"Generating targets for {self.exec_fname} in {self.build_dir}")
        self.generate_executable()
        self.generate_runtime_dir()
        self.generate_image()
        self.generate_recover_files()
        if "debug" not in self.build_dir:
            self.run_experiment()
        else:
            self.debug_experiment()

    def generate_executable(self) -> None:
        print(f"#{self.sep} Generate executable {self.sep}")
        print(f"{self.exec_path}: check_perf_event_paranoid") # check_perf_event_paranoid is PHONY, force rebuild
        print(f'\t@echo "{self.sep} Building {self.exec_path} {self.sep}"')
        print(
            f'\tcd {self.build_dir}/frontend && {self.cmake_cmd} {cfg.cmake_options} '
            f'&& make {self.exec_fname} -j{cfg.numjobs}\n'
        )
        
    def generate_runtime_dir(self) -> None:
        print(f"#{self.sep} Generate runtime dir {self.sep}")
        print(f"{self.runtime_dir}:")
        print(f'\t@echo "{self.sep} Creating CSV runtime dir {self.runtime_dir} {self.sep}"')
        print(f"\tmkdir -p {self.runtime_dir}\n")
        
    def generate_image(self) -> None:
        print(f"#{self.sep} Generate image file/dir {self.sep}")
        print(f"{self.image_file}:")
        print(f'\t@echo "{self.sep} Touching a new image file {self.image_file} {self.sep}"')
        print(f"\t{self.create_image_cmd}")
        print(f"{self.image_file}_temp: {self.image_file} FORCE") # force duplicate
        print(f'\t@echo "{self.sep} Duplicating temporary image file {self.image_file} for transactions {self.sep}"')
        print(f"\t{self.copy_image_cmd}")
        
    def generate_recover_files(self) -> None:
        print(f"#{self.sep} Generate recovery file {self.sep}")
        loading_files_str = " ".join(self.loading_files)
        print(f"{self.recover_file}: {LOADING_META_FILE} {loading_files_str} {self.image_file}")
        # for f in [recover, LOADING_META_FILE, loading_file(exe), img]:
            # print(f'\techo {f}; stat -c %y "{f}"')
        print(f'\techo "{self.sep} Persisting data to {self.recover_file} {self.sep}"')
        print(f"\tmkdir -p {self.runtime_dir}")
        prefix = "lldb -o run -- " if "debug" in self.build_dir else ""
        print(
            f"\t{prefix}{self.exec_path} {cfg.leanstore_flags} "
            f"--csv_path={self.runtime_dir} --persist_file={self.recover_file} "
            f"--trunc=true --ssd_path={self.image_file} --dram_gib=8 2>{self.runtime_dir}/stderr.txt\n"
        )
        
    def run_experiment(self) -> None:
        print(f"#{self.sep} Run experiment {self.sep}")
        separate_runs_str = " ".join(self.separate_runs)
        print(f"{self.exec_fname}: {separate_runs_str}")

        img_temp = f"{self.image_file}_temp"
        for structure in STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_{structure}: check_perf_event_paranoid {self.runtime_dir} {self.exec_path} {self.recover_file} {img_temp}")
            print(f"\ttouch {self.runtime_dir}/structure{structure}.log")
            print(
                f'\tscript -q -c "{self.exec_path} {cfg.leanstore_flags} '
                f"--storage_structure={structure} "
                f'--csv_path={self.runtime_dir} --recover_file={self.recover_file} '
                f'--ssd_path={img_temp} --dram_gib=$(dram) 2>{self.runtime_dir}/stderr.txt" {self.runtime_dir}/structure{structure}.log'
            )
    
    def debug_experiment(self) -> None:
        print(f"#{self.sep} Debug experiment {self.sep}")
        separate_runs_str = " ".join(self.separate_runs)
        print(f"{self.exec_fname}_lldb: {separate_runs_str}")

        img_temp = f"{self.image_file}_temp"
        args = list(cfg.leanstore_flags.split()) + [
            f"--csv_path={self.runtime_dir}",
            f"--recover_file={self.recover_file}",
            f"--ssd_path={img_temp}",
            f"--dram_gib=1",
        ]
        for i, arg in enumerate(args):
            arg = arg.replace("$(dram)", "1")
            arg = arg.replace("$(scale)", "10")
            args[i] = arg
        
        # separate runs
        for structure in STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_lldb_{structure}: {self.exec_path} {self.recover_file} check_perf_event_paranoid {img_temp}")
            print(
                f"\tlldb -o run -- "
                f"{self.exec_path} {cfg.leanstore_flags} "
                f"--storage_structure={structure} "
                f"--csv_path={self.runtime_dir} --recover_file={self.recover_file} "
                f"--ssd_path={img_temp} --dram_gib=$(dram)"
            )
            args_run = args + [f"--storage_structure={structure}"]
        
            exp_configs = {
                "name": f"{self.exec_fname}_{structure}",
                "type": "lldb",
                "request": "launch",
                "program": f"${{workspaceFolder}}/{self.exec_path}",
                "args": args_run,
                "cwd": "${workspaceFolder}",
                "stopOnEntry": False
            }
            vscode_launch_obj["configurations"].append(exp_configs)

LOADING_META_FILE = "./frontend/tpc-h/tpch_workload.hpp"

DIFF_DIRS = {
 "basic_group_variant": "basic_group",
 "geo_lsm": "geo_join"
}
            
STRUCTURE_OPTIONS = {
    "basic_join": [0, 1, 2],
    "basic_group": [0, 2],
    "basic_group_variant": [0, 2],
    "basic_join_group": [0, 2],
    "geo_join": [0, 1, 2, 3],
    "geo_lsm": [0, 1, 2, 3]
}

def main() -> None:
    """Emits the entire Makefile snippet to stdout."""
    print("# --- auto-generated by generate_targets.py; DO NOT EDIT ---\n")
    for build_dir in cfg.build_dirs:
        for exec_name in cfg.exec_names:
            exp = Experiment(build_dir, exec_name)
            exp.generate_all_targets()
    
    print(f"executables: {' '.join([str(e) for e in executables])}\n")
    print("run_all: " + " ".join(cfg.exec_names))
    print("lldb_all: " + " ".join([f"{e}_lldb" for e in cfg.exec_names]))
    print(f"clean_runtime_dirs:")
    for dir in runtime_dirs:
        print(f"\trm -rf {dir}")

    # phony declaration
    phony = ["FORCE", "check_perf_event_paranoid", "executables", "clean_runtime_dirs", "run_all", "lldb_all"] + cfg.exec_names + [f"{e}_lldb" for e in cfg.exec_names]
    print(f".PHONY: {' '.join(phony)}")
    
    vscode_launch = open(".vscode/launch.json", "w")
    json.dump(vscode_launch_obj, vscode_launch, indent=2)
    vscode_launch.close()

if __name__ == "__main__":
    main()