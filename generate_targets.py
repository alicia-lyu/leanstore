#!/usr/bin/env python3
"""
generate_targets.py

Auto-generates a Makefile fragment with build, csv-dir, image, recovery and LLDB rules.
"""

from pathlib import Path
import json
from typing import List

vscode_launch_obj = {
    "version": "0.2.0",
    "configurations": []
}

build_dirs = ["build", "build-debug"]
exec_names = ["geo_leanstore", "geo_lsm"]
shared_flags: dict[str, str] = {
    "vi": "false",
    "mv": "false",
    "isolation_level": "ser",
    "optimistic_scan": "false",
    "pp_threads": "1",
    "csv_truncate": "false",
    "worker_threads": "2"
}
# defined in the Makefile
NUMJONS_ENV: str = "$(NUMJOBS)"
CMAKE_DEBUG_ENV: str = "$(CMAKE_DEBUG) $(CMAKE_OPTIONS)"
CMAKE_REL_ENV: str = "$(CMAKE_RELWITHDEBINFO) $(CMAKE_OPTIONS)"
DRAM_ENV: str = "$(dram)"
SCALE_ENV: str = "$(scale)"

def kv_to_str(kv_dict: dict[str, str]) -> str:
    """Converts a dictionary of key-value pairs to a string."""
    return " ".join([f"--{k}={v}" for k, v in kv_dict.items()])
    
def get_build_vars(build_dir: Path):
    return CMAKE_DEBUG_ENV if "debug" in str(build_dir) else CMAKE_REL_ENV
    
def get_exec_vars(build_dir: Path, exec_fname: str) -> tuple[Path, Path, Path, Path]:
    exec_path = build_dir / "frontend" / exec_fname
    if "lsm" in exec_fname:
        image_path = Path("/mnt/hdd/rocksdb_images") / build_dir / exec_fname / f"{SCALE_ENV}"
    else:
        image_path = Path("/mnt/hdd/leanstore_images") / build_dir / exec_fname / f"{SCALE_ENV}.image"
    recover_file = build_dir / exec_fname / f"{SCALE_ENV}.json" # do recover
    runtime_dir = build_dir / exec_fname / f"{SCALE_ENV}-in-{DRAM_ENV}"
    return (
        exec_path,
        runtime_dir,
        image_path,
        recover_file
    )
    
def get_image_command(lsm: bool, image_path: Path) -> tuple[str, str]:
    """Returns the command to create an image file/dir."""
    if lsm:
        create_image_cmd = f"mkdir -p {image_path}"
        copy_image_cmd = f"rm -rf {image_path}_temp && cp -R -f {image_path} {image_path}_temp"
    else:
        create_image_cmd = f"mkdir -p {image_path.parent} && touch {image_path}"
        copy_image_cmd = f"cp -f {image_path} {image_path}_temp"
    return create_image_cmd, copy_image_cmd
    
def get_loading_files(exec_fname: str) -> List[str]:
    src_dir = DIFF_DIRS[exec_fname] if exec_fname in DIFF_DIRS else exec_fname
    file_base = f"./frontend/tpc-h/{src_dir}/load"
    loading_files = []
    for ext in ['tpp', 'hpp', 'cpp']:
        file = f"{file_base}.{ext}"
        if Path(file).exists():
            loading_files.append(file)
    return loading_files

executables = []
runtime_dirs = []

class Experiment:
    build_dir: Path
    exec_fname: str
    exec_path: Path
    runtime_dir: Path
    image_path: Path
    recover_file: Path
    class_flags: List[tuple[str,str]]
    
    sep = "-" * 20

    def __init__(self, build_dir: str, exec_fname: str):
        self.build_dir = Path(build_dir)
        self.exec_fname = exec_fname
        self.exec_path, self.runtime_dir, self.image_path, self.recover_file = get_exec_vars(Path(build_dir), exec_fname)
        # global lists
        executables.append(self.exec_path)
        runtime_dirs.append(self.runtime_dir)
        # class flags
        self.class_flags = shared_flags.copy()
        self.class_flags["csv_path"] = str(self.runtime_dir)
        self.class_flags["csv_truncate"] = "false"
        self.class_flags["log_progress"] = "true" if "debug" in str(self.build_dir) else "false"
    
    def generate_all_targets(self) -> None:
        """Generates all Makefile targets for this experiment."""
        big_sep = "=" * 20
        print(f"# {big_sep} Generating targets for {self.exec_fname} in {self.build_dir} {big_sep}")
        self.generate_executable()
        self.generate_runtime_dir()
        self.generate_image()
        self.generate_recover_file()
        if "debug" not in str(self.build_dir):
            self.run_experiment()
        else:
            self.debug_experiment()

    def makefile_subsection(self, title: str) -> None:
        print(f"#{self.sep} {title} {self.sep}")
        
    def console_print_subsection(self, title: str) -> None:
        print(f'\t@echo "{self.sep} {title} {self.sep}"')
    
    def generate_executable(self) -> None:
        self.makefile_subsection("Generate executable")
        # rule to compile executable
        print(f"{self.exec_path}: check_perf_event_paranoid") # 
        self.console_print_subsection(f"Building {self.exec_path}")
        cmake_cmd = get_build_vars(self.build_dir)
        print(
            f'\tcd {self.build_dir}/frontend && {cmake_cmd}',
            f'&& make {self.exec_fname} -j{NUMJONS_ENV}', sep=" "
        )
        print()
        
    def generate_runtime_dir(self) -> None:
        self.makefile_subsection("Generate CSV runtime dir")
        # rule to create runtime dir
        print(f"{self.runtime_dir}:")
        self.console_print_subsection(f"Creating CSV runtime dir {self.runtime_dir}")
        print(f"\tmkdir -p {self.runtime_dir}")
        print()
        
    def generate_image(self) -> None:
        self.makefile_subsection("Generate image file/dir")
        create_image_cmd, copy_image_cmd = get_image_command("lsm" in self.exec_fname, self.image_path)
        
        # rule to create image file/dir
        print(f"{self.image_path}: check_perf_event_paranoid {self.runtime_dir}")
        self.console_print_subsection(f"Creating image file/dir {self.image_path}")
        print(f"\t{create_image_cmd}")

        # rule to copy image file to a temporary "test field"
        if "lsm" not in self.exec_fname:
            print(f"{self.image_path}_temp: {self.image_path} FORCE") # force duplicate
            self.console_print_subsection(f"Copying image file {self.image_path} to {self.image_path}_temp")
            print(f"\t{copy_image_cmd}")

    def remaining_flags(self, recover_file: str, persist_file: str, trunc: bool, ssd_path: str, scale: int, dram_gib: int) -> dict[str, str]:
        flags = {
            "recover_file": recover_file,
            "persist_file": persist_file,
            "trunc": "true" if trunc else "false",
            "ssd_path": str(ssd_path),
            "tpch_scale_factor": str(scale),
            "dram_gib": str(dram_gib)
        }
        return flags
        
    def generate_recover_file(self) -> None:
        self.makefile_subsection("Generate recovery file")
        loading_files = get_loading_files(self.exec_fname)
        loading_files_str = " ".join(loading_files)
        # rule to load database and create recovery file
        print(f"{self.recover_file}: {LOADING_META_FILE} {loading_files_str} {self.image_path} {self.runtime_dir}")
        self.console_print_subsection(f"Persisting data to {self.recover_file}")
        prefix = "lldb -o run -- " if "debug" in str(self.build_dir) else ""
        rem_flags = self.remaining_flags(
                recover_file="./leanstore.json", # do not recover
                persist_file=self.recover_file, # do persist
                trunc=True,
                ssd_path=self.image_path,
                scale=SCALE_ENV,
                dram_gib=8
            )
        print(
            f"\t{prefix}{self.exec_path}", 
            kv_to_str(self.class_flags),
            kv_to_str(rem_flags),
            f"2>{self.runtime_dir}/stderr.txt",
            sep=" "
        )
        print()

    def experiment_flags(self) -> tuple[dict[str, str], str]:
        
        image_dep = self.image_path if "lsm" in self.exec_fname else f"{self.image_path}_temp"
        rem_flags = self.remaining_flags(
            recover_file=self.recover_file, # do recover
            persist_file="./leanstore.json", # do not persist
            trunc=False,
            ssd_path=image_dep, # duplicate image
            scale=SCALE_ENV,
            dram_gib="$(dram)"
        )
        return rem_flags, image_dep
        
    def run_experiment(self) -> None:
        self.makefile_subsection("Run experiment")
        separate_runs = [f"{self.exec_fname}_{str(i)}" for i in STRUCTURE_OPTIONS[self.exec_fname]]
        separate_runs_str = " ".join(separate_runs)
        rem_flags, image_dep = self.experiment_flags()
        
        # rule to run the experiment
        print(f"{self.exec_fname}: {separate_runs_str}")
        # rules for separate runs
        for structure in STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_{structure}: check_perf_event_paranoid {self.runtime_dir} {self.exec_path} {self.recover_file} {image_dep}")
            print(f"\ttouch {self.runtime_dir}/structure{structure}.log")
            print(
                f'\tscript -q -c "{self.exec_path}',
                kv_to_str(self.class_flags),
                kv_to_str(rem_flags),
                f"--storage_structure={structure}",
                f'2>{self.runtime_dir}/stderr.txt\"',
                f'{self.runtime_dir}/structure{structure}.log',
                sep=" "
            )
            print()
    
    def debug_experiment(self) -> None:
        print(f"#{self.sep} Debug experiment {self.sep}")
        separate_runs = [f"{self.exec_fname}_lldb_{str(i)}" for i in STRUCTURE_OPTIONS[self.exec_fname]]
        separate_runs_str = " ".join(separate_runs)
        rem_flags, img_dep = self.experiment_flags()
        # replace dram with 1 in vscode flags
        # vscode_flags = self.class_flags.copy() + rem_flags.copy()
        vscode_flags: dict[str, str] = self.class_flags.copy()
        vscode_flags.update(rem_flags.copy())
        for k, v in vscode_flags.items():
            vscode_flags[k] = str(v).replace("$(dram)", "1").replace("$(scale)", "10")
        
        # rule to run the experiment in LLDB
        print(f"{self.exec_fname}_lldb: {separate_runs_str}")
        # rules for separate runs
        for structure in STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_lldb_{structure}: {self.exec_path} {self.recover_file} check_perf_event_paranoid {img_dep}")
            print(
                f"\tlldb -o run --",
                f"{self.exec_path}",
                kv_to_str(self.class_flags),
                kv_to_str(rem_flags),
                f"--storage_structure={structure}",
                sep=" "
            )
            print()
        
            vscode_configs = {
                "name": f"{self.exec_fname}_{structure}",
                "type": "lldb",
                "request": "launch",
                "program": f"${{workspaceFolder}}/{self.exec_path}",
                "args": [f"--{k}={v}" for k, v in vscode_flags.items()],
                "cwd": "${workspaceFolder}",
                "stopOnEntry": False
            }
            vscode_launch_obj["configurations"].append(vscode_configs)

LOADING_META_FILE = "./frontend/tpc-h/tpch_workload.hpp"

DIFF_DIRS = {
 "geo_lsm": "geo_join"
}
            
STRUCTURE_OPTIONS = {
    "geo_leanstore": [0, 1, 2, 3],
    "geo_lsm": [0, 1, 2, 3]
}

def main() -> None:
    """Emits the entire Makefile snippet to stdout."""
    print("# --- auto-generated by generate_targets.py; DO NOT EDIT ---\n")
    for build_dir in build_dirs:
        for exec_name in exec_names:
            exp = Experiment(build_dir, exec_name)
            exp.generate_all_targets()
    
    print(f"executables: {' '.join([str(e) for e in executables])}\n")
    print("run_all: " + " ".join(exec_names))
    print("lldb_all: " + " ".join([f"{e}_lldb" for e in exec_names]))
    print(f"clean_runtime_dirs:")
    for dir in runtime_dirs:
        print(f"\trm -rf {dir}")

    # phony declaration
    phony = ["FORCE", "check_perf_event_paranoid", "executables", "clean_runtime_dirs", "run_all", "lldb_all"] + exec_names + [f"{e}_lldb" for e in exec_names]
    print(f".PHONY: {' '.join(phony)}")
    
    vscode_launch = open(".vscode/launch.json", "w")
    json.dump(vscode_launch_obj, vscode_launch, indent=2)
    vscode_launch.close()

if __name__ == "__main__":
    main()