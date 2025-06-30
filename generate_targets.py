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
shared_flags: List[tuple[str,str]] = [
    ("vi", "false"),
    ("mv", "false"),
    ("isolation_level", "ser"),
    ("optimistic_scan", "false"),
    ("pp_threads", "1"),
    ("csv_truncate", "false"),
    ("worker_threads", "2")
]
# defined in the Makefile
NUMJONS_ENV: str = "$(NUMJOBS)"
CMAKE_DEBUG_ENV: str = "$(CMAKE_DEBUG) $(CMAKE_OPTIONS)"
CMAKE_REL_ENV: str = "$(CMAKE_RELWITHDEBINFO) $(CMAKE_OPTIONS)"
DRAM_ENV: str = "$(dram)"

def print_section(title: str) -> None:
    """Prints a styled section header."""
    sep = "=" * 20
    print(f"# {sep} {title} {sep}")
    
def get_build_vars(build_dir: Path):
    return CMAKE_DEBUG_ENV if "debug" in str(build_dir) else CMAKE_REL_ENV
    
def get_exec_vars(build_dir: Path, exec_fname: str):
    exec_path = build_dir / "frontend" / exec_fname
    if "lsm" in exec_fname:
        scale = 10
        image_path = Path("/mnt/hdd/rocksdb_images") / build_dir / exec_fname / f"{scale}"
        recover_file = build_dir / exec_fname / f"leanstore.json" # no recover
    else:
        scale = 10
        image_path = Path("/mnt/hdd/leanstore_images") / build_dir / exec_fname / f"{scale}.image"
        recover_file = build_dir / exec_fname / f"{scale}.json" # do recover
        
    runtime_dir = build_dir / exec_fname / f"{scale}-in-{DRAM_ENV}"
    return (
        exec_path,
        runtime_dir,
        scale,
        image_path,
        recover_file
    )
    
def get_image_command(lsm: bool, image_path: Path) -> str:
    """Returns the command to create an image file/dir."""
    if lsm:
        create_image_cmd = f"mkdir -p {image_path}"
        copy_image_cmd = None
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
    scale: int
    class_flags: List[tuple[str,str]]
    
    sep = "-" * 20

    def __init__(self, build_dir: str, exec_fname: str):
        self.build_dir = Path(build_dir)
        self.exec_fname = exec_fname
        self.exec_path, self.runtime_dir, self.scale, self.image_path, self.recover_file = get_exec_vars(Path(build_dir), exec_fname)
        executables.append(self.exec_path)
        runtime_dirs.append(self.runtime_dir)
        self.class_flags = shared_flags.copy() + [
            ("csv_path", str(self.runtime_dir)),
            ("csv_truncate", "false"),
            ("log_progress", "true" if "debug" in str(self.build_dir) else "false")
        ]
    
    def generate_all_targets(self) -> None:
        """Generates all Makefile targets for this experiment."""
        print_section(f"Generating targets for {self.exec_fname} in {self.build_dir}")
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
        
    def print_subsection(self, title: str) -> None:
        print(f'\t@echo "{self.sep} {title} {self.sep}"')
    
    def generate_executable(self) -> None:
        self.makefile_subsection("Generate executable")
        print(f"{self.exec_path}: check_perf_event_paranoid") # 
        self.print_subsection(f"Building {self.exec_path}")
        cmake_cmd = get_build_vars(self.build_dir)
        print(
            f'\tcd {self.build_dir}/frontend && {cmake_cmd}',
            f'&& make {self.exec_fname} -j{NUMJONS_ENV}', sep=" "
        )
        print()
        
    def generate_runtime_dir(self) -> None:
        self.makefile_subsection("Generate CSV runtime dir")
        print(f"{self.runtime_dir}:")
        self.print_subsection(f"Creating CSV runtime dir {self.runtime_dir}")
        print(f"\tmkdir -p {self.runtime_dir}")
        print()
        
    def generate_image(self) -> None:
        self.makefile_subsection("Generate image file/dir")
        print(f"{self.image_path}:")
        # print(f'\t@echo "{self.sep} Touching a new image file {self.image_path} {self.sep}"')
        create_image_cmd, copy_image_cmd = get_image_command("lsm" in self.exec_fname, self.image_path)
        self.print_subsection(f"Creating image file/dir {self.image_path}")
        print(f"\t{create_image_cmd}")
        
        if "lsm" not in self.exec_fname: # lsm does not copy image
            print(f"{self.image_path}_temp: {self.image_path} FORCE") # force duplicate
            self.print_subsection(f"Copying image file {self.image_path} to {self.image_path}_temp")
            print(f"\t{copy_image_cmd}")
            
    def flags_str(self) -> str:
        return " ".join([f"--{k}={v}" for k, v in self.class_flags])

    def remaining_flags(self, recover_file: str, persist_file: str, trunc: bool, ssd_path: str, scale: int, dram_gib: int):
        flags = [
            ("recover_file", recover_file),
            ("persist_file", persist_file),
            ("trunc", "true" if trunc else "false"),
            ("ssd_path", str(ssd_path)),
            ("tpch_scale_factor", str(scale)),
            ("dram_gib", str(dram_gib))
        ]
        return flags
        
    def generate_recover_file(self) -> None:
        if "lsm" not in self.exec_fname:
            self.makefile_subsection("Generate recovery file")
            loading_files = get_loading_files(self.exec_fname)
            loading_files_str = " ".join(loading_files)
            print(f"{self.recover_file}: {LOADING_META_FILE} {loading_files_str} {self.image_path} {self.runtime_dir}")
            self.print_subsection(f"Persisting data to {self.recover_file}")
            prefix = "lldb -o run -- " if "debug" in str(self.build_dir) else ""
            rem_flags = self.remaining_flags(
                    recover_file=self.recover_file.parent / "leanstore.json", # do not recover
                    persist_file=self.recover_file, # do persist
                    trunc=True,
                    ssd_path=self.image_path,
                    scale=self.scale,
                    dram_gib=8
                )
            rem_flags_str = " ".join([f"--{k}={v}" for k, v in rem_flags])
            print(
                f"\t{prefix}{self.exec_path}", 
                self.flags_str(),
                rem_flags_str,
                f"2>{self.runtime_dir}/stderr.txt",
                sep=" "
            )
        else:
            print(f"{self.recover_file}: {self.runtime_dir}")
            print(f"\ttouch {self.recover_file}")
        print()
        
    def get_remaining_flags(self, lsm: bool):
        image_dep = f"{self.image_path}_temp" if not lsm else self.image_path
        if lsm:
            rem_flags = self.remaining_flags(
                recover_file=self.recover_file, # place holder
                persist_file=self.recover_file, # do not persist
                trunc=True,
                ssd_path=image_dep,
                scale=self.scale,
                dram_gib="$(dram)"
            ) # different from actual experiment flags in get_remaining_flags
        else:
            rem_flags = self.remaining_flags(
                recover_file=self.recover_file, # do recover
                persist_file=self.recover_file.parent / "leanstore.json", # do not persist
                trunc=False,
                ssd_path=image_dep, # duplicate image
                scale=self.scale,
                dram_gib="$(dram)"
            )
        return rem_flags, image_dep

        
    def run_experiment(self) -> None:
        self.makefile_subsection("Run experiment")
        separate_runs = [f"{self.exec_fname}_{str(i)}" for i in STRUCTURE_OPTIONS[self.exec_fname]]
        separate_runs_str = " ".join(separate_runs)
        print(f"{self.exec_fname}: {separate_runs_str}")
        
        rem_flags, image_dep = self.get_remaining_flags(lsm="lsm" in self.exec_fname)
        rem_flags_str = " ".join([f"--{k}={v}" for k, v in rem_flags])

        for structure in STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_{structure}: check_perf_event_paranoid {self.runtime_dir} {self.exec_path} {self.recover_file} {image_dep}")
            print(f"\ttouch {self.runtime_dir}/structure{structure}.log")
            print(
                f'\tscript -q -c "{self.exec_path}',
                self.flags_str(),
                rem_flags_str,
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
        print(f"{self.exec_fname}_lldb: {separate_runs_str}")
        
        rem_flags, img_temp = self.get_remaining_flags(lsm="lsm" in self.exec_fname)
        
        rem_flags_str = " ".join([f"--{k}={v}" for k, v in rem_flags])
        
        # replace dram with 1 in vscode flags
        vscode_flags = self.class_flags.copy() + rem_flags.copy()
        for i, (k, _) in enumerate(vscode_flags):
            if k == "dram_gib":
                vscode_flags[i] = (k, "1")
        
        # separate runs
        for structure in STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_lldb_{structure}: {self.exec_path} {self.recover_file} check_perf_event_paranoid {img_temp}")
            print(
                f"\tlldb -o run --",
                f"{self.exec_path}",
                self.flags_str(),
                rem_flags_str,
                f"--storage_structure={structure}",
                sep=" "
            )
            print()
        
            vscode_configs = {
                "name": f"{self.exec_fname}_{structure}",
                "type": "lldb",
                "request": "launch",
                "program": f"${{workspaceFolder}}/{self.exec_path}",
                "args": [f"--{k}={v}" for k, v in vscode_flags],
                "cwd": "${workspaceFolder}",
                "stopOnEntry": False
            }
            vscode_launch_obj["configurations"].append(vscode_configs)

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
    "geo_leanstore": [0, 1, 2, 3],
    "geo_lsm": [0]
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