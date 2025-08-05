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
exec_names = ["geo_btree", "geo_lsm"]
data_disk = Path("/mnt/ssd/")
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
        image_path = data_disk / exec_fname / f"{SCALE_ENV}"
    else:
        image_path = data_disk / exec_fname / f"{SCALE_ENV}.image"
    recover_file = data_disk / exec_fname / build_dir / f"{SCALE_ENV}.json" # do recover
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
        copy_image_cmd = None
    else:
        create_image_cmd = f"mkdir -p {image_path.parent} && touch {image_path}"
        copy_image_cmd = f"cp -f {image_path} {image_path}_temp"
    return create_image_cmd, copy_image_cmd
    
def get_loading_files(exec_fname: str) -> List[str]:
    src_dir = DIFF_DIRS[exec_fname] if exec_fname in DIFF_DIRS else exec_fname
    file_base = f"./frontend/{src_dir}/load"
    loading_files = []
    for ext in ['tpp', 'hpp', 'cpp']:
        file = f"{file_base}.{ext}"
        if Path(file).exists():
            loading_files.append(file)
    return loading_files

executables = []
runtime_dirs = []
exp_w_structure = []

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
        runtime_dirs.append(self.runtime_dir.parent)
        # class flags
        self.class_flags = shared_flags.copy()
        self.class_flags["csv_path"] = str(self.runtime_dir)
        self.class_flags["csv_truncate"] = "false"
        self.class_flags["log_progress"] = "true" if "debug" in str(self.build_dir) else "false"
        self.class_flags["tentative_skip_bytes"] = "$(tentative_skip_bytes)"
        self.class_flags["bgw_pct"] = "$(bgw_pct)"
    
    def generate_all_targets(self) -> None:
        """Generates all Makefile targets for this experiment."""
        big_sep = "=" * 20
        print(f"# {big_sep} Generating targets for {self.exec_fname} in {self.build_dir} {big_sep}")
        self.generate_runtime_dir()
        self.generate_executable()
        self.generate_image()
        self.generate_recover_file()
        if "debug" not in str(self.build_dir):
            self.run_experiment()
        else:
            self.debug_experiment()
        self.reload()

    def makefile_subsection(self, title: str) -> None:
        print(f"#{self.sep} {title} {self.sep}")
        
    def console_print_subsection(self, title: str) -> None:
        print(f'\t@echo "{self.sep} {title} {self.sep}"')
    
    def generate_runtime_dir(self) -> None:
        self.makefile_subsection("Generate runtime directory")
        # rule to create runtime directory
        print(f"{self.runtime_dir}: ")
        print(f"\t@mkdir -p {self.runtime_dir}")
        print()
    
    def generate_executable(self) -> None:
        self.makefile_subsection("Generate executable")
        # rule to compile executable
        print(f"{self.exec_path}: check_perf_event_paranoid") # 
        self.console_print_subsection(f"Building {self.exec_path}")
        cmake_cmd = get_build_vars(self.build_dir)
        print(
            f'\tcd {self.build_dir} && {cmake_cmd} .',
            f'cd frontend && make {self.exec_fname} -j{NUMJONS_ENV}', sep="; "
        )
        print()
        
    def generate_image(self) -> None:
        if Path(build_dirs[0]).resolve() == self.build_dir.resolve():
            return # only generate image once (all builds use the same image)
        self.makefile_subsection("Generate image file/dir")
        create_image_cmd, copy_image_cmd = get_image_command("lsm" in self.exec_fname, self.image_path)
        
        # rule to create image file/dir
        print(f"{self.image_path}:")
        self.console_print_subsection(f"Creating image file/dir {self.image_path}")
        print(f"\t{create_image_cmd}")
        print()

        # rule to copy image file to a temporary "test field"
        if "lsm" not in self.exec_fname:
            print(f"{self.image_path}_temp: {self.recover_file} {self.image_path} FORCE") # force duplicate; check recover target before image_path target
            self.console_print_subsection(f"Copying image file {self.image_path} to {self.image_path}_temp")
            print(f"\t{copy_image_cmd}")
            print()

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
        print(f"{self.recover_file}: {LOADING_META_FILE} {loading_files_str} | {self.image_path} # order-only dependency")
        self.console_print_subsection(f"Persisting data to {self.recover_file}")
        print(f"\tmkdir -p {self.recover_file}")
        prefix = "lldb -b -o run -o bt -- " if "debug" in str(self.build_dir) else 'script -q -c "'
        suffix = '' if "debug" in str(self.build_dir) else f'" {self.runtime_dir}/load.log'
        rem_flags = self.remaining_flags(
                recover_file="./leanstore.json", # do not recover
                persist_file=self.recover_file, # do persist
                trunc=True,
                ssd_path=self.image_path,
                scale=SCALE_ENV,
                dram_gib=8
            )
        print("\t${MAKE}", self.image_path)
        print("\t${MAKE}", self.runtime_dir)
        print(
            f"\t{prefix}{self.exec_path}", 
            kv_to_str(self.class_flags),
            kv_to_str(rem_flags),
            f"2>{self.runtime_dir}/stderr.txt",
            suffix,
            sep=" "
        )
        # copy recovery file to all other possible locations
        for b in build_dirs:
            b = Path(b)
            if b.resolve() == self.build_dir.resolve():
                continue
            print(f"\tcp -f {self.recover_file} {data_disk / b / self.exec_fname / f'{SCALE_ENV}.json'}")
        print("\techo \"-------------------Image size-------------------\";", f"du -sh {self.image_path}")
        print("\techo \"-------------------Data disk size-------------------\";", f"du -sh {data_disk}")
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
        for structure in [0] + STRUCTURE_OPTIONS[self.exec_fname]:
            exp_w_structure.append(f"{self.exec_fname}_{structure}")
            print(f"{self.exec_fname}_{structure}: check_perf_event_paranoid {self.exec_path} {self.recover_file} {image_dep}")
            print(f"\tmkdir -p {self.runtime_dir}")
            print(f"\ttouch {self.runtime_dir}/structure{structure}.log")
            print(
                f'\tscript -q -c "{self.exec_path}',
                kv_to_str(self.class_flags),
                kv_to_str(rem_flags),
                f"--storage_structure={structure}",
                f'2>{self.runtime_dir}/structure{structure}_stderr.txt\"',
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
            vscode_flags[k] = str(v).replace("$(dram)", "0.1").replace("$(scale)", "15").replace("$(tentative_skip_bytes)", "0").replace("$(bgw_pct)", "10")
        
        # rule to run the experiment in LLDB
        print(f"{self.exec_fname}_lldb: {separate_runs_str}")
        # rules for separate runs
        for structure in [0] + STRUCTURE_OPTIONS[self.exec_fname]:
            print(f"{self.exec_fname}_lldb_{structure}: {self.exec_path} {self.recover_file} check_perf_event_paranoid {img_dep}")
            print(f"\trm stderr.txt && touch stderr.txt") # reset stderr.txt
            print(f"\tmkdir -p {self.runtime_dir}")
            print(
                f"\tlldb -b -o run -o bt --",
                f"{self.exec_path}",
                kv_to_str(self.class_flags),
                kv_to_str(rem_flags),
                f"--storage_structure={structure}",
                sep=" "
            )
            print()
            vscode_flags_structure = vscode_flags.copy()
            vscode_flags_structure["storage_structure"] = str(structure)
            vscode_configs = {
                "name": f"{self.exec_fname}_{structure}",
                "type": "lldb",
                "request": "launch",
                "program": f"${{workspaceFolder}}/{self.exec_path}",
                "args": [f"--{k}={v}" for k, v in vscode_flags_structure.items()],
                "cwd": "${workspaceFolder}",
                "stopOnEntry": False
            }
            vscode_launch_obj["configurations"].append(vscode_configs)
            
    def reload(self):
        midfix = "_lldb" if "debug" in str(self.build_dir) else ""
        print(f"{self.exec_fname}{midfix}_reload:")
        # print(f"\trm -f {self.recover_file}")
        for b in build_dirs:
            print(f"\trm -f {data_disk / b / self.exec_fname / f'{SCALE_ENV}.json'}")
        print(f"\t$(MAKE) {self.recover_file}")
        print()

LOADING_META_FILE = "./frontend/tpc-h/workload.hpp"

DIFF_DIRS = {
 "geo_lsm": "geo",
 "geo_btree": "geo"
}
            
STRUCTURE_OPTIONS = {
    "geo_btree": [1, 2, 3],
    "geo_lsm": [1, 2, 3]
}

def main() -> None:
    """Emits the entire Makefile snippet to stdout."""
    print("# --- auto-generated by generate_targets.py; DO NOT EDIT ---\n")
    for build_dir in build_dirs:
        for exec_name in exec_names:
            exp = Experiment(build_dir, exec_name)
            exp.generate_all_targets()
    
    print(f"executables: {' '.join([str(e) for e in executables])}\n")
    print("all: " + " ".join(exec_names))
    print("all_lldb: " + " ".join([f"{e}_lldb" for e in exec_names]))
    print(f"clean_runtime_dirs:")
    for dir in runtime_dirs:
        print(f"\trm -rf {dir}")

    # phony declaration
    phony = ["FORCE", "check_perf_event_paranoid", "executables", "clean_runtime_dirs", "all", "all_lldb"] + exec_names + [f"{e}_lldb" for e in exec_names] + [f"{e}_reload" for e in exec_names] + [f"{e}_lldb_reload" for e in exec_names]
    print(f".PHONY: {' '.join(phony)}")
    
    vscode_launch = open(".vscode/launch.json", "w")
    json.dump(vscode_launch_obj, vscode_launch, indent=2)
    vscode_launch.close()

if __name__ == "__main__":
    main()