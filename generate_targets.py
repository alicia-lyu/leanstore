#!/usr/bin/env python3
"""
generate_targets.py

Auto-generates a Makefile fragment with build, csv-dir, image, recovery and LLDB rules.
"""

from dataclasses import dataclass
from pathlib import Path
import re, json, time

vscode_launch_obj = {
    "version": "0.2.0",
    "configurations": []
}

@dataclass(frozen=True)
class Config:
    build_dirs = ["build", "build-debug"]
    exec_names = ["basic_join", "basic_group", "basic_group_variant", "basic_join_group", "geo_join"]
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
    image_dir: Path = Path("/mnt/hdd/leanstore_images")


cfg = Config()

#### ================== helper functions================== ####

def print_section(title: str) -> None:
    """Prints a styled section header."""
    sep = "-" * 20
    print(f"# {sep} {title} {sep}")


def cmake_cmd(build_dir: str) -> str:
    """Selects the correct CMake invocation based on directory name."""
    return cfg.cmake_debug if "debug" in build_dir else cfg.cmake_relwithdebinfo


def executable_path(build_dir: str, exe: str) -> Path:
    """Returns the path to the compiled executable."""
    return Path(build_dir) / "frontend" / exe


def runtime_dir(build_dir: str, exe: str) -> Path:
    """
    Returns a dedicated runtime directory for CSV/log output,
    separate from 'frontend' so it's not confused with the binary.
    """
    return Path(build_dir) / exe / f"{cfg.scale}-in-{cfg.dram}"


def image_file(build_dir:str, exe: str) -> Path:
    """Returns the path to the SSD image file for a given executable."""
    return cfg.image_dir / build_dir / exe / f"{cfg.scale}.image"

#### ================== makefile generation functions ================== ####

sep = "=" * 20

def generate_build_rules() -> None:
    """Generates the build rules for the executables."""
    print_section("build executables")
    executables = []
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            exe_path = executable_path(bd, exe)
            executables.append(exe_path)
            print(f"{exe_path}: check_perf_event_paranoid") # check_perf_event_paranoid is PHONY, force rebuild
            print(f'\t@echo "{sep} Building {exe_path} {sep}"')
            print(
                f'\tcd {bd}/frontend && {cmake_cmd(bd)} {cfg.cmake_options} '
                f'&& make {exe} -j{cfg.numjobs}\n'
            )
    print(f"executables: {' '.join([str(e) for e in executables])}\n")

def generate_runtime_dirs() -> None:
    """Generates the runtime directories for CSV/log output."""
    print_section("runtime directories")
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            rd = runtime_dir(bd, exe)
            print(f"{rd}:")
            print(f'\t@echo "{sep} Creating CSV runtime dir {rd} {sep}"')
            print(f"\tmkdir -p {rd}\n")

def clean_runtime_dirs() -> None:
    print(f"clean_runtime_dirs:")
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            rd = runtime_dir(bd, exe)
            print(f"\trm -rf {rd}")
    print()

def generate_image_files() -> None:
    """Generates the image files for database recovery."""
    print_section("image files")
    print(f"FORCE:;")
    for dir in cfg.build_dirs:
        for exe in cfg.exec_names:
            img = image_file(dir, exe)
            print(f"{img}:")
            print(f'\t@echo "{sep} Touching a new image file {img} {sep}"')
            print(f"\tmkdir -p {img.parent} && touch {img}")
            print(f"{img}_temp: {img} FORCE") # force duplicate
            print(f'\t@echo "{sep} Duplicating temporary image file {img} for transactions {sep}"')
            print(f"\tmkdir -p {img.parent} && cp -f {img} {img}_temp")
            print()

LOADING_META_FILE = "./frontend/tpc-h/tpch_workload.hpp"

def loading_files(exe) -> str:
    """Returns the path to the loading files for a given executable."""
    # strip variant if any
    # return f"./frontend/tpc-h/{exe}/workload.hpp"
    pattern = r"^(.+)_variant$"
    match = re.match(pattern, exe)
    if match:
        exe = match.group(1)
    file_base = f"./frontend/tpc-h/{exe}/load"
    files = []
    for ext in ['tpp', 'hpp']:
        file = f"{file_base}.{ext}"
        if Path(file).exists():
            files.append(file)
    return " ".join(files)

def generate_recover_rules() -> None:
    """Generates the recovery rules for a database version/storage."""
    print_section("recover files")
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            exe_path = executable_path(bd, exe)
            rd = runtime_dir(bd, exe)
            recover = Path(bd) / exe / f"{cfg.scale}.json"
            img = image_file(bd, exe)

            print(f"{recover}: {LOADING_META_FILE} {loading_files(exe)} {img}")
            # for f in [recover, LOADING_META_FILE, loading_file(exe), img]:
                # print(f'\techo {f}; stat -c %y "{f}"')
            print(f'\techo "{sep} Persisting data to {recover} {sep}"')
            print(f"\tmkdir -p {rd}")
            print(
                f"\t{exe_path} {cfg.leanstore_flags} "
                f"--csv_path={rd} --persist_file=$@ "
                f"--trunc=true --ssd_path={img} --dram_gib=8 2>{rd}/stderr.txt\n"
            )

STRUCTURE_OPTIONS = {
    "basic_join": [0, 1, 2],
    "basic_group": [0, 2],
    "basic_group_variant": [0, 2],
    "basic_join_group": [0, 2],
    "geo_join": [0, 1, 2, 3]
}            

def generate_run_rules() -> None:
    """Generates the run rules for the experiments."""
    print_section("run experiments")
    bd = cfg.build_dirs[0]
    for exe in cfg.exec_names:
        exe_path = executable_path(bd, exe)
        rd = runtime_dir(bd, exe)
        recover = Path(bd) / exe / f"{cfg.scale}.json"
        img = f"{image_file(bd, exe)}"
        separate_runs = " ".join([f"{exe}_{str(i)}" for i in STRUCTURE_OPTIONS[exe]])
        print(f"{exe}: check_perf_event_paranoid {separate_runs}")
        
        img_temp = f"{img}_temp"
        for structure in STRUCTURE_OPTIONS[exe]:
            print(f"{exe}_{structure}: {rd} {exe_path} {recover} {img_temp}")
            print(f"\ttouch {rd}/structure{structure}.log")
            print(
                f'\tscript -q -c "{exe_path} {cfg.leanstore_flags} '
                f"--storage_structure={structure} "
                f'--csv_path={rd} --recover_file={recover} '
                f'--ssd_path={img_temp} --dram_gib=$(dram) 2>{rd}/stderr.txt" {rd}/structure{structure}.log'
            )
        print()
    print("run_all: " + " ".join(cfg.exec_names))
    print()

def generate_lldb_rules() -> None:
    """Generates the LLDB rules and vscode launch configurations for the experiments."""
    print_section("run with LLDB")
    bd = cfg.build_dirs[1]
    for exe in cfg.exec_names:
        exe_path = executable_path(bd, exe)
        rd = runtime_dir(bd, exe)
        recover = Path(bd) / exe / f"{cfg.scale}.json"
        img = image_file(bd, exe)
        separate_runs = " ".join([f"{exe}_lldb_{str(i)}" for i in STRUCTURE_OPTIONS[exe]])
        
        print(f"{exe}_lldb: {separate_runs}")
        
        img_temp = f"{img}_temp"
        args = list(cfg.leanstore_flags.split()) + [
            f"--csv_path={rd}",
            f"--recover_file={recover}",
            f"--ssd_path={img_temp}",
            f"--dram_gib=1",
        ]
        for i, arg in enumerate(args):
            arg = arg.replace("$(dram)", "1")
            arg = arg.replace("$(scale)", "10")
            args[i] = arg
        
        # separate runs
        for structure in STRUCTURE_OPTIONS[exe]:
            print(f"{exe}_lldb_{structure}: {exe_path} {recover} check_perf_event_paranoid {img_temp}")
            print(
                f"\tlldb -o run -- "
                f"{exe_path} {cfg.leanstore_flags} "
                f"--storage_structure={structure} "
                f"--csv_path={rd} --recover_file={recover} "
                f"--ssd_path={img_temp} --dram_gib=$(dram)"
            )
            args_run = args + [f"--storage_structure={structure}"]
        
            exp_configs = {
                "name": f"{exe}_{structure}",
                "type": "lldb",
                "request": "launch",
                "program": f"${{workspaceFolder}}/{exe_path}",
                "args": args_run,
                "cwd": "${workspaceFolder}",
                "stopOnEntry": False
            }
            vscode_launch_obj["configurations"].append(exp_configs)
        print()
    print("lldb_all: " + " ".join([f"{e}_lldb" for e in cfg.exec_names]))
    print()

def main() -> None:
    """Emits the entire Makefile snippet to stdout."""
    print("# --- auto-generated by generate_targets.py; DO NOT EDIT ---\n")
    generate_build_rules()
    generate_runtime_dirs()
    clean_runtime_dirs()
    generate_image_files()
    generate_recover_rules()
    generate_run_rules()
    generate_lldb_rules()

    # phony declaration
    phony = ["FORCE", "check_perf_event_paranoid", "executables", "clean_runtime_dirs", "run_all", "lldb_all"] + cfg.exec_names + [f"{e}_lldb" for e in cfg.exec_names]
    print(f".PHONY: {' '.join(phony)}")
    
    vscode_launch = open(".vscode/launch.json", "w")
    json.dump(vscode_launch_obj, vscode_launch, indent=2)
    vscode_launch.close()

if __name__ == "__main__":
    main()