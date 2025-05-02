#!/usr/bin/env python3
"""
generate_targets.py

Auto-generates a Makefile fragment with build, csv-dir, image, recovery and LLDB rules.
"""

from dataclasses import dataclass
from pathlib import Path
import re, json

vscode_launch_obj = {
    "version": "0.2.0",
    "configurations": []
}

@dataclass(frozen=True)
class Config:
    build_dirs = ["build", "build-debug"]
    exec_names = ["basic_join", "basic_group", "basic_group_variant", "basic_join_group"]
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


def image_file(exe: str) -> Path:
    """Returns the path to the SSD image file for a given executable."""
    return cfg.image_dir / exe / f"{cfg.scale}.image"


def generate_build_rules() -> None:
    print_section("build executables")
    print("FORCE: ;")  # Dummy target to force the build
    executables = []
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            exe_path = executable_path(bd, exe)
            executables.append(exe_path)
            print(f"{exe_path}: FORCE")
            print(f'\t@echo "Building {exe_path}"')
            print(
                f'\tcd {bd}/frontend && {cmake_cmd(bd)} {cfg.cmake_options} '
                f'&& make {exe} -j{cfg.numjobs}\n'
            )
    print(f"executables: {' '.join([str(e) for e in executables])}\n")

def generate_runtime_dirs() -> None:
    print_section("runtime directories")
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            rd = runtime_dir(bd, exe)
            print(f"{rd}:")
            print(f'\t@echo "Creating CSV runtime dir {rd}"')
            print(f"\tmkdir -p {rd}\n")

def clean_runtime_dirs() -> None:
    print(f"clean_runtime_dirs:")
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            rd = runtime_dir(bd, exe)
            print(f"\trm -rf {rd}")
    print()

def generate_image_files() -> None:
    print_section("image files")
    for exe in cfg.exec_names:
        img = image_file(exe)
        print(f"{img}:")
        print(f'\t@echo "Touching image file {img}"')
        print(f"\tmkdir -p {img.parent} && touch {img}\n")

LOADING_META_FILE = "./frontend/tpc-h/tpch_workload.hpp"

def loading_file(exe) -> str:
    # strip variant if any
    # return f"./frontend/tpc-h/{exe}/workload.hpp"
    pattern = r"^(.+)_variant$"
    match = re.match(pattern, exe)
    if match:
        exe = match.group(1)
    return f"./frontend/tpc-h/{exe}/workload.hpp"

def generate_recover_rules() -> None:
    print_section("recover files")
    for bd in cfg.build_dirs:
        for exe in cfg.exec_names:
            exe_path = executable_path(bd, exe)
            rd = runtime_dir(bd, exe)
            recover = Path(bd) / exe / f"{cfg.scale}.json"
            img = image_file(exe)
            print(f"{recover}: {LOADING_META_FILE} {loading_file(exe)} {img} {rd} check_perf_event_paranoid") # Reload the database if the executable is new or the image has been changed
            print(f'\t@echo "Persisting data to {recover}"')
            print(
                f"\t{exe_path} {cfg.leanstore_flags} "
                f"--csv_path={rd} --persist_file=$@ "
                f"--trunc=true --ssd_path={img} --dram_gib=4 2>{rd}/stderr.txt\n"
            )
            

def generate_run_rules() -> None:
    print_section("run experiments")
    bd = cfg.build_dirs[0]
    for exe in cfg.exec_names:
        exe_path = executable_path(bd, exe)
        rd = runtime_dir(bd, exe)
        recover = Path(bd) / exe / f"{cfg.scale}.json"
        img = image_file(exe)
        print(f"{exe}: {exe_path} {recover} check_perf_event_paranoid")
        print(f'\t@echo "Running {exe}"')
        print(
            f'\tscript -q -c "{exe_path} {cfg.leanstore_flags} '
            f'--csv_path={rd} --recover_file={recover} '
            f'--ssd_path={img} --dram_gib=$(dram) 2>{rd}/stderr.txt" {rd}/log\n'
        )
    print("run_all: " + " ".join(cfg.exec_names))
    print()


def generate_lldb_rules() -> None:
    print_section("run with LLDB")
    bd = cfg.build_dirs[1]
    for exe in cfg.exec_names:
        exe_path = executable_path(bd, exe)
        rd = runtime_dir(bd, exe)
        recover = Path(bd) / exe / f"{cfg.scale}.json"
        img = image_file(exe)
        print(f"{exe}_lldb: {exe_path} {recover} check_perf_event_paranoid")
        print(f'\t@echo "Running {exe}_lldb under LLDB"')
        print(
            f"\tlldb --source .lldbinit -- "
            f"{exe_path} {cfg.leanstore_flags} "
            f"--csv_path={rd} --recover_file={recover} "
            f"--ssd_path={img} --dram_gib=$(dram)\n"
        )
        args = list(cfg.leanstore_flags.split()) + [
            f"--csv_path={rd}",
            f"--recover_file={recover}",
            f"--ssd_path={img}",
            f"--dram_gib=1",
        ]
        for i, arg in enumerate(args):
            arg = arg.replace("$(dram)", "1")
            arg = arg.replace("$(scale)", "10")
            args[i] = arg
        exp_configs = {
            "name": f"{exe}",
            "type": "lldb",
            "request": "launch",
            "program": f"${{workspaceFolder}}/{exe_path}",
            "args": args,
            "cwd": "${workspaceFolder}",
            "stopOnEntry": False,
            "initCommands": [
                "command source .lldbinit"
            ]
        }
        vscode_launch_obj["configurations"].append(exp_configs)
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
    phony = ["check_perf_event_paranoid", "FORCE", "executables", "clean_runtime_dirs", "run_all", "lldb_all"] + cfg.exec_names + [f"{e}_lldb" for e in cfg.exec_names]
    print(f".PHONY: {' '.join(phony)}")
    
    vscode_launch = open(".vscode/launch.json", "w")
    json.dump(vscode_launch_obj, vscode_launch, indent=2)
    vscode_launch.close()

if __name__ == "__main__":
    main()