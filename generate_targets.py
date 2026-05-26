#!/usr/bin/env python3
"""
generate_targets.py

Auto-generates a Makefile fragment with build, csv-dir, image, recovery and LLDB rules.
"""

from pathlib import Path
import json
from typing import List
import platform

vscode_launch_obj = {
    "version": "0.2.0",
    "configurations": []
}

build_dirs = ["build", "build-debug"]
exec_names = ["geo_btree", "geo_lsm", "q12_btree", "q12_lsm", "q3i_btree", "q3i_lsm", "q3_btree", "q3_lsm", "q5_btree", "q5_lsm", "q5i_btree", "q5i_lsm", "q10_btree", "q10_lsm", "q10i_btree", "q10i_lsm"]
data_disk = Path("$(data_disk)")
IS_MACOS = platform.system() == "Darwin"
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
    
# Family grouping for the shared TPC-H load image. Both binaries in a family
# declare the full family adapter set and run tpch::load_<family>_family() so
# either can mount the same .json image. The image path is keyed on the family
# tag rather than the per-query exec name. Q12 stays self-contained (different
# pipeline — OL — and excluded from the paper-ready experiments).
TPCH_FAMILY = {
    # Vanilla COL family
    "q3_lsm":    "tpch_lsm",
    "q5_lsm":    "tpch_lsm",
    "q3_btree":  "tpch_btree",
    "q5_btree":  "tpch_btree",
    # Invoice-extended COLI family. NOTE: q10i is intentionally NOT a member —
    # its executable loads independently (q10i.load(); see
    # q10i/executable_*.cpp "Not plugged into the Q3I+Q5I family cohort") and
    # load_tpchi_family(tpch, q3i, q5i) does not populate q10i's view. Mapping
    # it here would make `make q10i_*` recover from a q3i/q5i image lacking the
    # q10i pipeline view (degenerate S2). q10i keeps its own image dir + loader.
    "q3i_lsm":   "tpchi_lsm",
    "q5i_lsm":   "tpchi_lsm",
    "q3i_btree": "tpchi_btree",
    "q5i_btree": "tpchi_btree",
}

def image_basename(exec_fname: str) -> str:
    """Image / recover-file parent directory.

    Family-shared binaries write into a family-tagged directory so a single
    persisted image is interchangeable across the family's binaries. Other
    binaries (geo_*, q12_*) keep per-exec directories.
    """
    return TPCH_FAMILY.get(exec_fname, exec_fname)


# Canonical loader per family. Make's "last recipe wins" behavior over
# duplicated recover-file recipes from multiple family members is fragile
# (and historically miscompiles when a later-defined member's load path has
# a bug — see paper-sweep 2026-05-18 smoke-test). Pin one canonical loader
# per image base and skip recover-file emission for the rest.
TPCH_FAMILY_LOADER = {
    "tpch_lsm":    "q3_lsm",
    "tpch_btree":  "q3_btree",
    "tpchi_lsm":   "q3i_lsm",
    "tpchi_btree": "q3i_btree",
}

def is_family_loader(exec_fname: str) -> bool:
    """True if this binary is the canonical loader for its family, or owns
    its image dir solo (geo_*, q12_*)."""
    base = image_basename(exec_fname)
    return TPCH_FAMILY_LOADER.get(base, exec_fname) == exec_fname

def get_exec_vars(build_dir: Path, exec_fname: str) -> tuple[Path, Path, Path, Path]:
    exec_path = build_dir / "frontend" / exec_fname
    image_base = image_basename(exec_fname)
    if "lsm" in exec_fname:
        image_path = data_disk / image_base / f"{SCALE_ENV}"
    else:
        image_path = data_disk / image_base / f"{SCALE_ENV}.image"
    recover_file = data_disk / image_base / build_dir / f"{SCALE_ENV}.json" # do recover
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
        # The bgw_pct flag is a TPC-H/geo-shared write-percentage knob that
        # the geo benchmark no longer accepts (geo is now a decoupled
        # microbenchmark — see frontend/geo/CLAUDE.md). Pass it only for
        # TPC-H per-query binaries; geo uses --geo_bg_thread instead.
        if not self.exec_fname.startswith("geo_"):
            self.class_flags["bgw_pct"] = "$(bgw_pct)"
            # TPC-H contention axis for the paper sweep. C++ default in
            # tpch_flags.hpp is `false`; the Makefile pins the same default.
            self.class_flags["bg_query_thread"] = "$(bg_query_thread)"
            # bg=2: heterogeneous cohort adds a random-table point-lookup step
            # to the bg cohort. Only meaningful when bg_query_thread=true.
            self.class_flags["bg_point_lookups"] = "$(bg_point_lookups)"
        else:
            self.class_flags["geo_bg_thread"] = "$(geo_bg_thread)"
    
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
            # A5: isolated-DB variant — one image per storage structure with
            # only that structure's secondary loaded. Lets us measure each
            # path without cross-structure cache pollution (H8). Only emitted
            # for the build directory (not build-debug) and for q3i, which
            # is the only target with the H8 hypothesis on its worklist.
            if self.exec_fname in ("q3i_lsm", "q3i_btree"):
                self.run_isolated_experiment()
        else:
            self.debug_experiment()
        self.reload()

    def run_isolated_experiment(self) -> None:
        """A5 isolated-DB variant: per-structure image + recover file.

        For each --storage_structure N, emits:
          - $(data_disk)/{exec}_iso/iso_{N}/{scale}: the image dir/file
          - $(data_disk)/{exec}_iso/iso_{N}/build/{scale}.json: persist
            target (loads ONLY structure N's secondary via
            --load_only_structure=N)
          - {exec}_iso_{N}: recover + run target
          - {exec}_iso: aggregate of all structures.

        Runtime / CSV output is shared at
        build/{exec}_iso/{scale}-in-{dram}/, mirroring the non-iso
        layout: per-structure log files cohabit one runtime dir, with
        a single TPut.csv / size.csv carrying one row per N. Lets
        downstream comparisons paste shared-vs-iso CSVs row-by-row.
        """
        self.makefile_subsection("A5 isolated-DB experiment")
        is_lsm = "lsm" in self.exec_fname
        iso_runtime = Path(f"{self.build_dir}/{self.exec_fname}_iso/{SCALE_ENV}-in-$(dram)")
        # Per-iso-target csv_path override so iso TPut/size CSVs land
        # under build/{exec}_iso/, not the inherited non-iso runtime_dir.
        iso_class_flags = self.class_flags.copy()
        iso_class_flags["csv_path"] = str(iso_runtime)
        for n in STRUCTURE_OPTIONS[self.exec_fname]:
            iso_image = data_disk / f"{self.exec_fname}_iso" / f"iso_{n}" / f"{SCALE_ENV}"
            iso_recover = data_disk / f"{self.exec_fname}_iso" / f"iso_{n}" / "build" / f"{SCALE_ENV}.json"
            iso_image_str = str(iso_image) if is_lsm else f"{iso_image}.image"
            create_cmd, _ = get_image_command(is_lsm, Path(iso_image_str))

            # image dir/file
            print(f"{iso_image_str}:")
            print(f"\t{create_cmd}")
            print()
            # persist target
            iso_loading_files_str = " ".join(get_loading_files(self.exec_fname))
            print(f"{iso_recover}: {LOADING_META_FILE} {iso_loading_files_str} | {iso_image_str}")
            self.console_print_subsection(f"Persisting isolated structure {n} → {iso_recover}")
            print(f"\tmkdir -p {iso_recover.parent}")
            persist_flags = self.remaining_flags(
                recover_file="./leanstore.json",
                persist_file=str(iso_recover),
                trunc=True,
                ssd_path=iso_image_str,
                scale=SCALE_ENV,
                dram_gib=8,
            )
            if IS_MACOS:
                prefix = f"script -q {iso_runtime}/load.log "
                suffix = ""
            else:
                prefix = "script -q -c \""
                suffix = f"\" {iso_runtime}/load.log"
            print(f"\t@mkdir -p {iso_runtime}")
            print(
                f"\t{prefix}{self.exec_path}",
                kv_to_str(self.class_flags),
                kv_to_str(persist_flags),
                f"--storage_structure={n}",
                f"--load_only_structure={n}",
                f"2>{iso_runtime}/load_stderr.txt{suffix}",
                sep=" ",
            )
            print()
            # run target
            run_flags = self.remaining_flags(
                recover_file=str(iso_recover),
                persist_file="./leanstore.json",
                trunc=False,
                ssd_path=iso_image_str,
                scale=SCALE_ENV,
                dram_gib="$(dram)",
            )
            print(f"{self.exec_fname}_iso_{n}: check_perf_event_paranoid {self.exec_path} {iso_recover} {iso_image_str}")
            print(f"\t@mkdir -p {iso_runtime}")
            print(f"\ttouch {iso_runtime}/structure{n}.log")
            if IS_MACOS:
                print(
                    f"\tscript -q {iso_runtime}/structure{n}.log",
                    f"{self.exec_path}",
                    kv_to_str(iso_class_flags),
                    kv_to_str(run_flags),
                    f"--storage_structure={n}",
                    "--micro_perf=true",
                    "--cfstats=true",
                    "--coli_walker_variant=$(coli_walker_variant)",
                    "--use_seek_skip=$(use_seek_skip)",
                    f"2>{iso_runtime}/structure{n}_stderr.txt",
                    sep=" ",
                )
            else:
                print(
                    f"\tscript -q -c \"{self.exec_path}",
                    kv_to_str(iso_class_flags),
                    kv_to_str(run_flags),
                    f"--storage_structure={n}",
                    "--micro_perf=true",
                    "--cfstats=true",
                    "--coli_walker_variant=$(coli_walker_variant)",
                    "--use_seek_skip=$(use_seek_skip)",
                    f"2>{iso_runtime}/structure{n}_stderr.txt\"",
                    f"{iso_runtime}/structure{n}.log",
                    sep=" ",
                )
            print()
        # aggregate target
        agg_deps = " ".join([f"{self.exec_fname}_iso_{n}" for n in STRUCTURE_OPTIONS[self.exec_fname]])
        print(f"{self.exec_fname}_iso: {agg_deps}")
        print()

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

        # rule to copy image file to a temporary "test field". image_temp must
        # depend on the BUILD-mode recover-file (production binary writes it),
        # not self.recover_file (which is build-debug here because generate_image
        # only runs from the build-debug Experiment iteration). Depending on
        # build-debug pulled in the lldb-driven debug load recipe, which fails
        # with exit 127 on hosts that don't have lldb installed.
        if "lsm" not in self.exec_fname:
            build_recover_file = data_disk / image_basename(self.exec_fname) / "build" / f"{SCALE_ENV}.json"
            print(f"{self.image_path}_temp: {build_recover_file} {self.image_path} FORCE") # force duplicate; check recover target before image_path target
            self.console_print_subsection(f"Copying image file {self.image_path} to {self.image_path}_temp")
            print(f"\t{copy_image_cmd}")
            print()

    def remaining_flags(self, recover_file: str, persist_file: str, trunc: bool, ssd_path: str, scale: int, dram_gib: int) -> dict[str, str]:
        flags = {
            "recover_file": recover_file,
            "persist_file": persist_file,
            "trunc": "true" if trunc else "false",
            "ssd_path": str(ssd_path),
            "dram_gib": str(dram_gib)
        }
        # Geo binaries take --geo_scale_factor (post-decouple); TPC-H per-query
        # binaries take --tpch_scale_factor.
        if self.exec_fname.startswith("geo_"):
            flags["geo_scale_factor"] = str(scale)
        else:
            flags["tpch_scale_factor"] = str(scale)
        return flags
        
    def generate_recover_file(self) -> None:
        # Only the canonical loader for a family emits the recover-file rule.
        # Skipping duplicates avoids make's "overriding recipe / ignoring old
        # recipe" warnings and the silent miscompile when a later-defined
        # family member has a buggy load path (see paper-sweep 2026-05-18
        # smoke test where q5_btree's load aborted on COLI keys it should
        # never have seen).
        if not is_family_loader(self.exec_fname):
            return
        self.makefile_subsection("Generate recovery file")
        loading_files = get_loading_files(self.exec_fname)
        loading_files_str = " ".join(loading_files)
        # rule to load database and create recovery file
        print(f"{self.recover_file}: {LOADING_META_FILE} {loading_files_str} | {self.image_path} # order-only dependency")
        self.console_print_subsection(f"Persisting data to {self.recover_file}")
        print(f"\tmkdir -p {self.recover_file.parent}")
        if "debug" in str(self.build_dir):
            prefix = "lldb -b -o run -o bt -- "
            suffix = ''
        elif IS_MACOS:
            prefix = f'script -q {self.runtime_dir}/load.log '
            suffix = ''
        else:
            prefix = 'script -q -c "'
            suffix = f'" {self.runtime_dir}/load.log'
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
            dest = data_disk / self.exec_fname / b / f'{SCALE_ENV}.json'
            print(f"\tmkdir -p {dest.parent}")
            print(f"\tcp -f {self.recover_file} {dest}")
        print("\techo \"-------------------Image size-------------------\";", f"du -sh {self.image_path} | awk '{{print $1}}'")
        print("\techo \"-------------------Data disk size-------------------\";", f"du -sh {data_disk} | awk '{{print $1}}'")
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
            # Diagnostic flags: opt-in via Makefile vars `micro_perf=true cfstats=true`.
            # Default false in the Makefile; A1 sweep enables them per-run.
            # `coli_walker_variant` default is fused_emit post-A2c.
            # `use_seek_skip` default -1 = use Backend trait; A3-Linux sweep flips for RocksDB.
            # geo_* executables don't declare the TPC-H diagnostic flags;
            # only emit them for TPC-H targets.
            if self.exec_fname.startswith("geo_"):
                diag_flags = ""
            else:
                diag_flags = "--micro_perf=$(micro_perf) --cfstats=$(cfstats) --coli_walker_variant=$(coli_walker_variant) --use_seek_skip=$(use_seek_skip) --param_seed=$(param_seed)"
                # --q10_stats is declared only by the q10 executables.
                # --q10_view_variant / --skip_order_physical are declared in
                # tpch_flags.hpp (all tpch binaries) but only acted on by q10;
                # emit them on q10 targets so the S2/S3 A/B sweeps are
                # Makefile-driven.
                if self.exec_fname.startswith("q10_"):
                    diag_flags += " --q10_stats=$(q10_stats)"
                    diag_flags += " --q10_view_variant=$(q10_view_variant)"
                    diag_flags += " --skip_order_physical=$(skip_order_physical)"
                # --q10i_view_variant is declared in tpch_flags.hpp; only acted
                # on by q10i. q10i_ does NOT match the q10_ prefix above.
                if self.exec_fname.startswith("q10i_"):
                    diag_flags += " --q10i_view_variant=$(q10i_view_variant)"
            if IS_MACOS:
                print(
                    f'\tscript -q {self.runtime_dir}/structure{structure}.log',
                    f'{self.exec_path}',
                    kv_to_str(self.class_flags),
                    kv_to_str(rem_flags),
                    f"--storage_structure={structure}",
                    diag_flags,
                    f'2>{self.runtime_dir}/structure{structure}_stderr.txt',
                    sep=" "
                )
            else:
                print(
                    f'\tscript -q -c "{self.exec_path}',
                    kv_to_str(self.class_flags),
                    kv_to_str(rem_flags),
                    f"--storage_structure={structure}",
                    diag_flags,
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
            vscode_flags[k] = str(v).replace("$(dram)", "0.1").replace("$(scale)", "15").replace("$(tentative_skip_bytes)", "0").replace("$(bgw_pct)", "0").replace("$(bg_query_thread)", "false").replace("$(bg_point_lookups)", "false").replace("$(geo_bg_thread)", "false") # for debugging, use no bgw to prevent keyInCurrentBoundaries = false error
        
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

# Files whose mtime change must invalidate every persisted recovery
# image (`$(data_disk)/<exec>/build/$(scale).json`). The CLAUDE.md
# "Reload eagerly" workflow rule documents this list. If you add a
# field to a record type, edit a populate_*() body, or otherwise
# change on-disk byte layout / load logic, touching one of these
# files (or the per-query `<q>/load.{tpp,hpp,cpp}`) is what tells
# Make to re-derive the image. Keep this list in sync with anything
# that affects what bytes get written during load.
LOADING_META_FILES = [
    "./frontend/tpch/tpch_workload.hpp",
    "./frontend/tpch/tpchi_family/tpchi_workload.hpp",
    "./frontend/tpch/tpch_family/views_ol.hpp",
    "./frontend/tpch/tpch_family/views_col.hpp",
    "./frontend/tpch/tpch_family/views_coli.hpp",
    "./frontend/tpch/tpch_family/ol_pipeline.tpp",
    "./frontend/tpch/tpch_family/col_pipeline.tpp",
    "./frontend/tpch/tpchi_family/coli_pipeline.tpp",
]
LOADING_META_FILE = " ".join(LOADING_META_FILES)

DIFF_DIRS = {
 "geo_lsm": "geo",
 "geo_btree": "geo",
 "q12_lsm": "tpch/q12",
 "q12_btree": "tpch/q12",
 "q3i_lsm": "tpch/q3i",
 "q3i_btree": "tpch/q3i",
 "q3_lsm": "tpch/q3",
 "q3_btree": "tpch/q3",
 "q5_lsm": "tpch/q5",
 "q5_btree": "tpch/q5",
 "q5i_lsm": "tpch/q5i",
 "q5i_btree": "tpch/q5i",
 "q10_lsm": "tpch/q10",
 "q10_btree": "tpch/q10",
 "q10i_lsm": "tpch/q10i",
 "q10i_btree": "tpch/q10i",
}

STRUCTURE_OPTIONS = {
    "geo_btree": [1, 2, 3, 4],
    "geo_lsm": [1, 2, 3, 4],
    "q12_btree": [1, 2, 3, 4],
    "q12_lsm": [1, 2, 3, 4],
    "q3i_btree": [1, 2, 3, 4, 5],
    "q3i_lsm": [1, 2, 3, 4, 5],
    "q3_btree": [1, 2, 3, 4, 6],
    "q3_lsm": [1, 2, 3, 4, 6],
    "q5_btree": [1, 2, 3, 4, 6],
    "q5_lsm": [1, 2, 3, 4, 6],
    "q5i_btree": [1, 2, 3, 4],
    "q5i_lsm": [1, 2, 3, 4],
    "q10_btree": [1, 2, 3, 4, 5, 6],
    "q10_lsm": [1, 2, 3, 4, 5, 6],
    "q10i_btree": [1, 2, 3, 4, 5],
    "q10i_lsm": [1, 2, 3, 4, 5],
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
    # A5 iso aggregate targets are phony (per-structure run targets too).
    for e in ("q3i_lsm", "q3i_btree"):
        phony.append(f"{e}_iso")
        for n in STRUCTURE_OPTIONS[e]:
            phony.append(f"{e}_iso_{n}")
    print(f".PHONY: {' '.join(phony)}")
    
    vscode_launch = open(".vscode/launch.json", "w")
    json.dump(vscode_launch_obj, vscode_launch, indent=2)
    vscode_launch.close()

if __name__ == "__main__":
    main()