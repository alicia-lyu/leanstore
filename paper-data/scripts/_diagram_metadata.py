"""Shared helper for reading paper-data/diagrams.yaml.

All three plotters (plot_paper_sweep.py, plot_refresh_lsm_vs_btree.py,
plot_refresh_sales.py) use this module so the YAML format and the
paper-data root path live in exactly one place.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml


# Absolute path to the paper-data/ directory regardless of cwd.
_SCRIPTS_DIR = Path(__file__).parent
PAPER_DATA_ROOT = _SCRIPTS_DIR.parent
_DEFAULT_YAML = PAPER_DATA_ROOT / "diagrams.yaml"
DIAGRAMS_OUTPUT_DIR = PAPER_DATA_ROOT / "diagrams"


def load_metadata(path: Path = _DEFAULT_YAML) -> Dict:
    """Return the full diagrams.yaml content as a dict.

    Raises ``FileNotFoundError`` when the YAML is absent and
    ``yaml.YAMLError`` on malformed input — both should propagate so
    callers fail loudly rather than silently returning empty data.
    """
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("diagrams", {})


def sources_for(name: str,
                path: Path = _DEFAULT_YAML) -> List[Path]:
    """Return the list of resolved tag-dir paths for diagram ``name``.

    Each element is ``PAPER_DATA_ROOT / <tag>``. Raises ``KeyError`` when
    ``name`` is absent from the YAML; the caller decides whether to skip
    or abort.
    """
    meta = load_metadata(path)
    if name not in meta:
        raise KeyError(f"diagram '{name}' not found in {path}")
    tags: List[str] = meta[name]["sources"]
    # When the artifact runs the plotter against neutral result dirs, the
    # parent exports PAPER_RESULTS_ROOT (+ optional PAPER_TAG_MAP) so even the
    # in-process refresh shim resolves to /results/<neutral> instead of the
    # in-repo authoring tag dir (which the reproduction image doesn't ship).
    results_root = os.environ.get("PAPER_RESULTS_ROOT")
    tag_map: Dict[str, str] = {}
    raw_map = os.environ.get("PAPER_TAG_MAP", "")
    if raw_map:
        try:
            tag_map = yaml.safe_load(raw_map) or {}
        except yaml.YAMLError:
            tag_map = {}
    if results_root:
        return [Path(results_root) / tag_map.get(tag, tag) for tag in tags]
    return [PAPER_DATA_ROOT / tag for tag in tags]


def output_path(name: str, suffix: str = "") -> Path:
    """Return the canonical output path for diagram ``name``.

    ``suffix`` is appended before the extension, e.g. ``suffix='_lsm'``
    gives ``diagrams/<name>_lsm.pdf``. The caller appends the extension
    (``with_suffix('.pdf')`` etc.) — this function returns the stem path.
    """
    return DIAGRAMS_OUTPUT_DIR / f"{name}{suffix}"
