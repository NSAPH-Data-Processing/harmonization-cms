from __future__ import annotations

import glob
from typing import Iterable, List


def _normalize_basepaths(basepath: str | Iterable[str]) -> list[str]:
    if isinstance(basepath, str):
        return [basepath]
    return [str(path) for path in basepath if path]


def resolve_input_patterns(basepath: str | Iterable[str], year: int, path_patterns: List[str]) -> List[str]:
    """
    Expand each input pattern across one or many basepaths.
    """
    resolved_patterns: list[str] = []
    for root in _normalize_basepaths(basepath):
        for pattern in path_patterns:
            resolved_patterns.append(pattern.replace("{basepath}", root).replace("{year}", str(year)))
    return resolved_patterns


def get_parquet_files(basepath: str | Iterable[str], year: int, path_patterns: List[str]) -> List[str]:
    """
    Expand patterns and return a sorted list of part-*.parquet files across all matches.

    Patterns may include {basepath} and {year}. If multiple basepaths are
    provided, each pattern is expanded against each root.
    """
    all_parquet_files: list[str] = []

    for pattern in resolve_input_patterns(basepath=basepath, year=year, path_patterns=path_patterns):
        all_parquet_files.extend(sorted(glob.glob(pattern)))

    # de-dupe while preserving order
    seen = set()
    out = []
    for f in all_parquet_files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out
