from __future__ import annotations

import glob
import os
from typing import List


def get_parquet_files(basepath: str, year: int, path_patterns: List[str]) -> List[str]:
    """
    Expand patterns and return a sorted list of part-*.parquet files across all matches.

    Patterns may include {basepath} and {year}.
    """
    all_parquet_files: list[str] = []

    for pattern in path_patterns:
        pattern = pattern.replace("{basepath}", basepath).replace("{year}", str(year))
        dir_pattern = os.path.dirname(pattern)

        matched_dirs = [d for d in glob.glob(dir_pattern) if os.path.isdir(d)]
        for directory in matched_dirs:
            all_parquet_files.extend(sorted(glob.glob(os.path.join(directory, "part-*.parquet"))))

    # de-dupe while preserving order
    seen = set()
    out = []
    for f in all_parquet_files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out
