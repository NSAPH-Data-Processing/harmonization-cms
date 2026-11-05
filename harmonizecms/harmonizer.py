from __future__ import annotations

import glob
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from typing import Iterable, Mapping, Optional

import duckdb
import yaml

from . import stays as _stays
from .io import get_parquet_files
from .stays import (STAY_COLUMNS, admissions_key_sharing, aggregate_stays, drop_prepare_tables,
                    load_first_taf_year, max_rows_to_stays, prepare_claims, prepare_links,
                    stay_column_types)

LOGGER = logging.getLogger(__name__)


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _escape_sql_string(s: str) -> str:
    # Escape single quotes for safe embedding in SQL string literals
    return s.replace("'", "''")


def _jsonable_basepath(basepath: str | Iterable[str]) -> str | list[str]:
    if isinstance(basepath, str):
        return basepath
    return [str(path) for path in basepath if path]


def _normalize_basepaths(basepath: str | Iterable[str]) -> list[str]:
    if isinstance(basepath, str):
        return [basepath]
    return [str(path) for path in basepath if path]


def _resolve_single_basepath(basepath: str | Iterable[str]) -> str:
    if isinstance(basepath, str):
        return basepath
    basepaths = [str(path) for path in basepath if path]
    if len(basepaths) != 1:
        raise ValueError(
            "Template resolution with {basepath} requires exactly one basepath. "
            "When running with multiple basepaths, use {crosswalks_path} for shared lookup roots "
            "or ensure crosswalk paths are resolvable per branch."
        )
    return basepaths[0]


def _resolve_template(
    path_template: str,
    *,
    basepath: str | Iterable[str],
    crosswalks_path: Optional[str],
    year: int,
) -> str:
    """
    Resolve {basepath}, {crosswalks_path}, {year} placeholders.
    """
    resolved = str(path_template)
    if "{basepath}" in resolved:
        resolved = resolved.replace("{basepath}", _resolve_single_basepath(basepath))
    resolved = resolved.replace("{year}", str(year))
    if "{crosswalks_path}" in resolved:
        if not crosswalks_path:
            raise ValueError(
                "crosswalks_path is required because {crosswalks_path} is used in a crosswalk path."
            )
        resolved = resolved.replace("{crosswalks_path}", crosswalks_path)
    return resolved


def _cw_read_expr(path: str, fmt: str) -> str:
    """
    Return DuckDB table function for reading a crosswalk.
    """
    p = _escape_sql_string(path)
    fmt_norm = (fmt or "").lower().strip() or "parquet"
    if fmt_norm == "parquet":
        return f"read_parquet('{p}')"
    if fmt_norm in ("csv", "csv_auto"):
        # read_csv_auto infers schema; good default for simple crosswalks
        return f"read_csv_auto('{p}', all_varchar=true)"
    raise ValueError(f"Unsupported crosswalk format: {fmt!r}. Use parquet or csv.")


def _load_schema_lower(
    conn: duckdb.DuckDBPyConnection,
    parquet_files: list[str],
) -> dict[str, str]:
    if not parquet_files:
        return {}

    files_str = ", ".join([f"'{_escape_sql_string(f)}'" for f in parquet_files])
    df = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet([{files_str}], filename = true) LIMIT 0"
    ).fetchdf()

    schema = {
        row["column_name"].lower(): str(row["column_type"]).upper()
        for _, row in df.iterrows()
    }

    # YAML-accessible alias for DuckDB's virtual filename column.
    # This avoids requiring YAML to use `filename`, which may collide with
    # a physical parquet column named filename.
    schema["source_filename"] = "VARCHAR"

    return schema


def _select_first_existing(schema_lower: dict[str, str], candidates) -> Optional[str]:
    if candidates is None:
        return None
    if isinstance(candidates, str):
        candidates = [candidates]
    for c in candidates:
        if c and c.lower() in schema_lower:
            return c
    return None


def _sql_column_name(selected_source: str) -> str:
    """
    Translate YAML pseudo-columns into SQL column references.
    """
    if selected_source.lower() == "source_filename":
        return "filename"
    return selected_source


def _cast_expr(
    selected_source: str,
    schema_lower: dict[str, str],
    cast_dict: dict[str, str] | None,
    year: int,
) -> str:
    cast_dict_norm = {str(k).upper(): v for k, v in (cast_dict or {}).items()}
    src_type = schema_lower.get(selected_source.lower(), "")
    template = cast_dict_norm.get(src_type) or cast_dict_norm.get("*") or "{column_name}"

    sql_column_name = _sql_column_name(selected_source)

    return template.format(column_name=sql_column_name, year=year)


def _typed_null(sql_type: Optional[str]) -> str:
    """
    NULL literal carrying the declared YAML `type`. A bare NULL is typed as
    "NULL" by DuckDB and is written to parquet as INTEGER, so a column that is
    missing in one year would otherwise disagree with the schema of other years.
    """
    if not sql_type:
        return "NULL"
    return f"CAST(NULL AS {sql_type})"


def _array_element_type(sql_type: Optional[str]) -> Optional[str]:
    if not sql_type:
        return None
    sql_type = str(sql_type).strip()
    if sql_type.endswith("[]"):
        return sql_type[:-2].strip()
    return None


def _construct_array_expr(col_def: dict, schema_lower: dict[str, str], year: int) -> str:
    """
    Array contract:
      - if any source_array exists -> use that, applying cast dict
      - else build ARRAY[...] from ONE chosen component schema:
          * source_component_sets: list of lists; choose first set with any existing col
          * else fallback to source_components
      - missing components become NULL (preserves array length)
      - NULLs are typed from the declared `type` (e.g. VARCHAR[])
    """
    source_array = col_def.get("source_array") or []
    source_components = col_def.get("source_components") or []
    source_component_sets = col_def.get("source_component_sets") or []
    cast_dict = col_def.get("cast") or {}
    element_cast = col_def.get("element_cast")
    sql_type = col_def.get("type")
    element_type = _array_element_type(sql_type)

    selected_array = _select_first_existing(schema_lower, source_array)
    if selected_array:
        return _cast_expr(selected_array, schema_lower, cast_dict, year)

    components_to_use: list[str] = []
    if source_component_sets:
        for candidate_set in source_component_sets:
            if not isinstance(candidate_set, list):
                continue
            if any(c.lower() in schema_lower for c in candidate_set):
                components_to_use = candidate_set
                break
    else:
        components_to_use = source_components

    if not components_to_use:
        return _typed_null(sql_type)

    def elem(component_col: str) -> str:
        if component_col.lower() not in schema_lower:
            return _typed_null(element_type)
        sql_column_name = _sql_column_name(component_col)
        if element_cast:
            return str(element_cast).format(column_name=sql_column_name, year=year)
        return sql_column_name

    array_expr = f"ARRAY[{', '.join(elem(c) for c in components_to_use)}]"

    if col_def.get("drop_nulls", False):
        return f"list_filter({array_expr}, x -> x IS NOT NULL)"

    return array_expr


# Note: always writes to the temp table `raw_base`, so a pre_join table supports
# only one basepath per run (a second branch would overwrite the first).
def apply_pre_joins(
    conn: duckdb.DuckDBPyConnection,
    table_cfg: dict,
    parquet_files: list[str],
    year: int,
    basepath: str,
) -> str:
    files_str = ", ".join(f"'{_escape_sql_string(f)}'" for f in parquet_files)

    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE raw_base AS
        SELECT *
        FROM read_parquet([{files_str}], union_by_name=true)
    """)

    for join_cfg in table_cfg.get("pre_join", []) or []:
        if year < int(join_cfg.get("min_year", year)):
            continue
        if year > int(join_cfg.get("max_year", year)):
            continue

        join_path = (
            join_cfg["path"]
            .replace("{basepath}", basepath)
            .replace("{year}", str(year))
        )

        alias = join_cfg["name"]
        on_map = join_cfg["on"]
        key_cols = list(on_map.values())
        extra_cols = join_cfg.get("columns", [])
        source_cols = key_cols + extra_cols
        source_select = ", ".join(source_cols)

        extra_select = ""
        if extra_cols:
            extra_select = ", " + ", ".join(f"{alias}.{col}" for col in extra_cols)

        on_sql = " AND ".join(
            f"base.{left_col} = {alias}.{right_col}"
            for left_col, right_col in on_map.items()
        )

        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE raw_base AS
            SELECT
                base.*
                {extra_select}
            FROM raw_base AS base
            {join_cfg.get("how", "left").upper()} JOIN (
                SELECT {source_select}
                FROM read_parquet('{join_path}')
            ) AS {alias}
            ON {on_sql}
        """)
        
    return "raw_base"


def _build_crosswalk_join_sql(
    *,
    table_name: str,
    table_cfg: dict,
    parquet_files: list[str],
    year: int,
    basepath: str,
    crosswalks_path: Optional[str],
) -> str:
    """
    Builds SQL:
      WITH base AS (...),
           cw_<name> AS (...),
           joined AS (...)
      SELECT * FROM joined;

    If no crosswalks, returns original CREATE OR REPLACE TABLE ... SELECT ... FROM read_parquet([...]).
    """
    schema_lower = duckdb.connect(database=":memory:")  # only for typing? no, we won't use this connection
    # NOTE: We don't load schema here; construct_query already does with provided conn.
    raise RuntimeError("Do not call _build_crosswalk_join_sql directly.")


def _build_select_exprs(
    table_cfg: dict,
    schema_lower: dict[str, str],
    year: int,
) -> list[str]:
    select_exprs: list[str] = []

    for col in table_cfg.get("columns", []):
        col_name = list(col.keys())[0]
        col_def = col[col_name] or {}
        kind = col_def.get("kind", "scalar")

        if kind == "array":
            expr = _construct_array_expr(col_def, schema_lower, year)
            select_exprs.append(f"{expr} AS {col_name}")
            continue

        source = col_def.get("source")
        selected_source = _select_first_existing(schema_lower, source)
        if not selected_source:
            if col_name == "year":
                select_exprs.append(f"{year} AS {col_name}")
            else:
                select_exprs.append(f"{_typed_null(col_def.get('type'))} AS {col_name}")
            continue

        expr = _cast_expr(selected_source, schema_lower, col_def.get("cast"), year)
        select_exprs.append(f"{expr} AS {col_name}")

    return select_exprs


def _build_branch_sql(
    conn: duckdb.DuckDBPyConnection,
    *,
    branch_name: str,
    table_cfg: dict,
    parquet_files: list[str],
    year: int,
    basepath: str,
    crosswalks_path: Optional[str],
) -> str:
    """
    Build one harmonized SELECT branch for a single basepath.
    Crosswalks are applied inside the branch.
    """
    if not parquet_files:
        raise ValueError(f"Branch '{branch_name}' has no parquet files.")

    if table_cfg.get("pre_join"):
        apply_pre_joins(
            conn,
            table_cfg,
            parquet_files,
            year,
            basepath,
        )

        schema_lower = {
            row[1].lower(): str(row[2]).upper()
            for row in conn.execute(
                "PRAGMA table_info('raw_base')"
            ).fetchall()
        }
    else:
        schema_lower = _load_schema_lower(conn, parquet_files)

    # -----------------------
    # Base SELECT expressions
    # -----------------------
    select_exprs = _build_select_exprs(table_cfg, schema_lower, year)
    columns_str = ", ".join(select_exprs)
    files_str = ", ".join([f"'{_escape_sql_string(f)}'" for f in parquet_files])

    # -----------------------
    # Crosswalk join handling
    # -----------------------
    crosswalks = table_cfg.get("crosswalks") or []
    if not crosswalks:
        if table_cfg.get("pre_join"):
            return f"""
            SELECT {columns_str}
            FROM raw_base
            """

        return f"""
        SELECT {columns_str}
        FROM read_parquet([{files_str}], filename = true)
        """

    # Build CTEs
    ctes: list[str] = []

    if table_cfg.get("pre_join"):
        ctes.append(
            f"""base AS (
                SELECT {columns_str}
                FROM raw_base
            )""")

    else:
        ctes.append(
            f"""base AS (
                SELECT {columns_str}
                FROM read_parquet([{files_str}])
            )"""
        )

    join_clauses: list[str] = []

    exclude_base_cols = []
    for cw in crosswalks:
        min_year = cw.get("min_year")
        max_year = cw.get("max_year")

        if min_year is not None and year < int(min_year):
            continue
        if max_year is not None and year > int(max_year):
            continue

        select_block = cw.get("select") or {}
        exclude_cols = select_block.get("exclude_base") or []
        if not isinstance(exclude_cols, list):
            raise ValueError("select.exclude_base must be a list of original columns to exclude.")
        exclude_base_cols.extend([str(c) for c in exclude_cols])

    if exclude_base_cols:
        cols_to_exclude = ", ".join(exclude_base_cols)
        added_selects: list[str] = [f"base.* EXCLUDE ({cols_to_exclude})"]
    else:
        added_selects: list[str] = ["base.*"]

    for i, cw in enumerate(crosswalks):
        if not isinstance(cw, dict):
            raise ValueError(f"crosswalks[{i}] must be a mapping/dict.")
        min_year = cw.get("min_year")
        max_year = cw.get("max_year")

        if min_year is not None and year < int(min_year):
            continue
        if max_year is not None and year > int(max_year):
            continue
        
        name = cw.get("name") or f"cw{i+1}"
        cw_alias = f"cw_{name}"
        fmt = cw.get("format", "parquet")

        path_tmpl = cw.get("path")
        if not path_tmpl:
            raise ValueError(f"crosswalk '{name}' must include `path`.")

        cw_path = _resolve_template(
            str(path_tmpl),
            basepath=basepath,
            crosswalks_path=crosswalks_path,
            year=year,
        )

        read_expr = _cw_read_expr(cw_path, fmt)

        filters = cw.get("filters") or []
        where_sql = ""
        if filters:
            if not isinstance(filters, list):
                raise ValueError(f"crosswalk '{name}': filters must be a list of SQL expressions.")
            # Allow {year} substitution in filters
            filt_sql = []
            for fexpr in filters:
                fexpr_s = str(fexpr).replace("{year}", str(year))
                filt_sql.append(f"({fexpr_s})")
            where_sql = "WHERE " + " AND ".join(filt_sql)

        ctes.append(
            f"""{cw_alias} AS (
                SELECT *
                FROM {read_expr}
                {where_sql}
            )"""
        )

        join = cw.get("join") or {}
        how = str((join.get("how") or "left")).strip().upper()
        if how not in ("LEFT", "INNER"):
            # keep small set to avoid surprises; can expand later
            raise ValueError(f"crosswalk '{name}': join.how must be 'left' or 'inner'.")

        on_map = (join.get("on") or {})
        if not isinstance(on_map, dict) or not on_map:
            raise ValueError(f"crosswalk '{name}': join.on must be a mapping of left_col -> right_col.")

        on_parts = [f"base.{lcol} = {cw_alias}.{rcol}" for lcol, rcol in on_map.items()]

        # effective-dated join (optional)
        eff = join.get("effective_date")
        if eff:
            if not isinstance(eff, dict):
                raise ValueError(f"crosswalk '{name}': join.effective_date must be a dict.")
            left_date = eff.get("left_date")
            start_col = eff.get("start_col")
            end_col = eff.get("end_col")
            if not (left_date and start_col and end_col):
                raise ValueError(
                    f"crosswalk '{name}': effective_date requires left_date, start_col, end_col."
                )
            # Open-ended end_date supported: end_col IS NULL OR left_date <= end_col
            on_parts.append(f"base.{left_date} >= {cw_alias}.{start_col}")
            on_parts.append(f"({cw_alias}.{end_col} IS NULL OR base.{left_date} <= {cw_alias}.{end_col})")

        on_sql = " AND ".join(on_parts)

        select_block = cw.get("select") or {}
        add_map = (select_block.get("add") or {})
        if not isinstance(add_map, dict):
            raise ValueError(f"crosswalk '{name}': select.add must be a mapping of output_col -> crosswalk_col.")

        for out_col, cw_col in add_map.items():
            added_selects.append(f"{cw_alias}.{cw_col} AS {out_col}")
        
        coalesce_map = select_block.get("coalesce") or {}
        if not isinstance(coalesce_map, dict):
            raise ValueError(
                f"crosswalk '{name}': select.coalesce must be a mapping of output_col -> list of expressions."
            )

        for out_col, exprs in coalesce_map.items():
            if not isinstance(exprs, list) or len(exprs) < 2:
                raise ValueError(
                    f"crosswalk '{name}': select.coalesce.{out_col} must be a list with at least two expressions."
                )

            resolved_exprs = []
            for expr in exprs:
                expr = str(expr)
                expr = expr.replace("cw.", f"{cw_alias}.")
                resolved_exprs.append(expr)

            if out_col == "bene_id":
                added_selects.insert(0, f"COALESCE({', '.join(resolved_exprs)}) AS {out_col}")
            else:
                added_selects.append(f"COALESCE({', '.join(resolved_exprs)}) AS {out_col}")

        join_clauses.append(f"{how} JOIN {cw_alias} ON {on_sql}")

    # Final query with joins
    joined_from = "FROM base\n" + "\n".join(join_clauses)
    final_select = ",\n            ".join(added_selects)

    ctes_sql = ",\n        ".join(ctes)

    return f"""
    WITH
        {ctes_sql}
    SELECT
            {final_select}
    {joined_from}
    """


def construct_query(
    conn: duckdb.DuckDBPyConnection,
    table_cfg: dict,
    parquet_files_by_basepath: list[tuple[str, list[str]]],
    year: int,
    *,
    crosswalks_path: Optional[str] = None,
    out_table: Optional[str] = None,
) -> str:
    """
    Build DuckDB SQL by:
      - harmonizing each basepath separately
      - optionally applying crosswalk joins within each branch
      - UNION ALL BY NAME across branches
    The SQL creates table `out_table`, by default the YAML's table_name.
    """
    table_name = table_cfg.get("table_name") or table_cfg.get("name")
    if not table_name:
        raise ValueError("Per-table YAML must include `table_name`.")

    if not parquet_files_by_basepath:
        raise ValueError("No parquet file groups were provided.")

    branch_sqls: list[str] = []
    for i, (bp, parquet_files) in enumerate(parquet_files_by_basepath, start=1):
        branch_sql = _build_branch_sql(
            conn,
            branch_name=f"branch_{i}",
            table_cfg=table_cfg,
            parquet_files=parquet_files,
            year=year,
            basepath=bp,
            crosswalks_path=crosswalks_path,
        )
        branch_sqls.append(f"(\n{branch_sql}\n)")

    union_sql = "\nUNION ALL BY NAME\n".join(branch_sqls)

    return f"""
    CREATE OR REPLACE TABLE {out_table or table_name} AS
    {union_sql};
    """


def harmonize_table_year(
    table_config_path: str,
    basepath: str | Iterable[str],
    output_path: str,
    year: int,
    *,
    crosswalks_path: Optional[str] = None,
    duckdb_settings: Optional[Mapping] = None,
) -> str:
    """
    Harmonize one table for one year, write:
      output_path/<table_name>_<year>.parquet
    Also writes:
      output_path/<table_name>_<year>.run.json
    Returns output file path.

    A table with a `rollup:` block takes its TAF rows from the stay build (build_table_stays;
    `_harmonize_rollup_year`). `duckdb_settings`: optional memory_limit / threads / temp_directory.
    """
    t0 = time.perf_counter()
    started_utc = datetime.now(timezone.utc).isoformat()

    table_cfg = load_yaml(table_config_path)
    if table_cfg.get("rollup"):
        return _harmonize_rollup_year(
            table_config_path, table_cfg, basepath, output_path, year,
            crosswalks_path=crosswalks_path, duckdb_settings=duckdb_settings,
            t0=t0, started_utc=started_utc)

    path_patterns = table_cfg.get("path_pattern", [])
    if isinstance(path_patterns, str):
        path_patterns = [path_patterns]

    table_name = table_cfg.get("table_name")
    if not table_name:
        raise ValueError("Per-table YAML must include `table_name`.")

    os.makedirs(output_path, exist_ok=True)
    run_file = os.path.join(output_path, f"{table_name}_{year}.run.json")

    basepaths = _normalize_basepaths(basepath)

    parquet_files_by_basepath: list[tuple[str, list[str]]] = []
    for bp in basepaths:
        files = get_parquet_files(basepath=bp, year=year, path_patterns=path_patterns)
        if files:
            parquet_files_by_basepath.append((bp, files))

    n_input_files = sum(len(files) for _, files in parquet_files_by_basepath)

    if not parquet_files_by_basepath:
        payload = {
            "status": "error",
            "table": table_name,
            "year": year,
            "started_utc": started_utc,
            "duration_sec": time.perf_counter() - t0,
            "n_input_files": 0,
            "output_file": None,
            "table_config_path": table_config_path,
            "basepath": _jsonable_basepath(basepath),
            "crosswalks_path": crosswalks_path,
            "error": f"No parquet parts found using patterns={path_patterns}",
        }
        with open(run_file, "w") as f:
            json.dump(payload, f, indent=2)
        raise FileNotFoundError(payload["error"])

    conn = duckdb.connect(database=":memory:")
    _apply_duckdb_settings(conn, duckdb_settings)

    query = construct_query(
        conn,
        table_cfg,
        parquet_files_by_basepath,
        year,
        crosswalks_path=crosswalks_path,
    )
    conn.execute(query)

    out_file = os.path.join(output_path, f"{table_name}_{year}.parquet")
    conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{_escape_sql_string(out_file)}' (FORMAT 'parquet')")
    conn.close()

    payload = {
        "status": "ok",
        "table": table_name,
        "year": year,
        "started_utc": started_utc,
        "duration_sec": time.perf_counter() - t0,
        "n_input_files": n_input_files,
        "n_input_branches": len(parquet_files_by_basepath),
        "input_basepaths_used": [bp for bp, _ in parquet_files_by_basepath],
        "output_file": out_file,
        "table_config_path": table_config_path,
        "basepath": _jsonable_basepath(basepath),
        "crosswalks_path": crosswalks_path,
        "crosswalks": [
        cw.get("name")
        for cw in (table_cfg.get("crosswalks") or [])
        if isinstance(cw, dict)
        and (cw.get("min_year") is None or year >= int(cw.get("min_year")))
        and (cw.get("max_year") is None or year <= int(cw.get("max_year")))
        ],
    }
    with open(run_file, "w") as f:
        json.dump(payload, f, indent=2)

    return out_file


# ---------------------------------------------------------------------------------------------
# Stay rollup wiring (IP-R54, IP-R55; harmonizecms/stays.py). Used only by tables whose YAML has
# a `rollup:` block; every other table takes the code above unchanged.
# ---------------------------------------------------------------------------------------------

# Optional duckdb settings (Hydra `++duckdb.<key>=`), applied with SET
DUCKDB_SETTINGS = ("memory_limit", "threads", "temp_directory")
# Code the stay build depends on (IP-R54 f, IP-R55 d), hashed into the build run.json
BUILD_CODE_FILES = ("stays.py", "harmonizer.py", "io.py")
# Parquet key-value metadata key of the stays file holding its build's nonce, which is also the
# build_id (IP-R56 c). The guard compares it, not the file mtime: Snakemake touches every job output
# after the job (snakemake dag.py)
STAYS_NONCE_KEY = "harmonizecms_stays_nonce"
REBUILD_HINT = ("rebuild the stays: Snakemake rule build_stays, or "
                "`python run_harmonizer.py table=<table> ++mode=stays '++years=[...]'`")


def stays_build_files(output_path: str, table_name: str) -> tuple[str, str]:
    """(stays parquet, build run.json) of a rollup table, in `_build/` so `{table}_*.parquet` globs
    of the yearly files don't pick them up."""
    build_dir = os.path.join(output_path, "_build")
    return (os.path.join(build_dir, f"{table_name}_stays.parquet"),
            os.path.join(build_dir, f"{table_name}_stays.run.json"))


def _check_duckdb_settings(settings: Optional[Mapping]) -> dict:
    """`settings` as a plain dict; ValueError on a key other than DUCKDB_SETTINGS (a typo)."""
    settings = dict(settings or {})
    unknown = sorted(set(settings) - set(DUCKDB_SETTINGS))
    if unknown:
        raise ValueError(f"Unknown duckdb settings {unknown}; allowed: {list(DUCKDB_SETTINGS)}")
    return settings


def _apply_duckdb_settings(conn: duckdb.DuckDBPyConnection, settings: Optional[Mapping]) -> dict:
    """SETs the DUCKDB_SETTINGS given (None or empty values skipped); returns what was applied."""
    applied: dict = {}
    for k, v in _check_duckdb_settings(settings).items():
        if v is None or v == "":
            continue
        if k == "threads":
            conn.execute(f"SET threads = {int(v)}")
            applied[k] = int(v)
        else:
            conn.execute(f"SET {k} = '{_escape_sql_string(str(v))}'")
            applied[k] = str(v)
    return applied


def _resolve_from_yaml(table_config_path: str, path: str) -> str:
    """An absolute `path` as is; a relative one against the table YAML's folder (rollup.cutover)."""
    path = str(path)
    if os.path.isabs(path):
        return path
    return os.path.join(os.path.dirname(str(table_config_path)), path)


def _path_patterns(table_cfg: dict) -> list[str]:
    patterns = table_cfg.get("path_pattern", [])
    return [patterns] if isinstance(patterns, str) else list(patterns)


def _rollup_patterns(table_cfg: dict) -> list[str]:
    """
    rollup.path_pattern (the rollup's own files, TAF) as a list. ValueError when it is missing or
    an entry is not also in the table's path_pattern.
    """
    patterns = (table_cfg.get("rollup") or {}).get("path_pattern")
    if not patterns:
        raise ValueError("rollup.path_pattern (the files the stay build reads) is missing (IP-R54 d)")
    if isinstance(patterns, str):
        patterns = [patterns]
    outside = [p for p in patterns if p not in _path_patterns(table_cfg)]
    if outside:
        raise ValueError(f"rollup.path_pattern entries {outside} are not in the table's path_pattern")
    return list(patterns)


def _slash(path: str) -> str:
    """`path` with `/` separators and no repeated `/` (for matching glob output)."""
    return re.sub(r"/+", "/", str(path).replace("\\", "/"))


def _pattern_regex(pattern: str) -> re.Pattern:
    """Regex of a path pattern ({basepath} already substituted): {year} -> 4 digits, * -> one
    path component's characters, the rest literal."""
    out, seen_year = [], False
    for part in re.split(r"(\{year\}|\*)", _slash(pattern)):
        if part == "{year}":
            out.append(r"(?P=year)" if seen_year else r"(?P<year>\d{4})")
            seen_year = True
        elif part == "*":
            out.append(r"[^/]*")
        else:
            out.append(re.escape(part))
    return re.compile("".join(out))


def _years_on_disk(basepaths: str | Iterable[str], patterns: list[str]) -> list[int]:
    """Years with files under `patterns` in any basepath (IP-R54 a): {year} globbed as `*`, then
    kept only where that path component is exactly 4 digits (so `2016_old` is not a year)."""
    years: set[int] = set()
    for bp in _normalize_basepaths(basepaths):
        for pattern in patterns:
            resolved = pattern.replace("{basepath}", bp)
            if "{year}" not in resolved:
                continue
            rx = _pattern_regex(resolved)
            for f in glob.glob(resolved.replace("{year}", "*")):
                m = rx.fullmatch(_slash(f))
                if m:
                    years.add(int(m.group("year")))
    return sorted(years)


def _files_fingerprint(files: list[str]) -> dict:
    """{"n": files, "sha256": hash of the sorted (normpath, size, mtime_ns)} (IP-R54, WPR-9)."""
    entries = []
    for f in files:
        st = os.stat(f)
        entries.append([os.path.normpath(f), st.st_size, st.st_mtime_ns])
    entries.sort()
    return {"n": len(entries), "sha256": hashlib.sha256(json.dumps(entries).encode("utf-8")).hexdigest()}


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _code_hashes() -> dict[str, str]:
    here = os.path.dirname(os.path.abspath(__file__))
    return {f"harmonizecms/{name}": _sha256_file(os.path.join(here, name)) for name in BUILD_CODE_FILES}


def _normpath_basepaths(basepath: str | Iterable[str]) -> list[str]:
    return [os.path.normpath(bp) for bp in _normalize_basepaths(basepath)]


def _normpath_basepath_json(basepath: str | Iterable[str]) -> str | list[str]:
    if isinstance(basepath, str):
        return os.path.normpath(basepath)
    return _normpath_basepaths(basepath)


def _normpath_optional(path: Optional[str]) -> Optional[str]:
    return os.path.normpath(str(path)) if path else None


def _yaml_types(table_cfg: dict) -> dict[str, str]:
    """{column: YAML type} of every table column; ValueError for a column without `type`."""
    types: dict[str, str] = {}
    for col in table_cfg.get("columns") or []:
        name = list(col.keys())[0]
        types[name] = (col[name] or {}).get("type")
    untyped = [c for c, t in types.items() if not t]
    if untyped:
        raise ValueError(f"Columns without a `type` (the stay build types its input from the YAML): {untyped}")
    return types


def _remove_if_exists(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def build_table_stays(
    table_config_path: str,
    basepath: str | Iterable[str],
    output_path: str,
    years: Iterable[int],
    *,
    crosswalks_path: Optional[str] = None,
    duckdb_settings: Optional[Mapping] = None,
) -> str:
    """
    Builds the stays of a table with a `rollup:` block once from the rollup files
    (`rollup.path_pattern`, TAF) of all `years` (IP-R54 a), steps 1-4 of harmonizecms/stays.py in a
    file-backed duckdb in a temp dir (IP-R54 d), and writes `_build/{table}_stays.parquet` plus
    `_build/{table}_stays.run.json` (written last; status ok). harmonize_table_year then takes each
    year's stays from that file. Returns the stays parquet path.

    The old build files are removed first. On an error after that, no parquet is left and the
    run.json gets status "error"; the error is re-raised. `duckdb_settings`: optional
    memory_limit / threads / temp_directory (the temp dir holding the build DB and its spill is
    made inside temp_directory, else the system temp dir, and removed at the end).
    """
    t0 = time.perf_counter()
    started_utc = datetime.now(timezone.utc).isoformat()
    cfg = load_yaml(table_config_path)
    rollup = cfg.get("rollup")
    if not rollup:
        raise ValueError(f"{table_config_path} has no `rollup:` block: stays are built only for rollup tables")
    table_name = cfg.get("table_name")
    if not table_name:
        raise ValueError("Per-table YAML must include `table_name`.")
    patterns = _rollup_patterns(cfg)
    if not rollup.get("cutover"):
        raise ValueError("rollup.cutover (the cutover.yaml path, IP-R40) is missing (IP-R54 d)")
    cutover_path = _resolve_from_yaml(table_config_path, rollup["cutover"])
    settings = _check_duckdb_settings(duckdb_settings)

    parquet_path, run_path = stays_build_files(output_path, table_name)
    tmp_parquet = parquet_path + ".tmp"
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    # First, so a crash or an OS kill (no `except`) can't leave an "ok" build behind
    for p in (run_path, parquet_path, tmp_parquet):
        _remove_if_exists(p)

    payload: dict = {
        "status": "error",
        "mode": "stays",
        "table": table_name,
        "years": None,
        "started_utc": started_utc,
        "duration_sec": None,
        "table_config_path": table_config_path,
        "cutover_path": cutover_path,
        "basepath": _normpath_basepath_json(basepath),
        "crosswalks_path": _normpath_optional(crosswalks_path),
    }
    try:
        years_run = sorted({int(y) for y in years})
        payload["years"] = years_run
        if not years_run:
            raise ValueError("build_table_stays needs at least one year")
        payload |= _build_stays(cfg, table_config_path, cutover_path, patterns, basepath, years_run,
                                crosswalks_path, settings, parquet_path, tmp_parquet, started_utc)
    except Exception as e:
        _remove_if_exists(tmp_parquet)
        _remove_if_exists(parquet_path)
        payload["status"] = "error"
        payload["error"] = repr(e)
        payload["duration_sec"] = time.perf_counter() - t0
        with open(run_path, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        raise
    payload["status"] = "ok"
    payload["duration_sec"] = time.perf_counter() - t0
    with open(run_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    LOGGER.info("Wrote %s (%s stays)", parquet_path, payload["output_rows"])
    return parquet_path


def _build_stays(cfg: dict, table_config_path: str, cutover_path: str, patterns: list[str],
                 basepath: str | Iterable[str], years: list[int], crosswalks_path: Optional[str],
                 settings: dict, parquet_path: str, tmp_parquet: str, started_utc: str) -> dict:
    """The body of build_table_stays after its checks; returns the run.json keys it adds."""
    rollup = cfg["rollup"]
    basepaths = _normalize_basepaths(basepath)
    warnings: list[str] = []
    fingerprint = {"table_config_sha256": _sha256_file(table_config_path),
                   "cutover_sha256": _sha256_file(cutover_path),
                   "code_sha256": _code_hashes()}
    first_taf_year = load_first_taf_year(cutover_path)
    yaml_types = _yaml_types(cfg)

    tmp_root = settings.get("temp_directory") or None
    if tmp_root:
        os.makedirs(tmp_root, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix=f"{cfg['table_name']}_stays_", dir=tmp_root)
    try:
        conn = duckdb.connect(os.path.join(tmp, "build.duckdb"))
        try:
            applied = _apply_duckdb_settings(conn, {k: v for k, v in settings.items() if k != "temp_directory"})
            spill = os.path.join(tmp, "spill")
            conn.execute(f"SET temp_directory = '{_escape_sql_string(spill)}'")
            memory_limit = applied.get("memory_limit")
            if memory_limit is None:
                current = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
                memory_limit = f"default ({current})"
                LOGGER.warning(f"No duckdb memory_limit set for the stay build: duckdb's default {current} "
                               f"(80% of RAM) may exceed the job's memory; pass ++duckdb.memory_limit=")
            threads = conn.execute("SELECT current_setting('threads')").fetchone()[0]

            # Rollup rows of every year, typed from the YAML (a typed empty table without files)
            cols_sql = ", ".join(f"{c} {t}" for c, t in yaml_types.items())
            conn.execute(f"CREATE TABLE stay_input ({cols_sql}, _file_year INTEGER)")
            taf_files_by_year, taf_rows_by_year = {}, {}
            for y in years:
                groups, files_y = [], []
                for bp in basepaths:
                    files = get_parquet_files(basepath=bp, year=y, path_patterns=patterns)
                    if files:
                        groups.append((bp, files))
                        files_y.extend(files)
                taf_files_by_year[str(y)] = _files_fingerprint(files_y)
                n_rows = 0
                if groups:
                    conn.execute(construct_query(conn, cfg, groups, y, crosswalks_path=crosswalks_path,
                                                 out_table="_stay_year_rows"))
                    conn.execute(f"INSERT INTO stay_input BY NAME SELECT *, {int(y)} AS _file_year "
                                 f"FROM _stay_year_rows")
                    n_rows = conn.execute("SELECT COUNT(*) FROM _stay_year_rows").fetchone()[0]
                    conn.execute("DROP TABLE _stay_year_rows")
                taf_rows_by_year[str(y)] = n_rows
            n_files = sum(v["n"] for v in taf_files_by_year.values())
            if not n_files:
                msg = f"no rollup (TAF) files for years {years}: the stays file is empty (check rollup.path_pattern)"
                LOGGER.warning(msg)
                warnings.append(msg)

            # Steps 1-4 (harmonizecms/stays.py)
            diag = prepare_claims(conn, rollup, "stay_input")
            drop_prepare_tables(conn, "stay_input")
            conn.execute("CHECKPOINT")
            diag |= prepare_links(conn, diag["link_state_fallback_years_input"])
            diag |= _stays.link_claims(conn, island_cap=_stays.ISLAND_CAP)
            diag |= aggregate_stays(conn, rollup, diag["file_years"], first_taf_year)
            conn.execute("DROP TABLE stay_claims")

            # Integrity before writing
            got = {r[0]: r[1] for r in conn.execute("DESCRIBE built_stays").fetchall()}
            want = stay_column_types(conn, cfg)
            if list(got.items()) != list(want.items()):
                raise RuntimeError(f"built_stays columns/types differ from the stay output: got {got}, want {want}")
            bad_years = sorted(r[0] for r in conn.execute(
                "SELECT DISTINCT year FROM built_stays").fetchall() if r[0] not in years)
            if bad_years:
                raise RuntimeError(f"built_stays has stay years {bad_years} outside the run's years {years}")
            # Sorted by year so a yearly read can skip other years' row groups; no row order is
            # promised (IP-R51 b). The nonce is the build_id (IP-R56 c); it identifies this file to the
            # yearly guard.
            nonce = uuid.uuid4().hex
            conn.execute(f"COPY (SELECT * FROM built_stays ORDER BY year) TO "
                         f"'{_escape_sql_string(tmp_parquet)}' "
                         f"(FORMAT parquet, KV_METADATA {{{STAYS_NONCE_KEY}: '{nonce}'}})")
            output_rows = conn.execute("SELECT COUNT(*) FROM built_stays").fetchone()[0]
        finally:
            conn.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    os.replace(tmp_parquet, parquet_path)
    st = os.stat(parquet_path)

    on_disk = _years_on_disk(basepaths, patterns)
    not_in_run = sorted(set(on_disk) - set(years))
    if not_in_run:
        msg = (f"rollup (TAF) years on disk not in the run: {not_in_run}; stays at the run's edge years and "
               f"Illinois families may be cut (IP-R54 a)")
        LOGGER.warning(msg)
        warnings.append(msg)
    return {
        "build_id": nonce,
        "output_file": parquet_path,
        "output_rows": output_rows,
        "output_size": st.st_size,
        "output_mtime_ns": st.st_mtime_ns,
        "duckdb": {"version": duckdb.__version__, "database": "file", "memory_limit": memory_limit,
                   "threads": threads, "temp_directory": tmp_root, "spill_directory": spill},
        "taf_files_by_year": taf_files_by_year,
        "taf_rows_by_year": taf_rows_by_year,
        "taf_years_on_disk": on_disk,
        "taf_years_on_disk_not_in_run": not_in_run,
        # C6 for flagged MAX years outside the run: the build reads no MAX files (IP-R54 c)
        "max_cutover_flagged_year_not_in_run": {
            s: y - 1 for s, y in sorted(first_taf_year.items()) if y - 1 not in years},
        "fingerprint": fingerprint,
        "warnings": warnings,
        "rollup": diag,
    }


def _check_stays_build(
    table_config_path: str,
    table_cfg: dict,
    basepaths: list[str],
    output_path: str,
    year: int,
    taf_files: list[str],
    *,
    crosswalks_path: Optional[str] = None,
) -> dict:
    """
    The yearly run's check that the stay build matches its inputs (stale build). Raises
    FileNotFoundError / ValueError naming the fix; returns {"build_id", "year_in_build",
    "warnings", "run", "stays_file", "stays_run_json"}. `taf_files`: year `year`'s rollup files.
    Code changes since the build are warnings only (OQ6).
    """
    table_name = table_cfg["table_name"]
    parquet_path, run_path = stays_build_files(output_path, table_name)
    if not os.path.exists(run_path):
        raise FileNotFoundError(f"Stay build run.json {run_path} not found; {REBUILD_HINT}")
    try:
        with open(run_path) as f:
            run = json.load(f)
    except (OSError, ValueError) as e:
        raise ValueError(f"Stay build run.json {run_path} is unreadable ({e}); {REBUILD_HINT}") from e
    if not isinstance(run, dict) or run.get("status") != "ok":
        status = run.get("status") if isinstance(run, dict) else None
        error = run.get("error") if isinstance(run, dict) else None
        raise ValueError(f"Stay build {run_path} has status {status!r} ({error}); {REBUILD_HINT}")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Stays file {parquet_path} not found; {REBUILD_HINT}")
    if run.get("table") != table_name:
        raise ValueError(f"Stay build {run_path} is for table {run.get('table')!r}, not {table_name!r}")
    # Size and the build's nonce = build_id (parquet key-value metadata), not mtime: Snakemake touches outputs
    c = duckdb.connect()
    try:
        p = _escape_sql_string(parquet_path)
        n_rows = c.execute(f"SELECT SUM(num_rows) FROM parquet_file_metadata('{p}')").fetchone()[0]
        nonce = c.execute(f"SELECT decode(value) FROM parquet_kv_metadata('{p}') "
                          f"WHERE decode(key) = '{STAYS_NONCE_KEY}'").fetchall()
    finally:
        c.close()
    st = os.stat(parquet_path)
    nonce = nonce[0][0] if len(nonce) == 1 else None
    if st.st_size != run.get("output_size") or nonce is None or nonce != run.get("build_id"):
        raise ValueError(f"Stays file {parquet_path} is not the file its build wrote (size/nonce); {REBUILD_HINT}")
    if n_rows != run.get("output_rows"):
        raise ValueError(f"Stays file {parquet_path} has {n_rows} rows, its build wrote {run.get('output_rows')}; "
                         f"{REBUILD_HINT}")
    build_years = [int(y) for y in run.get("years") or []]
    year_in_build = int(year) in build_years
    if not year_in_build and taf_files:
        raise ValueError(f"Year {year} is not in the stay build (years {build_years}) but has {len(taf_files)} "
                         f"rollup (TAF) files; {REBUILD_HINT} including {year} (IP-R55 c)")
    built_bp = run.get("basepath")
    built_bp = [built_bp] if isinstance(built_bp, str) else list(built_bp or [])
    if _normpath_basepaths(basepaths) != built_bp:
        raise ValueError(f"basepath {_normpath_basepaths(basepaths)} differs from the stay build's {built_bp}; "
                         f"{REBUILD_HINT}")
    if _normpath_optional(crosswalks_path) != run.get("crosswalks_path"):
        raise ValueError(f"crosswalks_path {_normpath_optional(crosswalks_path)!r} differs from the stay build's "
                         f"{run.get('crosswalks_path')!r}; {REBUILD_HINT}")
    fp = run.get("fingerprint") or {}
    if _sha256_file(table_config_path) != fp.get("table_config_sha256"):
        raise ValueError(f"{table_config_path} changed since the stay build; {REBUILD_HINT}")
    cutover_path = _resolve_from_yaml(table_config_path, table_cfg["rollup"]["cutover"])
    if _sha256_file(cutover_path) != fp.get("cutover_sha256"):
        raise ValueError(f"{cutover_path} changed since the stay build; {REBUILD_HINT}")
    # Every build year: a stay of year Y can hold claims from the files of other years (IP-R05,
    # IP-R11, IP-R49 a)
    patterns = _rollup_patterns(table_cfg)
    stored = run.get("taf_files_by_year") or {}
    changed = []
    for y in build_years:
        files = [f for bp in basepaths for f in get_parquet_files(basepath=bp, year=y, path_patterns=patterns)]
        if _files_fingerprint(files) != stored.get(str(y)):
            changed.append(y)
    if changed:
        raise ValueError(f"Rollup (TAF) files of years {changed} changed since the stay build; {REBUILD_HINT}")
    warnings = []
    code = _code_hashes()
    built_code = fp.get("code_sha256") or {}
    code_changed = sorted(k for k in code if code[k] != built_code.get(k))
    if code_changed:
        msg = f"code changed since the stay build: {', '.join(code_changed)}"
        LOGGER.warning(msg)
        warnings.append(msg)
    if not year_in_build:
        warnings.append(f"year {year} is not in the stay build (years {build_years}) and has no rollup files: "
                        f"no stays (IP-R55 c)")
    return {"build_id": run.get("build_id"), "year_in_build": year_in_build, "warnings": warnings, "run": run,
            "stays_file": parquet_path, "stays_run_json": run_path}


def _harmonize_rollup_year(
    table_config_path: str,
    table_cfg: dict,
    basepath: str | Iterable[str],
    output_path: str,
    year: int,
    *,
    crosswalks_path: Optional[str],
    duckdb_settings: Optional[Mapping],
    t0: float,
    started_utc: str,
) -> str:
    """
    harmonize_table_year for a table with a `rollup:` block: the year's non-rollup (MAX) rows
    through max_rows_to_stays, plus the built stays whose `year` is this year (the rollup files
    are not read here). Checks the build first (_check_stays_build); on any error after that the
    year's old output parquet is removed and the run.json gets status "error".
    """
    year = int(year)
    table_name = table_cfg.get("table_name")
    if not table_name:
        raise ValueError("Per-table YAML must include `table_name`.")
    rollup = table_cfg["rollup"]
    patterns = _rollup_patterns(table_cfg)
    path_patterns = _path_patterns(table_cfg)
    if not rollup.get("cutover"):
        raise ValueError("rollup.cutover (the cutover.yaml path, IP-R40) is missing (IP-R54 d)")
    _check_duckdb_settings(duckdb_settings)

    os.makedirs(output_path, exist_ok=True)
    run_file = os.path.join(output_path, f"{table_name}_{year}.run.json")
    out_file = os.path.join(output_path, f"{table_name}_{year}.parquet")
    basepaths = _normalize_basepaths(basepath)

    max_groups: list[tuple[str, list[str]]] = []
    taf_files: list[str] = []
    n_all = 0
    for bp in basepaths:
        all_files = get_parquet_files(basepath=bp, year=year, path_patterns=path_patterns)
        rollup_files = get_parquet_files(basepath=bp, year=year, path_patterns=patterns)
        rollup_set = set(rollup_files)
        n_all += len(all_files)
        taf_files.extend(rollup_files)
        max_files = [f for f in all_files if f not in rollup_set]
        if max_files:
            max_groups.append((bp, max_files))

    def error_payload(error: str) -> dict:
        return {
            "status": "error",
            "table": table_name,
            "year": year,
            "started_utc": started_utc,
            "duration_sec": time.perf_counter() - t0,
            "n_input_files": 0,
            "output_file": None,
            "table_config_path": table_config_path,
            "basepath": _jsonable_basepath(basepath),
            "crosswalks_path": crosswalks_path,
            "error": error,
        }

    if not n_all:  # as for every table
        payload = error_payload(f"No parquet parts found using patterns={path_patterns}")
        with open(run_file, "w") as f:
            json.dump(payload, f, indent=2)
        raise FileNotFoundError(payload["error"])

    try:
        guard = _check_stays_build(table_config_path, table_cfg, basepaths, output_path, year, taf_files,
                                   crosswalks_path=crosswalks_path)
        rollup_info = _rollup_year_rows(table_config_path, table_cfg, year, max_groups, guard,
                                        crosswalks_path, duckdb_settings, out_file)
    except Exception as e:
        _remove_if_exists(out_file)  # a refused or failed year leaves no stale output (WPR-6.3)
        with open(run_file, "w") as f:
            json.dump(error_payload(repr(e)), f, indent=2)
        raise

    n_max_files = sum(len(files) for _, files in max_groups)
    payload = {
        "status": "ok",
        "table": table_name,
        "year": year,
        "started_utc": started_utc,
        "duration_sec": time.perf_counter() - t0,
        "n_input_files": n_max_files,
        "n_input_branches": len(max_groups),
        "input_basepaths_used": [bp for bp, _ in max_groups],
        "output_file": out_file,
        "table_config_path": table_config_path,
        "basepath": _jsonable_basepath(basepath),
        "crosswalks_path": crosswalks_path,
        "crosswalks": [
        cw.get("name")
        for cw in (table_cfg.get("crosswalks") or [])
        if isinstance(cw, dict)
        and (cw.get("min_year") is None or year >= int(cw.get("min_year")))
        and (cw.get("max_year") is None or year <= int(cw.get("max_year")))
        ],
        "n_input_files_max": n_max_files,
        "rollup": rollup_info,
    }
    with open(run_file, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    return out_file


def _rollup_year_rows(table_config_path: str, table_cfg: dict, year: int,
                      max_groups: list[tuple[str, list[str]]], guard: dict, crosswalks_path: Optional[str],
                      duckdb_settings: Optional[Mapping], out_file: str) -> dict:
    """MAX rows + the year's built stays -> `out_file`; returns the run.json `rollup` block."""
    table_name = table_cfg["table_name"]
    rollup = table_cfg["rollup"]
    first_taf_year = load_first_taf_year(_resolve_from_yaml(table_config_path, rollup["cutover"]))
    conn = duckdb.connect(database=":memory:")
    try:
        _apply_duckdb_settings(conn, duckdb_settings)
        types = stay_column_types(conn, table_cfg)
        if max_groups:
            conn.execute(construct_query(conn, table_cfg, max_groups, year, crosswalks_path=crosswalks_path,
                                         out_table="_max_rows"))
        else:  # no MAX files this year: a typed empty input, so the counts are still computed
            sel = ", ".join(f"CAST(NULL AS {t}) AS {c}" for c, t in _yaml_types(table_cfg).items())
            conn.execute(f"CREATE TABLE _max_rows AS SELECT {sel} LIMIT 0")
        diag = max_rows_to_stays(conn, "_max_rows", year, rollup, first_taf_year, types, out_table=table_name)
        conn.execute("DROP TABLE _max_rows")
        cols = ", ".join(STAY_COLUMNS)
        stays = _escape_sql_string(guard["stays_file"])
        conn.execute(f"INSERT INTO {table_name} ({cols}) SELECT {cols} FROM read_parquet('{stays}') "
                     f"WHERE year = {year}")
        n_total, claims_in_stays = conn.execute(
            f"SELECT COUNT(*), CAST(COALESCE(SUM(n_claims_combined), 0) AS BIGINT) FROM {table_name}").fetchone()
        sharing = admissions_key_sharing(conn, table_name)
        # IP-R40 E26: state-years with both MAX and TAF rows
        same = {}
        for state, n_taf, n_max in conn.execute(f"""
                SELECT state, COUNT(*) FILTER (WHERE source_type = 'taf'),
                    COUNT(*) FILTER (WHERE source_type IS DISTINCT FROM 'taf')
                FROM {table_name} GROUP BY state ORDER BY state NULLS LAST""").fetchall():
            if n_taf and n_max:
                same["NULL" if state is None else str(state)] = {"max": n_max, "taf": n_taf}
        conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{_escape_sql_string(out_file)}' (FORMAT 'parquet')")
    finally:
        conn.close()
    run = guard["run"]
    return {
        "build_id": guard["build_id"],
        "year_in_build": guard["year_in_build"],
        "stays_file": guard["stays_file"],
        "stays_run_json": guard["stays_run_json"],
        "build_started_utc": run.get("started_utc"),
        "max_taf_rows_same_state_year": same,
        "taf_files_in_build": sum((v or {}).get("n", 0) for v in (run.get("taf_files_by_year") or {}).values()),
        "stay_rows": n_total - diag["max_rows"],
        "claims_in_stays": claims_in_stays,
        **diag,
        "rows_sharing_admissions_key": sharing,
        "warnings": guard["warnings"],
    }
