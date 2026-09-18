from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Optional

import duckdb
import yaml

from .io import get_parquet_files


def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _load_schema_lower(conn: duckdb.DuckDBPyConnection, parquet_file: str) -> dict[str, str]:
    df = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_file}') LIMIT 0"
    ).fetchdf()
    return {row["column_name"].lower(): str(row["column_type"]).upper() for _, row in df.iterrows()}


def _select_first_existing(schema_lower: dict[str, str], candidates) -> Optional[str]:
    if candidates is None:
        return None
    if isinstance(candidates, str):
        candidates = [candidates]
    for c in candidates:
        if c and c.lower() in schema_lower:
            return c
    return None


def _cast_expr(
    selected_source: str,
    schema_lower: dict[str, str],
    cast_dict: dict[str, str] | None,
    year: int,
) -> str:
    cast_dict_norm = {str(k).upper(): v for k, v in (cast_dict or {}).items()}
    src_type = schema_lower.get(selected_source.lower(), "")
    template = cast_dict_norm.get(src_type) or cast_dict_norm.get("*") or "{column_name}"
    return template.format(column_name=selected_source, year=year)


def _construct_array_expr(col_def: dict, schema_lower: dict[str, str], year: int) -> str:
    """
    Array contract:
      - if any source_array exists -> use that, applying cast dict
      - else build ARRAY[...] from ONE chosen component schema:
          * source_component_sets: list of lists; choose first set with any existing col
          * else fallback to source_components
      - missing components become NULL (preserves array length)
    """
    source_array = col_def.get("source_array") or []
    source_components = col_def.get("source_components") or []
    source_component_sets = col_def.get("source_component_sets") or []
    cast_dict = col_def.get("cast") or {}
    element_cast = col_def.get("element_cast")

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
        return "NULL"

    def elem(component_col: str) -> str:
        if component_col.lower() not in schema_lower:
            return "NULL"
        if element_cast:
            return str(element_cast).format(column_name=component_col, year=year)
        return component_col

    array_expr = f"ARRAY[{', '.join(elem(c) for c in components_to_use)}]"

    if col_def.get("drop_nulls", False):
        return f"list_filter({array_expr}, x -> x IS NOT NULL)"

    return array_expr


def _escape_sql_string(s: str) -> str:
    # Escape single quotes for safe embedding in SQL string literals
    return s.replace("'", "''")


def _resolve_template(path_template: str, *, basepath: str, crosswalks_path: Optional[str], year: int) -> str:
    """
    Resolve {basepath}, {crosswalks_path}, {year} placeholders.
    """
    resolved = str(path_template)
    resolved = resolved.replace("{basepath}", basepath).replace("{year}", str(year))
    if "{crosswalks_path}" in resolved:
        if not crosswalks_path:
            raise ValueError("crosswalks_path is required because {crosswalks_path} is used in a crosswalk path.")
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


def construct_query(
    conn: duckdb.DuckDBPyConnection,
    table_cfg: dict,
    parquet_files: list[str],
    year: int,
    *,
    basepath: str,
    crosswalks_path: Optional[str] = None,
) -> str:
    """
    Build the DuckDB SQL for:
      - harmonizing columns (existing behavior)
      - optional crosswalk joins (new behavior)
    """

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
        schema_lower = _load_schema_lower(conn, parquet_files[0])

    select_exprs: list[str] = []

    table_name = table_cfg.get("table_name") or table_cfg.get("name")
    if not table_name:
        raise ValueError("Per-table YAML must include `table_name`.")

    # -----------------------
    # Base SELECT expressions
    # -----------------------
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
                select_exprs.append(f"NULL AS {col_name}")
            continue

        expr = _cast_expr(selected_source, schema_lower, col_def.get("cast"), year)
        select_exprs.append(f"{expr} AS {col_name}")

    columns_str = ", ".join(select_exprs)
    files_str = ", ".join([f"'{_escape_sql_string(f)}'" for f in parquet_files])

    # -----------------------
    # Crosswalk join handling
    # -----------------------
    crosswalks = table_cfg.get("crosswalks") or []
    if not crosswalks:
        if table_cfg.get("pre_join"):
            return f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT {columns_str}
            FROM raw_base;
            """

        return f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT {columns_str}
        FROM read_parquet([{files_str}]);
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
    CREATE OR REPLACE TABLE {table_name} AS
    WITH
        {ctes_sql}
    SELECT
            {final_select}
    {joined_from};
    """


def harmonize_table_year(
    table_config_path: str,
    basepath: str,
    output_path: str,
    year: int,
    *,
    crosswalks_path: Optional[str] = None,
) -> str:
    """
    Harmonize one table for one year, write:
      output_path/<table_name>_<year>.parquet
    Also writes:
      output_path/<table_name>_<year>.run.json
    Returns output file path.
    """
    t0 = time.perf_counter()
    started_utc = datetime.now(timezone.utc).isoformat()

    table_cfg = load_yaml(table_config_path)

    path_patterns = table_cfg.get("path_pattern", [])
    if isinstance(path_patterns, str):
        path_patterns = [path_patterns]

    parquet_files = get_parquet_files(basepath=basepath, year=year, path_patterns=path_patterns)
    table_name = table_cfg.get("table_name")

    os.makedirs(output_path, exist_ok=True)
    run_file = os.path.join(output_path, f"{table_name}_{year}.run.json")

    if not parquet_files:
        payload = {
            "status": "error",
            "table": table_name,
            "year": year,
            "started_utc": started_utc,
            "duration_sec": time.perf_counter() - t0,
            "n_input_files": 0,
            "output_file": None,
            "table_config_path": table_config_path,
            "basepath": basepath,
            "crosswalks_path": crosswalks_path,
            "error": f"No parquet parts found using patterns={path_patterns}",
        }
        with open(run_file, "w") as f:
            json.dump(payload, f, indent=2)
        raise FileNotFoundError(payload["error"])

    conn = duckdb.connect(database=":memory:")

    query = construct_query(
        conn,
        table_cfg,
        parquet_files,
        year,
        basepath=basepath,
        crosswalks_path=crosswalks_path,
    )
    conn.execute(query)

    out_file = os.path.join(output_path, f"{table_name}_{year}.parquet")
    conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{out_file}' (FORMAT 'parquet')")
    conn.close()

    payload = {
        "status": "ok",
        "table": table_name,
        "year": year,
        "started_utc": started_utc,
        "duration_sec": time.perf_counter() - t0,
        "n_input_files": len(parquet_files),
        "output_file": out_file,
        "table_config_path": table_config_path,
        "basepath": basepath,
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
