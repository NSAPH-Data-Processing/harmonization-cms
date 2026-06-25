from __future__ import annotations

import csv
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import duckdb
import yaml


def _read_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _describe_parquet(conn: duckdb.DuckDBPyConnection, parquet_path: str) -> dict[str, str]:
    df = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}') LIMIT 0"
    ).fetchdf()
    return {row["column_name"]: str(row["column_type"]) for _, row in df.iterrows()}


def _schema_lower(schema: dict[str, str]) -> dict[str, str]:
    return {k.lower(): v for k, v in schema.items()}


def _select_first_existing(schema_lower: dict[str, str], candidates) -> Optional[str]:
    if candidates is None:
        return None
    if isinstance(candidates, str):
        candidates = [candidates]
    for c in candidates:
        if c and c.lower() in schema_lower:
            return c
    return None


def _choose_component_set(schema_lower: dict[str, str], sets: list[list[str]]) -> tuple[Optional[int], list[str]]:
    for i, s in enumerate(sets):
        if any(c.lower() in schema_lower for c in s):
            return i, s
    return None, []


def _safe_load_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def _qident(name: str) -> str:
    # Quote SQL identifiers safely for DuckDB
    return '"' + str(name).replace('"', '""') + '"'


def _resolve_template(path_template: str, *, basepath: str, crosswalks_path: Optional[str], year: int) -> str:
    """
    Resolve {basepath}, {crosswalks_path}, {year} placeholders.
    """
    resolved = str(path_template).replace("{basepath}", basepath).replace("{year}", str(year))
    if "{crosswalks_path}" in resolved:
        if not crosswalks_path:
            raise ValueError("crosswalks_path is required because {crosswalks_path} is used in a crosswalk path.")
        resolved = resolved.replace("{crosswalks_path}", crosswalks_path)
    return resolved


def run_qc(
    table_config_path: str,
    basepath: str,
    output_parquet_path: str,
    year: int,
    qc_dir: Optional[str] = None,
    sample_n: int = 100,
    crosswalks_path: Optional[str] = None,
) -> dict:
    """
    Writes QC artifacts next to the output parquet (qc_dir defaults to parquet folder).
    Never raises on QC failure: returns a qc dict with status=error and error message.

    Crosswalk QA/QC:
      If table YAML includes `crosswalks`, add a `crosswalk_qc` block to qc.json with:
        - crosswalk file existence
        - join key presence in output
        - per-added-column null fraction overall
        - unmatched fraction among rows where join keys are present (added column is NULL)
    """
    t0 = time.perf_counter()
    started_utc = datetime.now(timezone.utc).isoformat()

    if qc_dir is None:
        qc_dir = os.path.dirname(output_parquet_path) or "."

    os.makedirs(qc_dir, exist_ok=True)

    table_cfg = _read_yaml(table_config_path)
    table_name = table_cfg.get("table_name") or table_cfg.get("name") or "unknown"

    # Where to write artifacts
    qc_json_path = os.path.join(qc_dir, "qc.json")
    mapping_csv_path = os.path.join(qc_dir, "mapping.csv")
    schema_before_path = os.path.join(qc_dir, "schema_before.json")
    schema_after_path = os.path.join(qc_dir, "schema_after.json")
    sample_path = os.path.join(qc_dir, "sample.parquet")
    run_metrics_path = os.path.join(os.path.dirname(output_parquet_path), f"{table_name}_{year}.run.json")

    qc: dict[str, Any] = {
        "status": "ok",
        "table": table_name,
        "year": year,
        "started_utc": started_utc,
        "duration_sec": None,
        "output_parquet": output_parquet_path,
        "warnings": [],
        "errors": [],
    }

    try:
        if not os.path.exists(output_parquet_path):
            raise FileNotFoundError(f"Output parquet not found: {output_parquet_path}")

        conn = duckdb.connect(database=":memory:")

        # ---------- BEFORE schema (from first input parquet file we can find) ----------
        path_patterns = table_cfg.get("path_pattern", [])
        if isinstance(path_patterns, str):
            path_patterns = [path_patterns]

        import glob
        input_candidates = []
        for pat in path_patterns:
            pat2 = pat.replace("{basepath}", basepath).replace("{year}", str(year))
            input_candidates.extend(glob.glob(pat2))
        input_first = input_candidates[0] if input_candidates else None

        schema_before = {}
        if input_first and os.path.exists(input_first):
            schema_before = _describe_parquet(conn, input_first)
        else:
            qc["warnings"].append(
                "Could not locate an input parquet to describe schema_before (patterns may not match directly)."
            )

        # ---------- AFTER schema ----------
        schema_after = _describe_parquet(conn, output_parquet_path)

        with open(schema_before_path, "w") as f:
            json.dump(schema_before, f, indent=2)
        with open(schema_after_path, "w") as f:
            json.dump(schema_after, f, indent=2)

        # ---------- Row count ----------
        row_count = conn.execute(
            f"SELECT COUNT(*) AS n FROM read_parquet('{output_parquet_path}')"
        ).fetchone()[0]
        qc["row_count"] = int(row_count)
        qc["n_columns"] = int(len(schema_after))

        # ---------- Null counts per column ----------
        cols = list(schema_after.keys())
        if cols:
            null_exprs = ", ".join(
                [f"SUM(CASE WHEN {_qident(c)} IS NULL THEN 1 ELSE 0 END) AS {_qident(c)}" for c in cols]
            )
            nulls = conn.execute(
                f"SELECT {null_exprs} FROM read_parquet('{output_parquet_path}')"
            ).fetchdf()
            null_counts = {c: int(nulls.iloc[0][c]) for c in cols}
        else:
            null_counts = {}

        qc["null_counts"] = null_counts

        # ---------- Sample ----------
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{output_parquet_path}') LIMIT {int(sample_n)}) "
            f"TO '{sample_path}' (FORMAT 'parquet')"
        )

        # ---------- Mapping resolution (condensed sources) ----------
        schema_before_lower = _schema_lower(schema_before)
        schema_after_lower = _schema_lower(schema_after)

        expected_out_cols = [list(c.keys())[0] for c in table_cfg.get("columns", [])]
        missing_out_cols = [c for c in expected_out_cols if c.lower() not in schema_after_lower]
        if missing_out_cols:
            qc["warnings"].append(f"Missing expected output columns: {missing_out_cols}")

        mapping_rows = []
        for col in table_cfg.get("columns", []):
            out_col = list(col.keys())[0]
            col_def = col[out_col] or {}
            kind = col_def.get("kind", "scalar")

            row = {
                "output_col": out_col,
                "kind": kind,
                "strategy": "",
                "chosen_source": "",
                "chosen_sources": "",
                "input_type": "",
                "output_type": schema_after.get(out_col, ""),
                "note": "",
            }

            if kind == "array":
                src_array = col_def.get("source_array") or []
                chosen_array = _select_first_existing(schema_before_lower, src_array)
                if chosen_array:
                    row["strategy"] = "source_array"
                    row["chosen_source"] = chosen_array
                    row["input_type"] = schema_before.get(chosen_array, "")
                else:
                    sets = col_def.get("source_component_sets") or []
                    if sets:
                        idx, chosen_set = _choose_component_set(schema_before_lower, sets)
                        row["strategy"] = f"component_set_{idx}" if idx is not None else "component_set_none"
                    else:
                        chosen_set = col_def.get("source_components") or []
                        row["strategy"] = "source_components"

                    row["chosen_sources"] = ";".join(chosen_set)
                    present = [c for c in chosen_set if c.lower() in schema_before_lower]
                    missing = [c for c in chosen_set if c.lower() not in schema_before_lower]
                    row["note"] = f"components_present={len(present)} components_missing={len(missing)}"
            else:
                candidates = col_def.get("source")
                chosen = _select_first_existing(schema_before_lower, candidates)
                if chosen:
                    row["strategy"] = "scalar_first_match"
                    row["chosen_source"] = chosen
                    row["input_type"] = schema_before.get(chosen, "")
                else:
                    row["strategy"] = "null_fallback"
                    row["note"] = "No source candidate found in input schema"

            mapping_rows.append(row)

        with open(mapping_csv_path, "w", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "output_col",
                    "kind",
                    "strategy",
                    "chosen_source",
                    "chosen_sources",
                    "input_type",
                    "output_type",
                    "note",
                ],
            )
            w.writeheader()
            w.writerows(mapping_rows)

        # ---------- Convenience warnings ----------
        if qc.get("row_count", 0) == 0:
            qc["warnings"].append("Output row_count is 0.")

        if "bene_id" in schema_after:
            bene_nulls = null_counts.get("bene_id")
            if bene_nulls is not None and qc["row_count"] > 0:
                frac = bene_nulls / qc["row_count"]
                if frac > 0.5:
                    qc["warnings"].append(f"bene_id null fraction is high: {frac:.2%}")

        run_metrics = _safe_load_json(run_metrics_path)
        if run_metrics:
            qc["harmonize_run"] = run_metrics

        # ---------- Crosswalk QA/QC ----------
        # We do not re-run joins. We assess join success using the *added output columns*.
        crosswalks = table_cfg.get("crosswalks") or []
        crosswalk_qc: dict[str, Any] = {}

        if crosswalks:
            # Create a view for repeated querying
            out_p = output_parquet_path.replace("'", "''")
            conn.execute(f"CREATE OR REPLACE VIEW out_tbl AS SELECT * FROM read_parquet('{out_p}')")

            for idx, cw in enumerate(crosswalks):
                if not isinstance(cw, dict):
                    continue

                cw_name = cw.get("name") or f"crosswalk_{idx+1}"
                cw_entry: dict[str, Any] = {
                    "status": "ok",
                    "warnings": [],
                    "format": cw.get("format", "parquet"),
                    "path": None,
                    "key_present_rows": None,
                    "added_columns": {},
                }

                try:
                    cw_path_tmpl = cw.get("path")
                    if not cw_path_tmpl:
                        cw_entry["status"] = "warn"
                        cw_entry["warnings"].append("Missing `path` in crosswalk definition.")
                        crosswalk_qc[cw_name] = cw_entry
                        continue

                    cw_path = _resolve_template(
                        cw_path_tmpl, basepath=basepath, crosswalks_path=crosswalks_path, year=year
                    )
                    cw_entry["path"] = cw_path

                    if not os.path.exists(cw_path):
                        cw_entry["status"] = "warn"
                        cw_entry["warnings"].append(f"Crosswalk file not found: {cw_path}")
                        crosswalk_qc[cw_name] = cw_entry
                        continue

                    join = cw.get("join") or {}
                    on_map = join.get("on") or {}
                    if not isinstance(on_map, dict) or not on_map:
                        cw_entry["status"] = "warn"
                        cw_entry["warnings"].append("Missing/invalid `join.on` mapping; cannot compute match rates.")
                        crosswalk_qc[cw_name] = cw_entry
                        continue

                    left_keys = list(on_map.keys())

                    # Check left keys exist in output schema
                    missing_left_keys = [k for k in left_keys if k not in schema_after]
                    if missing_left_keys:
                        cw_entry["status"] = "warn"
                        cw_entry["warnings"].append(f"Left join key(s) missing from output schema: {missing_left_keys}")

                    select_block = cw.get("select") or {}
                    add_map = (select_block.get("add") or {})
                    if not isinstance(add_map, dict) or not add_map:
                        cw_entry["warnings"].append("No `select.add` columns specified; nothing to evaluate.")
                        crosswalk_qc[cw_name] = cw_entry
                        continue

                    # Rows where all join keys are present
                    if left_keys:
                        key_present_cond = " AND ".join([f"{_qident(k)} IS NOT NULL" for k in left_keys])
                    else:
                        key_present_cond = "TRUE"

                    denom = conn.execute(f"SELECT COUNT(*) FROM out_tbl WHERE {key_present_cond}").fetchone()[0]
                    cw_entry["key_present_rows"] = int(denom)

                    total_rows = int(qc.get("row_count") or 0)

                    for out_col in add_map.keys():
                        if out_col not in schema_after:
                            cw_entry["warnings"].append(
                                f"Added column '{out_col}' not found in output schema (did join run?)."
                            )
                            continue

                        nulls_total = null_counts.get(out_col, None)
                        null_frac_overall = (nulls_total / total_rows) if (nulls_total is not None and total_rows > 0) else None

                        if denom > 0:
                            unmatched = conn.execute(
                                f"SELECT COUNT(*) FROM out_tbl WHERE {key_present_cond} AND {_qident(out_col)} IS NULL"
                            ).fetchone()[0]
                            unmatched_frac = unmatched / denom
                        else:
                            unmatched = None
                            unmatched_frac = None

                        cw_entry["added_columns"][out_col] = {
                            "null_fraction_overall": null_frac_overall,
                            "unmatched_count_among_key_present": int(unmatched) if unmatched is not None else None,
                            "unmatched_fraction_among_key_present": unmatched_frac,
                        }

                    # Heuristic warning if match is extremely poor for any added column
                    for out_col, stats in cw_entry["added_columns"].items():
                        uf = stats.get("unmatched_fraction_among_key_present")
                        if uf is not None and uf > 0.5:
                            cw_entry["status"] = "warn"
                            cw_entry["warnings"].append(
                                f"High unmatched fraction for '{out_col}' among key-present rows: {uf:.2%}"
                            )

                    crosswalk_qc[cw_name] = cw_entry

                except Exception as e:
                    cw_entry["status"] = "error"
                    cw_entry["warnings"].append(f"Crosswalk QC error: {e}")
                    crosswalk_qc[cw_name] = cw_entry

        qc["crosswalk_qc"] = crosswalk_qc

        conn.close()

    except Exception as e:
        qc["status"] = "error"
        qc["errors"].append(str(e))

    qc["duration_sec"] = time.perf_counter() - t0

    with open(qc_json_path, "w") as f:
        json.dump(qc, f, indent=2)

    return qc