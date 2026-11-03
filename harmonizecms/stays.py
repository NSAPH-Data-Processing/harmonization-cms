"""
Combine TAF inpatient claims into MAX-style hospital stays (the `rollup:` block of ip.yaml).

Design: docs/max-taf/ip/rollup_plan_2026-10-22.md (IP-R18: MAX sets which claims go in, the CMS
brief 5011 Table 1 rule sets how they link); decisions in docs/max-taf/ip/decisions.md.

Built in steps (IP-R26). This file so far has step 1, the claim preparation that runs before
linking (plan §1 A, steps 1-4), in the order of IP-R19 / review R4:

1. `filters`: claim types MAX doesn't have (service tracking 4/D/X, IP-R08; Illinois Y, IP-R11).
   U/V/W/Y are kept and linked (IP-R27, IP-R38), and counted after step 2.
2. Repeated claim_ids: one copy of identical repeats in a year file; stop on copies that differ
   (IP-R07) or on a claim_id in two year files (IP-R06).
3. `claim_families`: each Illinois family of incremental adjustment records netted into one claim
   (IP-R11, IP-R43; counts from IP-R42, IP-R43).

Input must hold rollup rows only (`rollup.rows`, TAF); other rows stop the run.
4. `final_action_filters`: MAX's $0 FFS rule (IP-R12), on claims before linking (IP-R20).

Input: a table of harmonized TAF claim rows (the ip.yaml columns) plus `_file_year`, the year
file each row came from. Output: table `stay_claims`, one row per final-action claim.
"""
from __future__ import annotations

import logging

import duckdb

LOGGER = logging.getLogger(__name__)

# Amount columns. In an Illinois family they are summed (adjustments are marginal), except
# covered days, which take the latest value like other fields (findings/illinois.md).
SUMMED = ["mdcd_cvrd_days", "mdcd_pymt_amt", "tp_pymt_amt", "mdcr_coinsur_pymt_amt", "mdcr_ded_pymt_amt"]
# (codes, flags) pairs: each code stays paired with its version / coding-system flag
CODE_LISTS = [("diagnoses", "diagnosis_version_cds"), ("procedures", "procedure_coding_system")]
# Fields that belong with another field come from the same family record (IP-R43 a):
# {column: main column}. A code-list main column means the first record with any code (like the
# code lists themselves); otherwise the latest record whose main column is non-NULL.
PAIRED = {"primary_diagnosis": "diagnoses", "srvc_end_dt_cd": "srvc_end_dt",
          "drg_rel_group_ind": "drg_rel_group"}
# Claim types kept and linked that MAX would not have as Medicaid claims (IP-R27, IP-R38)
OTHER_TYPES = ("U", "V", "W", "Y")


def _count(conn: duckdb.DuckDBPyConnection, table_and_where: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_and_where}").fetchone()[0]


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    """{column name: type} of a table."""
    return {r[0]: r[1] for r in conn.execute(f"DESCRIBE {table}").fetchall()}


def _nested_counts(conn: duckdb.DuckDBPyConnection, sql: str) -> dict:
    """{a: {b: {c: n}}} from a query returning (a, b, c, n) rows; NULL keys become 'NULL'."""
    out: dict = {}
    for a, b, c, n in conn.execute(sql).fetchall():
        out.setdefault(str(a), {}).setdefault(str(b), {})["NULL" if c is None else str(c)] = n
    return out


def prepare_claims(
    conn: duckdb.DuckDBPyConnection,
    rollup_cfg: dict,
    input_table: str = "stay_input",
) -> dict:
    """
    Steps 1-4 above, from `input_table` into table `stay_claims`. Returns run.json diagnostics.
    Raises ValueError on input rows outside `rollup_cfg['rows']` (e.g. MAX rows), and on repeated
    claim_ids that disagree or that appear in two year files.
    """
    diag: dict = {}
    # The steps below are TAF rules (claim types, Illinois families, claim_id repeats): MAX rows
    # must not reach them
    n_outside = _count(conn, f"{input_table} WHERE NOT COALESCE(({rollup_cfg['rows']}), FALSE)")
    if n_outside:
        raise ValueError(f"{n_outside} input rows fail rollup rows `{rollup_cfg['rows']}`; "
                         f"prepare_claims takes rollup rows only")
    n_input = _count(conn, input_table)
    diag["claims_read"] = n_input
    file_years = [r[0] for r in conn.execute(
        f"SELECT DISTINCT _file_year FROM {input_table} ORDER BY 1").fetchall()]

    # 1. Claim-type filters, each counted on its own (a row failing two is counted under both)
    filters = rollup_cfg.get("filters") or []
    diag["claims_dropped_by_filter"] = {
        f: _count(conn, f"{input_table} WHERE NOT COALESCE(({f}), FALSE)") for f in filters}
    where = " AND ".join(f"COALESCE(({f}), FALSE)" for f in filters) or "TRUE"
    conn.execute(f"CREATE OR REPLACE TABLE stay_filtered AS SELECT * FROM {input_table} WHERE {where}")
    diag["claims_dropped_by_filters"] = n_input - _count(conn, "stay_filtered")
    LOGGER.info(f"Claim-type filters dropped {diag['claims_dropped_by_filters']} of {n_input} claims")

    # 2. One row per claim_id. Copies within a year file must agree on everything but the file
    # they came from (IP-R07); a claim_id in two year files isn't expected in TAF (IP-R06).
    # Only claim_ids repeated in a year file are compared (most occur once).
    cols = _columns(conn, "stay_filtered")
    compare = [c for c in cols if c not in ("claim_id", "raw_filename", "_file_year")]
    conflicts = conn.execute(f"""
        WITH repeated AS (
            SELECT _file_year, claim_id FROM stay_filtered
            WHERE claim_id IS NOT NULL
            GROUP BY _file_year, claim_id HAVING COUNT(*) > 1
        )
        SELECT _file_year, claim_id, COUNT(*) FROM stay_filtered
        SEMI JOIN repeated USING (_file_year, claim_id)
        GROUP BY _file_year, claim_id
        HAVING COUNT(DISTINCT ({", ".join(compare)})) > 1
        ORDER BY ALL LIMIT 10
    """).fetchall()
    if conflicts:
        raise ValueError(f"Repeated claim_ids whose copies disagree (year file, claim_id, copies), "
                         f"first 10: {conflicts} (IP-R07)")
    cross_year = conn.execute("""
        SELECT claim_id, list(DISTINCT _file_year ORDER BY _file_year) FROM stay_filtered
        WHERE claim_id IS NOT NULL
        GROUP BY claim_id HAVING COUNT(DISTINCT _file_year) > 1
        ORDER BY claim_id LIMIT 10
    """).fetchall()
    if cross_year:
        raise ValueError(f"claim_ids in more than one year file (claim_id, year files), first 10: "
                         f"{cross_year}. Not expected in TAF (IP-R06); check the extract.")
    conn.execute("""
        CREATE OR REPLACE TABLE stay_claims AS
        SELECT * FROM stay_filtered WHERE claim_id IS NULL
        UNION ALL BY NAME
        (SELECT DISTINCT ON (_file_year, claim_id) * FROM stay_filtered
         WHERE claim_id IS NOT NULL
         ORDER BY _file_year, claim_id, raw_filename)
    """)
    diag["claims_duplicate_copies_removed"] = _count(conn, "stay_filtered") - _count(conn, "stay_claims")
    diag["claims_null_claim_id"] = _count(conn, "stay_claims WHERE claim_id IS NULL")
    LOGGER.info(f"Removed {diag['claims_duplicate_copies_removed']} identical repeat copies of claim_ids")
    # Kept U/V/W/Y and NULL claim types, counted once per claim (after repeats, before netting)
    other = ", ".join(f"'{t}'" for t in OTHER_TYPES)
    diag["other_type_claims_kept"] = _nested_counts(conn, f"""
        SELECT state, _file_year, claim_type_cd, COUNT(*) FROM stay_claims
        WHERE claim_type_cd IN ({other}) GROUP BY ALL ORDER BY ALL
    """)
    diag["claims_null_claim_type_kept"] = _count(conn, "stay_claims WHERE claim_type_cd IS NULL")
    LOGGER.info(f"{diag['claims_null_claim_type_kept']} claims with NULL claim type kept")

    # 3. Illinois claim families
    family_cfg = rollup_cfg.get("claim_families")
    if family_cfg:
        _net_claim_families(conn, family_cfg, diag, file_years)
    for c in rollup_cfg.get("family_columns") or []:  # used only by the netting
        conn.execute(f"ALTER TABLE stay_claims DROP COLUMN {c}")

    # 4. Final-action filters ($0 FFS), after netting so a family netting to $0 goes too
    final_filters = rollup_cfg.get("final_action_filters") or []
    before = _count(conn, "stay_claims")
    if final_filters:
        where = " AND ".join(f"COALESCE(({f}), FALSE)" for f in final_filters)
        conn.execute(f"DELETE FROM stay_claims WHERE NOT ({where})")
    diag["claims_dropped_by_final_action_filters"] = before - _count(conn, "stay_claims")
    diag["claims_prepared"] = _count(conn, "stay_claims")
    LOGGER.info(f"Final-action filters dropped {diag['claims_dropped_by_final_action_filters']} "
                f"claims; {diag['claims_prepared']} claims go on to linking")
    return diag


def _net_claim_families(
    conn: duckdb.DuckDBPyConnection,
    cfg: dict,
    diag: dict,
    file_years: list[int],
) -> None:
    """
    Replace each claim family in `stay_claims` by one netted claim (IP-R11).

    Illinois submits adjustments as marginal debit/credit records instead of void + replacement,
    and each adjustment carries only the fields that changed; TAF keeps every record ("TAF
    Technical Guidance: How to Use Illinois Claims Data", CMS 2020). Per family (records ordered
    original first, by `original`, then by `order`): amounts in SUMMED are added up and rounded to cents (covered days excepted;
    IP-R43 b), code lists come from the first record that has any code, claim_id is the first
    record's non-NULL one, adjustment_cd is '1' for 2+ records, the year file and `year` are the
    latest, PAIRED fields come from their main column's record (IP-R43 a), and every other column
    is its latest non-NULL value.

    Rows without every key column are left as ordinary claims and counted by the missing column
    (IP-R42: the documented key stays; a row with no provider ID can still link by NPI, IP-R39).
    """
    rows, fkey, order = cfg.get("rows"), list(cfg.get("key") or []), cfg.get("order")
    original = cfg.get("original")
    if not (rows and fkey and order and original):
        raise ValueError("rollup.claim_families needs rows, key, order and original.")
    fkey_sql = ", ".join(fkey)
    selected = f"COALESCE(({rows}), FALSE)"
    has_key = " AND ".join(f"{c} IS NOT NULL" for c in fkey)
    # One definition of the original record: it sorts first and is what the counts check
    is_adjustment = f"NOT COALESCE(({original}), FALSE)"

    diag["family_rows"] = _count(conn, f"stay_claims WHERE {selected}")
    diag["family_rows_not_netted_missing_key"] = _count(
        conn, f"stay_claims WHERE {selected} AND NOT ({has_key})")
    diag["family_rows_missing_key_by_column"] = {
        c: _count(conn, f"stay_claims WHERE {selected} AND {c} IS NULL") for c in fkey}
    diag["family_rows_missing_provider_id_with_npi"] = _count(
        conn, f"stay_claims WHERE {selected} AND billing_provider_id IS NULL AND billing_npi IS NOT NULL")
    if diag["family_rows"] and diag["family_rows_not_netted_missing_key"] == diag["family_rows"]:
        LOGGER.warning(f"None of the {diag['family_rows']} claim-family rows have all of {fkey}: "
                       f"families can't be netted (is the column in the extract?)")

    # Payment reported in another column when the main one is 0 / NULL (never both)
    diag["family_amount_fallbacks_used"] = {}
    for col, fallback in (cfg.get("amount_fallbacks") or {}).items():
        cond = f"{selected} AND COALESCE({col}, 0) = 0 AND COALESCE({fallback}, 0) <> 0"
        diag["family_amount_fallbacks_used"][col] = _count(conn, f"stay_claims WHERE {cond}")
        conn.execute(f"UPDATE stay_claims SET {col} = {fallback} WHERE {cond}")

    conn.execute(f"""
        CREATE OR REPLACE TABLE family_records AS
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY {fkey_sql} ORDER BY {is_adjustment}, {order}) AS fam_order
        FROM stay_claims
        WHERE {selected} AND {has_key}
    """)
    types = _columns(conn, "stay_claims")
    # {column: code list whose first non-empty record it comes from}
    code_cols = ({flags: codes for codes, flags in CODE_LISTS} | {codes: codes for codes, _ in CODE_LISTS}
                 | {c: m for c, m in PAIRED.items() if m in dict(CODE_LISTS)})
    # {column: main column whose latest non-NULL record it comes from}
    latest_of = {c: m for c, m in PAIRED.items() if m not in dict(CODE_LISTS)}
    summed = [c for c in SUMMED if c != "mdcd_cvrd_days"]
    exprs = []
    for c, t in types.items():
        if c in fkey:
            continue
        if c in summed:
            # Rounded to cents: float residue (0.1 + 0.2 - 0.3) would escape the $0 rule (IP-R43 b)
            exprs.append(f"CAST(ROUND(SUM({c}), 2) AS {t}) AS {c}")
        elif c == "claim_id":
            exprs.append("arg_min(claim_id, fam_order) AS claim_id")
        elif c == "adjustment_cd":
            exprs.append("CASE WHEN COUNT(*) > 1 THEN '1' ELSE ANY_VALUE(adjustment_cd) END AS adjustment_cd")
        elif c in ("_file_year", "year"):  # agree with each other and with raw_filename
            exprs.append(f"MAX({c}) AS {c}")
        elif c == "raw_filename":
            exprs.append("arg_max_null(raw_filename, (_file_year, fam_order)) AS raw_filename")
        elif c in code_cols:
            exprs.append(f"arg_min_null({c}, fam_order) FILTER (WHERE list_count({code_cols[c]}) > 0) AS {c}")
        elif c in latest_of:
            exprs.append(f"arg_max_null({c}, fam_order) FILTER (WHERE {latest_of[c]} IS NOT NULL) AS {c}")
        else:
            exprs.append(f"arg_max({c}, fam_order) FILTER (WHERE {c} IS NOT NULL) AS {c}")
    conn.execute(f"""
        CREATE OR REPLACE TABLE family_netted AS
        SELECT {fkey_sql}, {", ".join(exprs)}, COUNT(*) AS n_records,
            COUNT(*) FILTER (WHERE NOT {is_adjustment}) AS n_originals
        FROM family_records
        GROUP BY {fkey_sql}
    """)
    before = _count(conn, "stay_claims")
    conn.execute(f"""
        CREATE OR REPLACE TABLE stay_claims AS
        SELECT * FROM stay_claims WHERE NOT ({selected} AND {has_key})
        UNION ALL BY NAME
        SELECT * EXCLUDE (n_records, n_originals) FROM family_netted
    """)
    diag["family_records_merged"] = before - _count(conn, "stay_claims")
    diag["families_multi_record"], diag["families_negative_net_paid"] = conn.execute("""
        SELECT COUNT(*) FILTER (WHERE n_records > 1), COUNT(*) FILTER (WHERE mdcd_pymt_amt < 0)
        FROM family_netted
    """).fetchone()
    # A family's records can sit in a year file outside this run (IP-R43 c): families with no
    # original record (outside the run or missing), and families with a record in the last year
    # read (the netted _file_year is the family's latest; later adjustments may be in the next file)
    diag["families_without_original"] = _count(conn, "family_netted WHERE n_originals = 0")
    if file_years:
        diag["families_latest_record_in_last_year"] = _count(
            conn, f"family_netted WHERE _file_year = {max(file_years)}")
    # ADJSTMT_IND 0 can mark more than one record of a family (IL guide,
    # TAF-TechGuide-IL-Claims.txt:212-215): those originals sort by `order` among themselves
    diag["families_multiple_originals"] = _count(conn, "family_netted WHERE n_originals > 1")
    LOGGER.info(f"Claim families: {diag['family_rows']} rows, {diag['families_multi_record']} families "
                f"of 2+ records netted ({diag['family_records_merged']} rows merged), "
                f"{diag['family_rows_not_netted_missing_key']} rows without a full family key left as is")
    if diag["families_negative_net_paid"]:
        LOGGER.warning(f"{diag['families_negative_net_paid']} netted claim families have a negative "
                       f"Medicaid payment")
