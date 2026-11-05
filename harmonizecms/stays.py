"""
Combine TAF inpatient claims into MAX-style hospital stays (the `rollup:` block of ip.yaml).

Design: docs/max-taf/ip/rollup_plan_2026-10-22.md (IP-R18: MAX sets which claims go in, the CMS
brief 5011 Table 1 rule sets how they link); decisions in docs/max-taf/ip/decisions.md.

Built in steps (IP-R26). This file so far has steps 1-4: the claim preparation that runs before
linking (1-2), the linking of claims into stays (3), and one row per stay (4).

Step 1, `prepare_claims` (plan §1 A, steps 1-4), in the order of IP-R19 / review R4:
1. `filters`: claim types MAX doesn't have (service tracking 4/D/X, IP-R08; Illinois Y, IP-R11).
   U/V/W/Y are kept and linked (IP-R27, IP-R38), and counted after step 2.
2. Repeated claim_ids: one copy of identical repeats in a year file; stop on copies that differ
   (IP-R07) or on a claim_id in two year files (IP-R06). Rows with NULL claim_id: one copy of rows
   identical but for raw_filename in a year file (IP-R46 b); copies in two year files are kept and
   counted (IP-R47 c).
3. `claim_families`: each Illinois family of incremental adjustment records netted into one claim
   (IP-R11, IP-R43, IP-R45 b; counts from IP-R42, IP-R43).
4. `final_action_filters`: MAX's $0 FFS rule (IP-R12), on claims before linking (IP-R20).

Step 2, `prepare_links` (plan §1, steps 5-8), adds columns to `stay_claims`:
5. Status kept as reported (99 is NULL from the cast, IP-R44 a); values outside MAX's list are
   counted (IP-R31). TAF 99s are counted by an enclave check only: a 99 on an Illinois adjustment
   is NULL before netting, so an earlier record's status wins and the 99 leaves no trace (IP-R45 c).
6. Link dates: `link_adm` = admission date, else service begin date (IP-R25); `link_dis` = end
   date only when it is a discharge or last line end date, code 1 or 4 (IP-R25, IP-R37).
7. Link key `LINK_KEY`: submitting state (IP-R44 b; STATE_CD's main code in a year file without
   it, IP-R45 a), MSIS ID, and billing provider ID, else 'NPI:' + NPI (IP-R18, IP-R39).
8. `nonlink_reasons`: why a claim can't link (empty = linkable), `&` claims included (IP-R47 a).
   Such claims are kept as single-claim stays (IP-R18, IP-R41).

Step 3, `link_claims` (plan §1 A step 9 and B; IP-R46, IP-R47), adds `claim_rid` and `stay_id`:
9. `claim_rid` = claim order 1..n: LINK_KEY, link_adm, srvc_bgn_dt, link_dis, CLASS_RANK, claim_id,
   then an md5 of the other columns serialized by `_row_text` (IP-R48 b). Step 4 orders by it.
10. Linkable claims with equal LINK_KEY link on any CMS condition (B5011:216-233; IP-R18, IP-R24):
   same admission or discharge date, inclusive containment, strict overlap, or the admission on
   or the day after the other's discharge when that preceding claim's status is 30 or NULL.
   Stays are the connected components of the links (IP-R23); `stay_id` = the stay's smallest
   `claim_rid`. A non-linkable claim is its own stay. Every link lies inside a touch-island (claims
   of a key whose dates touch within a day); an island with no blocked pair is one stay, the others
   go to `_components`. An island over `island_cap` claims stops the run (IP-R47 a).

Step 4, `aggregate_stays` (plan §1 C 10-13 and §4; IP-R28, IP-R29, IP-R35, IP-R38, IP-R40, IP-R49
to IP-R52): `stay_claims` -> table `built_stays`, one row per stay (columns STAY_COLUMNS), with the
per-state first TAF year from cutover.yaml (`load_first_taf_year`). Its row order is not defined
(IP-R51 b).

Input: a table of harmonized TAF claim rows (the ip.yaml columns) plus `_file_year`, the year
file each row came from. It must hold rollup rows only (`rollup.rows`, TAF); other rows stop the
run. Output: table `stay_claims`, one row per final-action claim.

Pipeline wiring (IP-R54, IP-R55; harmonizer.py): `build_table_stays` reads the TAF files of all the
run's years (`rollup.path_pattern`), runs steps 1-4 once in a file-backed duckdb (step-1 tables
dropped by `drop_prepare_tables`) and writes `{output}/_build/{table}_stays.parquet` plus its
run.json. Each yearly run of the table then takes the MAX rows of its year through
`max_rows_to_stays` (STAY_COLUMNS with `stay_column_types`; MAX crossover and cutover flags,
IP-R42 12, IP-R54 b), adds the built stays whose `year` is that year, and counts admissions-key
sharing by MAX/TAF mix (`admissions_key_sharing`, IP-R54 c).
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import duckdb
import yaml

LOGGER = logging.getLogger(__name__)

# Amount columns. In an Illinois family they are summed (adjustments are marginal), except
# covered days, which take the latest value like other fields (findings/illinois.md).
SUMMED = ["mdcd_cvrd_days", "mdcd_pymt_amt", "tp_pymt_amt", "mdcr_coinsur_pymt_amt", "mdcr_ded_pymt_amt"]
# (codes, flags) pairs: each code stays paired with its version / coding-system flag
CODE_LISTS = [("diagnoses", "diagnosis_version_cds"), ("procedures", "procedure_coding_system")]
# Fields that belong with another field come from the same family record (IP-R43 a):
# {column: main column}. A code-list main column means the first record with any code (like the
# code lists themselves); otherwise the latest record whose main column is non-NULL.
PAIRED = {"primary_diagnosis": "diagnoses", "drg_rel_group_ind": "drg_rel_group"}
# Illinois end date and its code come from one record, by tier: latest code 1, else latest code 4,
# else latest record with an end date (IP-R44 c, IP-R45 b)
END_DATE = ("srvc_end_dt", "srvc_end_dt_cd")
END_DATE_TIER = "CASE srvc_end_dt_cd WHEN '1' THEN 2 WHEN '4' THEN 1 ELSE 0 END"
# Claim types kept and linked that MAX would not have as Medicaid claims (IP-R27, IP-R38)
OTHER_TYPES = ("U", "V", "W", "Y")
# MAX 2015 patient status codes (max_ip_dictionary.txt:1545-1572); 99 is NULL already (IP-R44 a)
MAX_STATUS = ("01", "02", "03", "04", "05", "06", "07", "08", "09", "20", "30", "40", "41", "42",
              "43", "50", "51", "61", "62", "63", "65", "66", "71", "72")
# Postal code -> main SUBMTG_STATE_CD (taf_claims_codebook.txt:7712-7766, 7786-7856); 93/94/97
# are second submitters, never values here
STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08", "CT": "09", "DE": "10",
    "DC": "11", "FL": "12", "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
    "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34", "NM": "35",
    "NY": "36", "NC": "37", "ND": "38", "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56", "GU": "66", "PR": "72", "VI": "78",
}
# WY CHIP, MT TPA, PA CHIP (taf_claims_codebook.txt:7836-7860)
SECOND_SUBMITTERS = ("93", "94", "97")
# Stay link key (B5011:216-218; IP-R18, IP-R39, IP-R44 b, IP-R45 a): step 3 joins on these with `=`
LINK_KEY = ["link_state", "msis_id", "link_provider"]
# Why a claim can't link, in the order listed in `nonlink_reasons` (plan §1 step 8, IP-R41)
NONLINK_REASONS = {
    "msis_id_null": "msis_id IS NULL",
    # '&' claims (IP-R47 a; IP-R34): dropped at materialization, so not linked. A claim with NULL
    # MSIS ID also has prsn_clm_ind = 0 (IP-R34), so it is counted under both reasons
    "amp_msis_id": "prsn_clm_ind = 0 AND claim_type_cd IS NOT NULL",
    "state_null": "state IS NULL",
    "link_state_null": "link_state IS NULL",
    "link_adm_null": "link_adm IS NULL",
    "link_dis_null": "link_dis IS NULL",
    "link_adm_after_link_dis": "link_adm > link_dis",  # B5011:184 ("inconsistent data"), our reading
    "no_provider_id_or_npi": "link_provider IS NULL",  # both missing (IXF fn 45)
}
# Claim class in the claim order (plan §1 A step 9): FFS, encounter, other, supplemental. A shared
# SQL expression, not a stored column
CLASS_RANK = ("CASE WHEN claim_type_cd IN ('1', 'A') THEN 1 WHEN claim_type_cd IN ('3', 'C') THEN 2 "
              "WHEN claim_type_cd IN ('5', 'E') THEN 4 ELSE 3 END")
# Link conditions as named in run.json (B5011:216-233); `overlap` is counted as partial overlap
# (strict overlap without containment), which links the same pairs (IP-R24, IP-R46)
LINK_CONDITIONS = ("same_adm", "same_dis", "containment", "overlap", "contiguous")
# Most claims one touch-island may hold: link cost grows with its square (IP-R47 a)
ISLAND_CAP = 20_000
# SQL label-propagation rounds before the Python union-find in `_components`
COMPONENT_SQL_ROUNDS = 8
# E17 (plan §3b, IP-R21, IP-R46 a, IP-R47 d): a claim is "long" when its service span is over 30
# days; the threshold is 1% of multi-claim stays in a state with at least 100 of them, or 0.2% of
# all stays
E17_LONG_DAYS = 30
E17_STATE_SHARE = 0.01
E17_STATE_MIN_MULTI = 100
E17_ALL_SHARE = 0.002

# Step 4 (plan §4). No MIN/MAX/arg_min/arg_max whose value or key is a STRUCT or LIST here: order
# with integer ranks or list_sort of structs (PR4-1, PR4-4; MIN(struct) allocated outside
# memory_limit on duckdb 1.0.0).
# Helper columns used here and not output (ip.yaml helper block, after step 1)
HELPER_COLUMNS = ("srvc_end_dt_cd", "submtg_state_cd", "crossover_clm_ind", "prsn_clm_ind")
# Columns added by steps 1-3, not output
PIPELINE_COLUMNS = ("_file_year", "link_adm", "link_dis", "link_state", "link_provider",
                    "nonlink_reasons", "claim_rid", "stay_id")
# "Last claim" order (IP-R28, plan §4): latest link_dis, class, latest begin, then claim_rid
END_ORDER = f"link_dis DESC NULLS LAST, {CLASS_RANK}, srvc_bgn_dt DESC NULLS LAST, claim_rid DESC"
# '&' claim (IP-R35 e, IP-R42 12); same as the amp_msis_id non-link reason plus the literal 4/D/X rule.
# NULL when prsn_clm_ind is NULL, so a stay whose indicators are all NULL gets NULL (IP-R42 12,
# IP-R53 c; without the guard a NULL claim type made it FALSE)
AMP_CLAIM = (f"CASE WHEN prsn_clm_ind IS NOT NULL THEN {NONLINK_REASONS['amp_msis_id']} "
             f"AND claim_type_cd NOT IN ('4', 'D', 'X') END")
COVERED_DAYS_CAP = 365  # MAX 2006-2015 (IP-R29)
# Code-list order key claim_rid * CODE_POS_FACTOR + position (an integer, not a struct: PR4-1);
# lists must be shorter than this (checked before step 4 runs)
CODE_POS_FACTOR = 1000
# IP-R35 a admissions key, for the key-sharing count
ADMISSIONS_KEY = ["year", "state", "msis_id", "bene_id", "billing_provider_id", "billing_npi",
                  "admission_date", "srvc_bgn_dt", "srvc_end_dt"]
# Output columns: ip.yaml order, claim_id replaced in place by claim_ids + n_claims_combined
# (archive A:576-578), helpers dropped, the four stay columns last (IP-R35 e, IP-R38)
STAY_COLUMNS = [
    "bene_id", "msis_id", "claim_ids", "n_claims_combined", "year", "raw_filename", "source_type",
    "state", "admission_date", "srvc_bgn_dt", "srvc_end_dt", "patient_status_cd", "claim_type_cd",
    "adjustment_cd", "primary_diagnosis", "diagnoses", "diagnosis_version_cds", "procedures",
    "procedure_coding_system", "mdcd_cvrd_days", "mdcd_pymt_amt", "tp_pymt_amt",
    "mdcr_coinsur_pymt_amt", "mdcr_ded_pymt_amt", "drg_rel_group", "drg_rel_group_ind",
    "billing_provider_id", "billing_npi",
    "n_other_type_claims", "crossover_ind", "cutover_ind", "amp_msis_id_ind"]
NEW_STAY_COLUMNS = ("claim_ids", "n_claims_combined", "n_other_type_claims", "crossover_ind",
                    "cutover_ind", "amp_msis_id_ind")
STAY_RULE_INPUTS = [c for c in STAY_COLUMNS if c not in NEW_STAY_COLUMNS]  # = ip.yaml output columns
# Step 4 temp tables
STAY_TEMP_TABLES = ("_stay_rank", "_stay_agg", "_stay_codes_diagnoses", "_stay_codes_procedures")

# Wiring (IP-R54 d). Types of the stay columns that are not ip.yaml columns; the others take their
# ip.yaml `type`
NEW_STAY_TYPES = {"claim_ids": "VARCHAR[]", "n_claims_combined": "BIGINT", "n_other_type_claims": "BIGINT",
                  "crossover_ind": "BOOLEAN", "cutover_ind": "BOOLEAN", "amp_msis_id_ind": "BOOLEAN"}
# MAX row crossover (IP-R35 e, IP-R42 12; MAX dictionary EL_MDCR_XOVR_CLM_BSD_CD): Medicaid paid
# Medicare coinsurance or deductible; NULL amounts count as $0, so never NULL
MAX_CROSSOVER = "COALESCE(mdcr_coinsur_pymt_amt > 0 OR mdcr_ded_pymt_amt > 0, FALSE)"
# Tables prepare_claims leaves besides stay_claims (the family tables only with Illinois rows)
PREPARE_TABLES = ("stay_filtered", "family_records", "family_netted")


def _count(conn: duckdb.DuckDBPyConnection, table_and_where: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table_and_where}").fetchone()[0]


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> dict[str, str]:
    """{column name: type} of a table."""
    return {r[0]: r[1] for r in conn.execute(f"DESCRIBE {table}").fetchall()}


def _nested_counts(conn: duckdb.DuckDBPyConnection, sql: str) -> dict:
    """{a: {b: ... n}} from a query returning (a, b, ..., n) rows; NULL keys become 'NULL'."""
    out: dict = {}
    for *keys, n in conn.execute(sql).fetchall():
        keys = ["NULL" if k is None else str(k) for k in keys]
        d = out
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = n
    return out


def _sql_list(values) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _row_text(cols: dict[str, str]) -> str:
    """
    SQL text of a row for an md5 tie-break, the same on duckdb 1.0.0 and 1.5.2 (IP-R48 b): each
    column as length:value (NULL as '~'), joined by chr(31). A list column is its elements
    serialized the same way, joined by chr(30), then length-prefixed, so an empty list is '0:'
    (CAST of a list to VARCHAR differs between versions; scalar casts don't). Raises ValueError
    for any other type (STRUCT, MAP, UNION, nested or fixed-size list).
    """
    def text(v: str) -> str:
        return f"COALESCE(length({v})::VARCHAR || ':' || {v}, '~')"
    parts = []
    for c, t in cols.items():
        base = t[:-2] if t.endswith("[]") else t
        if base.endswith("]") or base.upper().startswith(("STRUCT", "MAP", "UNION")):
            raise ValueError(f"_row_text: column {c} has type {t}; only scalars and lists of scalars "
                             "are serialized")
        if t.endswith("[]"):
            elems = f"list_transform({c}, e -> {text('CAST(e AS VARCHAR)')})"
            # array_to_string of an empty list is NULL on 1.0.0 and '' on 1.5.2.
            joined = f"CASE WHEN {c} IS NOT NULL THEN COALESCE(array_to_string({elems}, chr(30)), '') END"
            parts.append(text(joined))
        else:
            parts.append(text(f"CAST({c} AS VARCHAR)"))
    return " || chr(31) || ".join(parts)


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
    diag["file_years"] = file_years  # the run's year files, for aggregate_stays (IP-R49 a)
    # Year files with no submitting state at all (column absent from the extract; IP-R45 a), for
    # prepare_links. Found before netting: a netted Illinois claim takes the latest year file but
    # may carry an earlier record's submitter code
    diag["link_state_fallback_years_input"] = [r[0] for r in conn.execute(f"""
        SELECT _file_year FROM {input_table} GROUP BY _file_year HAVING COUNT(submtg_state_cd) = 0
        ORDER BY 1
    """).fetchall()]

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
    # Rows with NULL claim_id: one copy of rows identical but for raw_filename in a year file, as
    # for claim_ids (IP-R46 b); otherwise they would link by their equal dates and be summed twice
    # Each group keeps its copy from the first-sorting file. The ROW_NUMBER picks rowids only and the
    # rows are fetched by a semi-join, so the window sorts keys, not whole rows: `DISTINCT ON ... *`
    # ran out of memory (IP-R55 a). Same rows: copies tied on raw_filename are identical (the
    # IP-R07 check above; for NULL claim_id, by the partition)
    cmp_sql = ", ".join(compare)
    conn.execute(f"""
        CREATE OR REPLACE TABLE stay_claims AS
        (SELECT * FROM stay_filtered WHERE rowid IN (SELECT rid FROM (
            SELECT rowid AS rid,
                   ROW_NUMBER() OVER (PARTITION BY {cmp_sql}, _file_year ORDER BY raw_filename) AS rn
            FROM stay_filtered WHERE claim_id IS NULL) WHERE rn = 1))
        UNION ALL BY NAME
        (SELECT * FROM stay_filtered WHERE rowid IN (SELECT rid FROM (
            SELECT rowid AS rid,
                   ROW_NUMBER() OVER (PARTITION BY _file_year, claim_id ORDER BY raw_filename) AS rn
            FROM stay_filtered WHERE claim_id IS NOT NULL) WHERE rn = 1))
    """)
    diag["claims_duplicate_copies_removed"] = (_count(conn, "stay_filtered WHERE claim_id IS NOT NULL")
                                               - _count(conn, "stay_claims WHERE claim_id IS NOT NULL"))
    diag["claims_null_claim_id_copies_removed"] = (_count(conn, "stay_filtered WHERE claim_id IS NULL")
                                                   - _count(conn, "stay_claims WHERE claim_id IS NULL"))
    # Rows (not groups) with NULL claim_id identical but for their year file: kept, counted only
    # (IP-R47 c)
    no_year = ", ".join(c for c in compare if c != "year")
    diag["claims_null_claim_id_cross_year_copies"] = conn.execute(f"""
        SELECT COALESCE(SUM(n), 0)::BIGINT FROM (
            SELECT COUNT(*) AS n FROM stay_claims WHERE claim_id IS NULL
            GROUP BY {no_year} HAVING COUNT(DISTINCT _file_year) > 1)
    """).fetchone()[0]
    # Rows with NULL claim_id in a year file that differ only in amounts (SUMMED): kept, counted
    not_summed = ", ".join(c for c in compare if c not in SUMMED)
    diag["claims_null_claim_id_near_copies"] = conn.execute(f"""
        SELECT COALESCE(SUM(n), 0)::BIGINT FROM (
            SELECT COUNT(*) AS n FROM stay_claims WHERE claim_id IS NULL
            GROUP BY {not_summed}, _file_year HAVING COUNT(*) > 1)
    """).fetchone()[0]
    diag["claims_null_claim_id"] = _count(conn, "stay_claims WHERE claim_id IS NULL")
    LOGGER.info(f"Removed {diag['claims_duplicate_copies_removed']} identical repeat copies of claim_ids "
                f"and {diag['claims_null_claim_id_copies_removed']} identical copies of rows with NULL "
                f"claim_id")
    # Kept U/V/W/Y and NULL claim types, counted once per claim (after repeats, before netting)
    other = _sql_list(OTHER_TYPES)
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
    latest, PAIRED fields come from their main column's record (IP-R43 a), END_DATE columns from
    one record by code tier (IP-R44 c, IP-R45 b), and every other column is its latest non-NULL
    value.

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

    types = _columns(conn, "stay_claims")
    # Records tied on `order` (NULL claim_id, same adjudication date) are ordered by an md5 of the
    # row (`_row_text`), so the netted claim doesn't depend on input order (IP-R47 e, IP-R48 b).
    # `_file_rank` and `_end_rank` rank (year file, family order) and (END_DATE_TIER, family order) as integers
    # (duckdb 1.0.0's arg_max_null takes no struct)
    conn.execute(f"""
        CREATE OR REPLACE TABLE family_records AS
        WITH r AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {fkey_sql} ORDER BY {is_adjustment}, {order},
                md5({_row_text(types)})) AS fam_order
            FROM stay_claims
            WHERE {selected} AND {has_key})
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY {fkey_sql} ORDER BY _file_year, fam_order) AS _file_rank,
            ROW_NUMBER() OVER (PARTITION BY {fkey_sql} ORDER BY {END_DATE_TIER}, fam_order) AS _end_rank
        FROM r
    """)
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
            exprs.append("arg_max_null(raw_filename, _file_rank) AS raw_filename")
        elif c in code_cols:
            exprs.append(f"arg_min_null({c}, fam_order) FILTER (WHERE list_count({code_cols[c]}) > 0) AS {c}")
        elif c in latest_of:
            exprs.append(f"arg_max_null({c}, fam_order) FILTER (WHERE {latest_of[c]} IS NOT NULL) AS {c}")
        elif c in END_DATE:  # a record with no end date is never chosen, whatever its code
            exprs.append(f"arg_max_null({c}, _end_rank) FILTER (WHERE srvc_end_dt IS NOT NULL) AS {c}")
        else:
            exprs.append(f"arg_max({c}, fam_order) FILTER (WHERE {c} IS NOT NULL) AS {c}")
    # The IP-R43 a rule (latest record with an end date), to count where the tiers change it
    old_end = ("arg_max(srvc_end_dt, fam_order) FILTER (WHERE srvc_end_dt IS NOT NULL) AS _old_end_dt, "
               "arg_max_null(srvc_end_dt_cd, fam_order) FILTER (WHERE srvc_end_dt IS NOT NULL) AS _old_end_cd")
    conn.execute(f"""
        CREATE OR REPLACE TABLE family_netted AS
        SELECT {fkey_sql}, {", ".join(exprs)}, COUNT(*) AS n_records,
            COUNT(*) FILTER (WHERE NOT {is_adjustment}) AS n_originals, {old_end}
        FROM family_records
        GROUP BY {fkey_sql}
    """)
    before = _count(conn, "stay_claims")
    conn.execute(f"""
        CREATE OR REPLACE TABLE stay_claims AS
        SELECT * FROM stay_claims WHERE NOT ({selected} AND {has_key})
        UNION ALL BY NAME
        SELECT * EXCLUDE (n_records, n_originals, _old_end_dt, _old_end_cd) FROM family_netted
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
    # End-date tier each family of 2+ records used, and families where it differs from the
    # IP-R43 a rule
    diag["families_end_date_tier"] = dict(conn.execute("""
        SELECT CASE WHEN srvc_end_dt_cd IN ('1', '4') THEN srvc_end_dt_cd ELSE 'other' END, COUNT(*)
        FROM family_netted WHERE n_records > 1 AND srvc_end_dt IS NOT NULL GROUP BY ALL ORDER BY ALL
    """).fetchall())
    diag["families_end_date_changed"] = _count(
        conn, "family_netted WHERE srvc_end_dt IS DISTINCT FROM _old_end_dt "
              "OR srvc_end_dt_cd IS DISTINCT FROM _old_end_cd")
    LOGGER.info(f"Claim families: {diag['family_rows']} rows, {diag['families_multi_record']} families "
                f"of 2+ records netted ({diag['family_records_merged']} rows merged), "
                f"{diag['family_rows_not_netted_missing_key']} rows without a full family key left as is")
    if diag["families_negative_net_paid"]:
        LOGGER.warning(f"{diag['families_negative_net_paid']} netted claim families have a negative "
                       f"Medicaid payment")


def prepare_links(conn: duckdb.DuckDBPyConnection, fallback_years: list[int] | None = None) -> dict:
    """
    Step 2 (plan §1 steps 5-8) on `stay_claims`, after `prepare_claims`: status counts, then
    columns `link_adm`, `link_dis`, `link_state`, `link_provider` and `nonlink_reasons`. Rows and
    other columns are unchanged. Returns run.json diagnostics.

    `fallback_years`: year files with no SUBMTG_STATE_CD (IP-R45 a), found before netting; pass
    prepare_claims' `link_state_fallback_years_input`. If None, they are found on `stay_claims`.
    A fallback year is reported even with 0 claims left (the year file still lacked the column).
    """
    diag: dict = {}
    # 5. Status kept as reported; values outside MAX's list counted (IP-R31). NULL is unknown and
    # links (B5011:228), so not counted
    diag["status_outside_max_list"] = _nested_counts(conn, f"""
        SELECT state, _file_year, patient_status_cd, COUNT(*) FROM stay_claims
        WHERE patient_status_cd NOT IN ({_sql_list(MAX_STATUS)}) GROUP BY ALL ORDER BY ALL
    """)

    # 7. Year files with no submitting state at all (the column is absent from the extract): key on
    # STATE_CD's main code there (IP-R45 a). Every claim in such a year file uses it, including a
    # netted Illinois claim carrying an earlier year's submitter code (its year file is the latest)
    if fallback_years is None:  # after netting: misses a year an Illinois claim netted into
        fallback_years = [r[0] for r in conn.execute(
            "SELECT _file_year FROM stay_claims GROUP BY _file_year HAVING COUNT(submtg_state_cd) = 0"
        ).fetchall()]
    fallback = {y: _count(conn, f"stay_claims WHERE _file_year = {int(y)}") for y in sorted(fallback_years)}
    diag["link_state_fallback_years"] = {str(y): n for y, n in fallback.items()}
    for y in fallback:
        LOGGER.warning(f"No SUBMTG_STATE_CD in year file {y}: link key uses STATE_CD's main FIPS code")
    main_code = ("CASE state " + " ".join(f"WHEN '{s}' THEN '{f}'" for s, f in STATE_FIPS.items())
                 + " END")  # NULL for a NULL or unmapped state
    link_state = (f"CASE WHEN _file_year IN ({', '.join(str(y) for y in fallback)}) THEN {main_code} "
                  f"ELSE submtg_state_cd END" if fallback else "submtg_state_cd")

    # 6-8. Link dates (IP-R25, IP-R37), key (IP-R18, IP-R39, IP-R44 b) and the reasons a claim can't
    # link, each on its own (NONLINK_REASONS, IP-R41)
    reasons = ", ".join(f"CASE WHEN {cond} THEN '{r}' END" for r, cond in NONLINK_REASONS.items())
    conn.execute(f"""
        CREATE OR REPLACE TABLE stay_claims AS
        WITH d AS (
            SELECT *,
                COALESCE(admission_date, srvc_bgn_dt) AS link_adm,
                CASE WHEN srvc_end_dt_cd IN ('1', '4') THEN srvc_end_dt END AS link_dis,
                {link_state} AS link_state,
                COALESCE(billing_provider_id, 'NPI:' || billing_npi) AS link_provider
            FROM stay_claims
        )
        SELECT *, list_filter([{reasons}], r -> r IS NOT NULL)::VARCHAR[] AS nonlink_reasons
        FROM d
    """)

    # Link dates
    diag["link_adm_filled"] = _nested_counts(conn, """
        SELECT state, COUNT(*) FROM stay_claims
        WHERE admission_date IS NULL AND srvc_bgn_dt IS NOT NULL GROUP BY ALL ORDER BY ALL
    """)
    diag["link_dis_null_by_end_date_code"] = _nested_counts(conn, """
        SELECT state, _file_year, srvc_end_dt_cd, COUNT(*) FROM stay_claims
        WHERE link_dis IS NULL GROUP BY ALL ORDER BY ALL
    """)
    # Link key: submitters (IP-R44 b), on year files that have them
    not_fallback = (f"_file_year NOT IN ({', '.join(str(y) for y in fallback)})" if fallback else "TRUE")
    diag["claims_state_not_in_fips_map"] = _nested_counts(conn, f"""
        SELECT state, COUNT(*) FROM stay_claims
        WHERE state IS NOT NULL AND state NOT IN ({_sql_list(STATE_FIPS)}) GROUP BY ALL ORDER BY ALL
    """)
    diag["claims_second_submitter"] = _nested_counts(conn, f"""
        SELECT submtg_state_cd, COUNT(*) FROM stay_claims
        WHERE {not_fallback} AND submtg_state_cd IN ({_sql_list(SECOND_SUBMITTERS)})
        GROUP BY ALL ORDER BY ALL
    """)
    # A NULL or unmapped state has no main code, so its claims aren't counted here (they are in
    # claims_by_state_and_submitter, and in claims_state_not_in_fips_map or the state_null reason)
    diag["claims_submitter_not_main_code"] = _nested_counts(conn, f"""
        SELECT state, submtg_state_cd, COUNT(*) FROM stay_claims
        WHERE {not_fallback} AND submtg_state_cd <> ({main_code}) GROUP BY ALL ORDER BY ALL
    """)
    diag["claims_by_state_and_submitter"] = _nested_counts(conn, f"""
        SELECT state, submtg_state_cd, COUNT(*) FROM stay_claims
        WHERE {not_fallback} GROUP BY ALL ORDER BY ALL
    """)
    # Provider: the share of the key carried by NPI (all claims), and "no provider" read as both
    # missing (IXF fn 45, the rule) or either missing (B5011:196-197)
    diag["claims_keyed_by_npi"] = _nested_counts(conn, """
        SELECT state, COUNT(*) FROM stay_claims
        WHERE billing_provider_id IS NULL AND billing_npi IS NOT NULL GROUP BY ALL ORDER BY ALL
    """)
    both, either = conn.execute("""
        SELECT COUNT(*) FILTER (WHERE billing_provider_id IS NULL AND billing_npi IS NULL),
               COUNT(*) FILTER (WHERE billing_provider_id IS NULL OR billing_npi IS NULL)
        FROM stay_claims
    """).fetchone()
    diag["claims_provider_missing"] = {"both": both, "either": either}
    # Non-linkable claims: inverted dates split by whether link_adm was reported or filled
    reported, filled = conn.execute("""
        SELECT COUNT(*) FILTER (WHERE admission_date IS NOT NULL), COUNT(*) FILTER (WHERE admission_date IS NULL)
        FROM stay_claims WHERE link_adm > link_dis
    """).fetchone()
    diag["claims_link_adm_after_link_dis"] = {"reported": reported, "filled": filled}
    diag["claims_non_linkable"] = _count(conn, "stay_claims WHERE len(nonlink_reasons) > 0")
    diag["claims_non_linkable_by_reason"] = _nested_counts(conn, """
        SELECT reason, state, _file_year, COUNT(*)
        FROM (SELECT unnest(nonlink_reasons) AS reason, state, _file_year FROM stay_claims)
        GROUP BY ALL ORDER BY ALL
    """)
    diag["claims_linkable"] = _count(conn, "stay_claims WHERE len(nonlink_reasons) = 0")
    LOGGER.info(f"Link preparation: {diag['claims_linkable']} claims linkable, "
                f"{diag['claims_non_linkable']} non-linkable (kept as single-claim stays)")
    return diag


def link_claims(conn: duckdb.DuckDBPyConnection, island_cap: int = ISLAND_CAP) -> dict:
    """
    Step 3 (plan §1 A step 9 and B; IP-R18, IP-R23, IP-R24, IP-R46, IP-R47) on `stay_claims`,
    after `prepare_links`: adds `claim_rid` (claim order) and `stay_id` (the stay's smallest
    claim_rid). Rows and other columns are unchanged. Returns run.json diagnostics.

    Raises ValueError if it already ran on `stay_claims`, or if a touch-island holds more than
    `island_cap` claims (IP-R47 a).
    """
    cols = _columns(conn, "stay_claims")
    if "claim_rid" in cols or "stay_id" in cols:
        raise ValueError("link_claims already ran on stay_claims")
    try:
        return _link_claims(conn, cols, island_cap)
    finally:  # on errors too (e.g. the island cap)
        for t in ("_stay_order", "_stay_link", "_stay_pairs", "_stay_comp", "_stay_ids", "_stay_state",
                  "_stay_split", "_stay_sub", "_stay_subcomp"):
            _drop_temp(conn, t)


def _link_claims(conn: duckdb.DuckDBPyConnection, cols: dict[str, str], island_cap: int) -> dict:
    """The body of `link_claims`, on `stay_claims` with columns `cols`."""
    diag: dict = {}
    key = ", ".join(LINK_KEY)

    # 9. Claim order, sorted on a narrow projection (rowid -> claim_rid), joined to the claims once
    # at the end. The last tie-break is an md5 of the other columns (`_row_text`, IP-R48 b), so the
    # order doesn't depend on input order (NULL claim_ids included)
    ordered = LINK_KEY + ["link_adm", "srvc_bgn_dt", "link_dis"]
    rest = {c: t for c, t in cols.items() if c not in ordered + ["claim_id"]}
    order = (f"{', '.join(f'{c} NULLS LAST' for c in ordered)}, {CLASS_RANK}, claim_id NULLS LAST, "
             f"md5({_row_text(rest)})")
    if conn.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]:
        # An ORDER BY result is stored in order, so its rowid is the rank. Much faster than an
        # unpartitioned ROW_NUMBER, which duckdb 1.0.0 runs on one thread (minutes per million rows)
        conn.execute(f"CREATE OR REPLACE TEMP TABLE _stay_order AS SELECT rowid AS _row FROM stay_claims "
                     f"ORDER BY {order}")
        conn.execute("CREATE OR REPLACE TEMP TABLE _stay_order AS SELECT _row, rowid + 1 AS claim_rid "
                     "FROM _stay_order")
    else:
        conn.execute(f"CREATE OR REPLACE TEMP TABLE _stay_order AS "
                     f"SELECT rowid AS _row, ROW_NUMBER() OVER (ORDER BY {order}) AS claim_rid FROM stay_claims")

    # 10a. Touch-islands of linkable claims: within a key, in claim order (link_adm ascending), a
    # claim starts a new island when it begins more than a day after every earlier claim ended.
    # Every link lies inside an island (IP-R46)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _stay_link AS
        WITH m AS (
            SELECT o.claim_rid AS rid, state, link_adm, link_dis, patient_status_cd, {key},
                MAX(link_dis) OVER (PARTITION BY {key} ORDER BY o.claim_rid
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
            FROM stay_claims s JOIN _stay_order o ON o._row = s.rowid
            WHERE len(nonlink_reasons) = 0)
        SELECT rid, state, link_adm, link_dis, patient_status_cd,
            MAX(CASE WHEN prev_max IS NULL OR link_adm > prev_max + 1 THEN rid END)
                OVER (PARTITION BY {key} ORDER BY rid ROWS UNBOUNDED PRECEDING) AS island
        FROM m
    """)

    # 10b. Island cap, before the pair join (IP-R47 a)
    largest = conn.execute("""
        SELECT arg_min_null(state, rid), COUNT(*) AS n, MIN(link_adm), MAX(link_adm)
        FROM _stay_link GROUP BY island ORDER BY n DESC, island LIMIT 10
    """).fetchall()
    diag["link_island_size_max"] = largest[0][1] if largest else 0
    diag["link_island_sizes_top"] = [["NULL" if s is None else s, n] for s, n, _, _ in largest]
    if largest and largest[0][1] > island_cap:
        top = [("NULL" if s is None else s, n, str(a), str(b)) for s, n, a, b in largest]
        raise ValueError(f"A group of touching same-key claims holds {largest[0][1]} claims, over "
                         f"island_cap {island_cap} (IP-R47 a). Largest groups (state, claims, first "
                         f"and last link_adm): {top}")

    # 10c. Pairs of touching claims in an island, with each condition (B5011:216-233). Linking
    # uses strict overlap (IP-R24); the `overlap` flag leaves out containment (attribution only,
    # same links). `contiguous`: admission on or the day after the other's discharge, when that
    # preceding claim's status is 30 or NULL (Table 1; other values block, IP-R31)
    contain = ("((a.link_adm <= b.link_adm AND b.link_dis <= a.link_dis) "
               "OR (b.link_adm <= a.link_adm AND a.link_dis <= b.link_dis))")
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _stay_pairs AS
        WITH p AS (
            SELECT a.rid AS a, b.rid AS b, a.island, a.patient_status_cd AS a_status,
                a.link_adm = b.link_adm AS same_adm,
                a.link_dis = b.link_dis AS same_dis,
                {contain} AS containment,
                a.link_adm < b.link_dis AND b.link_adm < a.link_dis AND NOT {contain} AS overlap,
                (b.link_adm IN (a.link_dis, a.link_dis + 1) AND COALESCE(a.patient_status_cd = '30', TRUE))
                 OR (a.link_adm IN (b.link_dis, b.link_dis + 1)
                     AND COALESCE(b.patient_status_cd = '30', TRUE)) AS contiguous
            FROM _stay_link a JOIN _stay_link b
                ON a.island = b.island AND a.rid < b.rid AND b.link_adm <= a.link_dis + 1)
        SELECT *, {" OR ".join(LINK_CONDITIONS)} AS linked FROM p
    """)

    # 10d. Stays = components of the links (IP-R23). An island with no blocked pair is one stay;
    # the others go to `_components`
    blocked = "SELECT DISTINCT island FROM _stay_pairs WHERE NOT linked"
    rounds, uf_edges = _components(
        conn, f"SELECT rid FROM _stay_link WHERE island IN ({blocked})",
        f"SELECT a, b FROM _stay_pairs WHERE linked AND island IN ({blocked})", "_stay_comp")
    conn.execute("""
        CREATE OR REPLACE TABLE stay_claims AS
        SELECT o.claim_rid, s.*, COALESCE(c.comp, l.island, o.claim_rid) AS stay_id
        FROM stay_claims s JOIN _stay_order o ON o._row = s.rowid
            LEFT JOIN _stay_link l ON l.rid = o.claim_rid LEFT JOIN _stay_comp c ON c.rid = o.claim_rid
    """)
    diag["link_islands_with_blocked_pairs"] = _count(conn, f"({blocked})")
    diag["link_component_sql_rounds"] = rounds
    diag["link_component_uf_edges"] = uf_edges

    _link_diagnostics(conn, diag)
    LOGGER.info(f"Linking: {diag['stays']} stays ({diag['stays_non_linkable_single']} non-linkable "
                f"singles) from {diag['link_edges']} links; largest stay {diag['stay_size_max']} claims, "
                f"largest touch-island {diag['link_island_size_max']}")
    return diag


def _link_diagnostics(conn: duckdb.DuckDBPyConnection, diag: dict) -> None:
    """run.json counts of step 3 (IP-R46 c), from `_stay_pairs` and the linked `stay_claims`."""
    # Links and their conditions
    diag["link_pairs_touching"] = _count(conn, "_stay_pairs")
    diag["link_edges"] = _count(conn, "_stay_pairs WHERE linked")
    names = ", ".join(f"CASE WHEN {c} THEN '{c}' END" for c in LINK_CONDITIONS)
    diag["link_edges_by_conditions"] = dict(conn.execute(f"""
        SELECT concat_ws('+', {names}) AS conds, COUNT(*) FROM _stay_pairs WHERE linked
        GROUP BY conds ORDER BY conds
    """).fetchall())
    # The only attributed condition. An `overlap` or `contiguous` sole link would be lost without
    # it; a `containment` sole link is a strict containment, also a literal strict overlap
    diag["link_edges_by_sole_condition"] = {
        c: n for c, n in diag["link_edges_by_conditions"].items() if "+" not in c}
    # Blocked contiguities by the preceding claim's status. `a` has the smaller claim_rid, so the
    # earlier or equal link_adm: a blocked pair's preceding claim is always `a` (NULL never blocks)
    diag["link_contiguities_blocked"] = _nested_counts(conn, """
        SELECT a_status, COUNT(*) FROM _stay_pairs WHERE NOT linked GROUP BY ALL ORDER BY ALL
    """)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _stay_ids AS
        SELECT claim_rid AS rid, stay_id, len(nonlink_reasons) = 0 AS linkable, link_adm, link_dis,
            COALESCE(claim_type_cd IN ('5', 'E'), FALSE) AS e17_5e,
            NOT COALESCE(claim_type_cd IN ('5', 'E'), FALSE)
                AND COALESCE(srvc_end_dt - srvc_bgn_dt > {E17_LONG_DAYS}, FALSE) AS e17_long
        FROM stay_claims
    """)
    diag["link_contiguities_blocked_same_stay"] = _count(conn, """
        _stay_pairs p JOIN _stay_ids x ON x.rid = p.a JOIN _stay_ids y ON y.rid = p.b
        WHERE NOT p.linked AND x.stay_id = y.stay_id""")

    # Stays. A stay's state is its first claim's (IP-R47 b)
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE _stay_state AS
        SELECT stay_id, arg_min_null(state, claim_rid) AS stay_state, COUNT(*) AS n_claims,
            COUNT(DISTINCT state) AS n_states, COUNT(DISTINCT link_adm) AS n_link_adm,
            COUNT(DISTINCT admission_date) AS n_admission_date, COUNT(DISTINCT _file_year) AS n_file_years
        FROM stay_claims GROUP BY stay_id
    """)
    diag["stays"] = _count(conn, "_stay_state")
    diag["stays_non_linkable_single"] = _count(conn, "stay_claims WHERE len(nonlink_reasons) > 0")

    def by_state(where: str) -> dict:
        return _nested_counts(conn, f"""
            SELECT stay_state, COUNT(*) FROM _stay_state WHERE {where} GROUP BY ALL ORDER BY ALL""")
    diag["stays_multi_claim"] = by_state("n_claims > 1")
    diag["stays_multiple_states"] = by_state("n_states > 1")
    diag["stay_size_counts"] = _nested_counts(conn, """
        SELECT n_claims, COUNT(*) FROM _stay_state GROUP BY ALL ORDER BY ALL""")
    diag["stay_size_max"] = conn.execute("SELECT COALESCE(MAX(n_claims), 0) FROM _stay_state").fetchone()[0]
    diag["stays_multiple_link_adm"] = by_state("n_link_adm > 1")
    diag["stays_multiple_admission_date"] = by_state("n_admission_date > 1")
    diag["stays_multiple_file_years"] = by_state("n_file_years > 1")

    # Zero-length claims that bridge: the stay's other linkable claims would form 2+ stays without
    # them (IP-R46 c)
    _split_if_removed(conn, "link_adm = link_dis", "_stay_split")
    diag["stays_zero_length_bridged"] = _nested_counts(conn, """
        SELECT stay_state, COUNT(*) FROM _stay_split JOIN _stay_state USING (stay_id)
        GROUP BY ALL ORDER BY ALL""")

    # E17 (plan §3b, IP-R21, IP-R46): stays whose base claims would form 2+ stays without their 5/E
    # and long claims. Kind = the excluded claims the stay has; conditions are counted on links
    # between an excluded and a base claim only
    _split_if_removed(conn, "e17_5e OR e17_long", "_stay_split")
    diag["e17_bridged_stays"] = _nested_counts(conn, """
        WITH k AS (
            SELECT stay_id, bool_or(e17_5e) AS has_5e, bool_or(e17_long) AS has_long
            FROM _stay_ids WHERE linkable AND stay_id IN (SELECT stay_id FROM _stay_split)
            GROUP BY stay_id)
        SELECT stay_state, CASE WHEN has_5e AND has_long THEN '5/E+long' WHEN has_5e THEN '5/E'
                                ELSE 'long' END, COUNT(*)
        FROM k JOIN _stay_state USING (stay_id) GROUP BY ALL ORDER BY ALL""")
    counts = conn.execute(f"""
        SELECT {", ".join(f"COUNT(DISTINCT x.stay_id) FILTER (WHERE p.{c})" for c in LINK_CONDITIONS)}
        FROM _stay_pairs p JOIN _stay_ids x ON x.rid = p.a JOIN _stay_ids y ON y.rid = p.b
        WHERE p.linked AND x.stay_id IN (SELECT stay_id FROM _stay_split)
            AND (x.e17_5e OR x.e17_long) <> (y.e17_5e OR y.e17_long)
    """).fetchone()
    diag["e17_bridged_stays_by_condition"] = {c: n for c, n in zip(LINK_CONDITIONS, counts) if n}
    diag["e17_extra_stays"] = _nested_counts(conn, """
        SELECT stay_state, SUM(n_parts - 1)::BIGINT FROM _stay_split JOIN _stay_state USING (stay_id)
        GROUP BY ALL ORDER BY ALL""")
    bridged = _nested_counts(conn, """
        SELECT stay_state, COUNT(*) FROM _stay_split JOIN _stay_state USING (stay_id)
        GROUP BY ALL ORDER BY ALL""")
    # Denominator: stays materialization keeps, so not the NULL-MSIS-ID and '&' singles (IP-R48 a)
    dropped = _count(conn, "stay_claims WHERE list_contains(nonlink_reasons, 'msis_id_null') "
                           "OR list_contains(nonlink_reasons, 'amp_msis_id')")
    diag["e17_threshold"] = _e17_threshold(bridged, diag["stays_multi_claim"], diag["stays"] - dropped)
    conn.execute("DROP TABLE _stay_split")


def _split_if_removed(conn: duckdb.DuckDBPyConnection, excluded_sql: str, out: str) -> None:
    """
    Table `out` (stay_id, n_parts): stays with an excluded linkable claim (`excluded_sql`, on
    `_stay_ids` columns) whose other linkable claims, linked as in `_stay_pairs`, would form
    n_parts >= 2 stays without the excluded ones.
    """
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _stay_sub AS
        WITH x AS (SELECT rid, stay_id, COALESCE(({excluded_sql}), FALSE) AS excl FROM _stay_ids
                   WHERE linkable)
        SELECT rid, stay_id FROM x WHERE NOT excl AND stay_id IN (
            SELECT stay_id FROM x GROUP BY stay_id
            HAVING COUNT(*) FILTER (WHERE excl) > 0 AND COUNT(*) FILTER (WHERE NOT excl) > 1)
    """)
    _components(conn, "SELECT rid FROM _stay_sub",
                "SELECT a, b FROM _stay_pairs WHERE linked AND a IN (SELECT rid FROM _stay_sub) "
                "AND b IN (SELECT rid FROM _stay_sub)", "_stay_subcomp")
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT s.stay_id, COUNT(DISTINCT c.comp) AS n_parts
        FROM _stay_subcomp c JOIN _stay_sub s USING (rid)
        GROUP BY s.stay_id HAVING COUNT(DISTINCT c.comp) > 1
    """)
    conn.execute("DROP TABLE _stay_sub")
    conn.execute("DROP TABLE _stay_subcomp")


def _components(
    conn: duckdb.DuckDBPyConnection,
    nodes_sql: str,
    edges_sql: str,
    out: str,
    sql_rounds: int = COMPONENT_SQL_ROUNDS,
) -> tuple[int, int]:
    """
    Connected components: table `out` (rid, comp), comp = the component's smallest rid.
    `nodes_sql` returns column rid; `edges_sql` returns a, b (both nodes). Returns (SQL rounds
    used, edges left for the Python union-find).

    Up to `sql_rounds` rounds of min-label propagation with pointer jumping, then a union-find on
    the distinct pairs of different labels (each label is a node of its own component, so they give
    the same components). Correct whatever the number of rounds: the rounds only shrink what Python
    gets (on a path whose ids are out of order, propagation alone can need about n rounds).
    """
    try:
        used, n_pairs = _components_run(conn, nodes_sql, edges_sql, out, sql_rounds)
    finally:  # on errors too
        for t in ("_cc_lab", "_cc_lab2", "_cc_e", "_cc_fix"):
            _drop_temp(conn, t)
    return used, n_pairs


def _drop_temp(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    """
    Drops temp table `table` if it exists; a caller's main table of that name is left alone.
    Best-effort: run in a `finally`, a failure here (e.g. an aborted transaction) mustn't replace
    the original exception.
    """
    try:
        conn.execute(f"DROP TABLE IF EXISTS temp.{table}")
    except duckdb.Error:
        pass


def _components_run(conn: duckdb.DuckDBPyConnection, nodes_sql: str, edges_sql: str, out: str,
                    sql_rounds: int) -> tuple[int, int]:
    """The body of `_components`."""
    conn.execute(f"CREATE OR REPLACE TEMP TABLE _cc_lab AS SELECT DISTINCT rid, rid AS l FROM ({nodes_sql})")
    n_nodes = _count(conn, "_cc_lab")
    conn.execute(f"CREATE OR REPLACE TEMP TABLE _cc_e AS SELECT a, b FROM ({edges_sql}) "
                 f"UNION ALL SELECT b, a FROM ({edges_sql})")
    used = 0
    for used in range(1, (sql_rounds if n_nodes else 0) + 1):
        # m(v) = smallest label of v and its neighbours, then l(v) = min(m(v), m(m(v)))
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _cc_lab2 AS
            WITH m AS (
                SELECT x.rid, LEAST(x.l, COALESCE(MIN(n.l), x.l)) AS l
                FROM _cc_lab x LEFT JOIN _cc_e e ON e.a = x.rid LEFT JOIN _cc_lab n ON n.rid = e.b
                GROUP BY x.rid, x.l)
            SELECT m.rid, LEAST(m.l, j.l) AS l FROM m JOIN m j ON j.rid = m.l
        """)
        changed = _count(conn, "_cc_lab JOIN _cc_lab2 USING (rid) WHERE _cc_lab.l <> _cc_lab2.l")
        conn.execute("CREATE OR REPLACE TEMP TABLE _cc_lab AS FROM _cc_lab2")
        if not changed:
            break
    # Contract to distinct pairs of different labels; union-find with the smaller label as root
    pairs = conn.execute("""
        SELECT DISTINCT la.l, lb.l FROM _cc_e e
        JOIN _cc_lab la ON la.rid = e.a JOIN _cc_lab lb ON lb.rid = e.b WHERE la.l < lb.l
    """).fetchall()
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:  # path compression
            parent[x], x = root, parent[x]
        return root
    for a, b in pairs:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    labels = list(parent)
    conn.execute("CREATE OR REPLACE TEMP TABLE _cc_fix AS "
                 "SELECT unnest(?::BIGINT[]) AS l, unnest(?::BIGINT[]) AS root",
                 [labels, [find(x) for x in labels]])
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE {out} AS
        SELECT x.rid, COALESCE(f.root, x.l) AS comp FROM _cc_lab x LEFT JOIN _cc_fix f ON f.l = x.l
    """)
    if _count(conn, out) != n_nodes:
        raise RuntimeError(f"_components lost or repeated a node ({_count(conn, out)} rows, {n_nodes} nodes)")
    return used, len(pairs)


def _e17_threshold(bridged_by_state: dict, multi_by_state: dict, n_stays: int) -> dict:
    """
    The E17 threshold (plan §3b; IP-R47 d), from {state: bridged stays}, {state: multi-claim
    stays} and `n_stays`, the stays materialization keeps (IP-R48 a). Reached when bridged stays
    are E17_STATE_SHARE or more of the multi-claim stays of a state with at least
    E17_STATE_MIN_MULTI of them (every state is listed), or E17_ALL_SHARE or more of `n_stays`.
    Logs a warning then; the run goes on.
    """
    states = {}
    for s in sorted(set(multi_by_state) | set(bridged_by_state)):
        b, m = bridged_by_state.get(s, 0), multi_by_state.get(s, 0)
        states[s] = {"bridged": b, "multi_claim": m, "share": b / m if m else 0.0,
                     "tested": m >= E17_STATE_MIN_MULTI}
    over = [s for s, v in states.items() if v["tested"] and v["share"] >= E17_STATE_SHARE]
    share_all = sum(bridged_by_state.values()) / n_stays if n_stays else 0.0
    out = {"states": states, "states_over": over, "n_stays": n_stays, "share_of_all_stays": share_all,
           "exceeded": bool(over) or share_all >= E17_ALL_SHARE}
    if out["exceeded"]:
        LOGGER.warning(f"E17 threshold reached (plan §3b): IP-R21 goes back to the user (states "
                       f"over {E17_STATE_SHARE:.0%}: {over}; share of all stays {share_all:.3%})")
    return out


def _check_first_taf_year(first_taf_year: dict) -> None:
    """
    Raises ValueError unless every key is a two-letter upper-case state code and every value an
    int (not bool) from 1999 to 2100. Keys and values go into SQL after this check.
    """
    if not isinstance(first_taf_year, dict):
        raise ValueError(f"first_taf_year must be a dict {{state: year}}, not {type(first_taf_year).__name__}")
    bad = {k: v for k, v in first_taf_year.items()
           if not (isinstance(k, str) and re.fullmatch(r"[A-Z]{2}", k))
           or not (isinstance(v, int) and not isinstance(v, bool) and 1999 <= v <= 2100)}
    if bad:
        raise ValueError(f"first_taf_year: bad entries (key must be a two-letter upper-case state code, "
                         f"value an int year 1999-2100): {bad}")


def load_first_taf_year(path: str | Path) -> dict[str, int]:
    """First TAF year per state from cutover.yaml (IP-R40); validated by _check_first_taf_year."""
    cfg = yaml.safe_load(Path(path).read_text())
    if not isinstance(cfg, dict) or "first_taf_year" not in cfg:
        raise ValueError(f"{path}: no `first_taf_year` mapping")
    table = cfg["first_taf_year"]
    _check_first_taf_year(table)
    return {str(k): int(v) for k, v in table.items()}


def _first_taf_case(first_taf_year: dict[str, int], state_expr: str) -> str:
    """SQL: the first TAF year of `state_expr`'s state, NULL for a NULL state or one not in the table."""
    if not first_taf_year:
        return "NULL::INT"
    return (f"(CASE {state_expr} "
            + " ".join(f"WHEN '{s}' THEN {int(y)}" for s, y in sorted(first_taf_year.items())) + " END)")


def _stay_rules(file_years: list[int]) -> dict[str, str | None]:
    """
    {output column: SQL on a stay's claims (GROUP BY stay_id), or None when the output step sets
    it}, in STAY_COLUMNS order (plan §2.2). The claims carry `_end_rank` (1 = the last claim,
    END_ORDER), `_file_rank` (1 = latest year file, then highest claim_rid) and `_xover`.
    """
    years = ", ".join(str(int(y)) for y in file_years) or "NULL"
    end_year = "YEAR(MAX(srvc_end_dt))"

    def first(c):  # the first claim's value, NULL included
        return f"arg_min_null({c}, claim_rid)"

    def first_nn(c):  # the first non-NULL value
        return f"arg_min({c}, claim_rid) FILTER (WHERE {c} IS NOT NULL)"

    def last(c):  # the last claim's value (IP-R28), NULL included
        return f"arg_min_null({c}, _end_rank)"

    def drg(c):  # DRG and its indicator from the last claim with a DRG
        return f"arg_min_null({c}, _end_rank) FILTER (WHERE drg_rel_group IS NOT NULL)"

    def latest_file(c):
        return f"arg_min_null({c}, _file_rank)"

    def money(c):
        # Multi-claim stays: rounded to cents; singles: the claim's value. `+ 0.0` turns -0.0 into
        # 0.0 and changes no other double (IP-R51 c, IP-R52 a)
        return f"CASE WHEN COUNT(*) = 1 THEN MIN({c}) + 0.0 ELSE CAST(ROUND(SUM({c}), 2) AS DOUBLE) + 0.0 END"
    return {
        "bene_id": first_nn("bene_id"),
        "msis_id": first("msis_id"),
        # NULL claim_ids kept, so the length is n_claims_combined (IP-R49 c)
        "claim_ids": "list_transform(list_sort(list({'r': claim_rid, 'c': claim_id})), x -> x.c)",
        "n_claims_combined": "COUNT(*)",
        # IP-R05, IP-R49 a, IP-R51 d: the latest end date's year if the run has files for it, else
        # the stay's latest year file
        "year": f"CAST(CASE WHEN {end_year} IN ({years}) THEN {end_year} ELSE MAX(_file_year) END AS INT)",
        "raw_filename": latest_file("raw_filename"),
        "source_type": latest_file("source_type"),
        "state": first("state"),  # = step 3's stay state (IP-R47 b)
        "admission_date": "MIN(link_adm)",
        "srvc_bgn_dt": "MIN(srvc_bgn_dt)",
        "srvc_end_dt": "MAX(srvc_end_dt)",
        "patient_status_cd": last("patient_status_cd"),
        "claim_type_cd": last("claim_type_cd"),
        "adjustment_cd": "MAX(adjustment_cd)",
        "primary_diagnosis": first_nn("primary_diagnosis"),
        # Code lists: singles from their claim, multi-claim stays from _stay_codes_* (IP-R49 c, PR4-4)
        "diagnoses": None,
        "diagnosis_version_cds": None,
        "procedures": None,
        "procedure_coding_system": None,
        # IP-R29: crossover -> 0 (even when the sum is NULL, IP-R49 d), else capped. A CASE: duckdb's
        # LEAST skips NULLs, so LEAST(NULL, 365) would be 365
        "mdcd_cvrd_days": (f"CAST(CASE WHEN bool_or(_xover) THEN 0 WHEN SUM(mdcd_cvrd_days) > {COVERED_DAYS_CAP} "
                           f"THEN {COVERED_DAYS_CAP} ELSE SUM(mdcd_cvrd_days) END AS INT)"),
        "mdcd_pymt_amt": money("mdcd_pymt_amt"),
        "tp_pymt_amt": money("tp_pymt_amt"),
        "mdcr_coinsur_pymt_amt": money("mdcr_coinsur_pymt_amt"),
        "mdcr_ded_pymt_amt": money("mdcr_ded_pymt_amt"),
        "drg_rel_group": drg("drg_rel_group"),
        "drg_rel_group_ind": drg("drg_rel_group_ind"),
        "billing_provider_id": first("billing_provider_id"),
        "billing_npi": first_nn("billing_npi"),
        "n_other_type_claims": f"COUNT(*) FILTER (WHERE claim_type_cd IN ({_sql_list(OTHER_TYPES)}))",
        "crossover_ind": "bool_or(_xover)",
        "cutover_ind": None,  # set in the output step (IP-R40)
        "amp_msis_id_ind": f"bool_or({AMP_CLAIM})",
    }


def aggregate_stays(
    conn: duckdb.DuckDBPyConnection,
    rollup_cfg: dict,
    file_years: list[int],
    first_taf_year: dict[str, int],
    out_table: str = "built_stays",
) -> dict:
    """
    Step 4 (plan §1 C 10-13; §4): `stay_claims` after link_claims -> `out_table`, one row per stay
    (columns STAY_COLUMNS). Row order of `out_table` is not defined (IP-R51 b). `stay_claims` is
    read only. Returns run.json diagnostics.

    `file_years`: the run's year files (prepare_claims' `file_years`), for the `year` fallback
    (IP-R49 a, IP-R51 d). `first_taf_year`: {state: first TAF year}, from `load_first_taf_year`
    (IP-R40, IP-R49 b). `rollup_cfg['crossover']`: the per-claim crossover SQL (IP-R50 a).

    Raises ValueError on bad input (before link_claims, unknown or missing columns, no crossover
    rule, a bad first_taf_year, claims of a year file not in `file_years`, code lists of
    CODE_POS_FACTOR or more elements), RuntimeError if the built stays fail the integrity checks.
    """
    cols = _columns(conn, "stay_claims")
    if "claim_rid" not in cols or "stay_id" not in cols:
        raise ValueError("stay_claims has no claim_rid / stay_id: run link_claims first")
    if out_table.lower() == "stay_claims" or out_table.lower().startswith("_stay_"):
        raise ValueError(f"out_table {out_table!r} would overwrite stay_claims or a step-4 temp table")
    # Every column must have a rule (IP-R16)
    expected = set(STAY_RULE_INPUTS) | {"claim_id"} | set(HELPER_COLUMNS) | set(PIPELINE_COLUMNS)
    unknown, missing = sorted(set(cols) - expected), sorted(expected - set(cols))
    if unknown or missing:
        raise ValueError(f"stay_claims columns don't match step 4's rules: unknown (no rule) {unknown}, "
                         f"missing {missing}")
    crossover = (rollup_cfg or {}).get("crossover")
    if not crossover:
        raise ValueError("rollup.crossover (the per-claim Medicare crossover SQL) is missing (IP-R50 a)")
    _check_first_taf_year(first_taf_year)
    data_years = [r[0] for r in conn.execute("SELECT DISTINCT _file_year FROM stay_claims").fetchall()]
    known = {int(y) for y in file_years}
    bad_years = sorted((y for y in data_years if y is None or y not in known), key=lambda y: (y is None, y))
    if bad_years:
        raise ValueError(f"stay_claims has claims with _file_year {bad_years} not in file_years "
                         f"{sorted(known)} (pass prepare_claims' file_years)")
    lens = conn.execute("SELECT " + ", ".join(f"COALESCE(MAX(len({c})), 0)" for c, _ in CODE_LISTS)
                        + " FROM stay_claims").fetchone()
    if max(lens) >= CODE_POS_FACTOR:
        raise ValueError(f"A code list is longer than {CODE_POS_FACTOR - 1} elements "
                         f"({dict(zip((c for c, _ in CODE_LISTS), lens))}); the code order key needs "
                         f"fewer (CODE_POS_FACTOR)")
    try:
        return _aggregate_stays(conn, crossover, list(file_years), dict(first_taf_year), out_table)
    finally:  # on errors too
        for t in STAY_TEMP_TABLES:
            _drop_temp(conn, t)


def _aggregate_stays(conn: duckdb.DuckDBPyConnection, crossover: str, file_years: list[int],
                     first_taf_year: dict[str, int], out_table: str) -> dict:
    """The body of `aggregate_stays`, after its checks."""
    rules = _stay_rules(file_years)
    if set(rules) != set(STAY_COLUMNS):
        raise RuntimeError(f"_stay_rules keys differ from STAY_COLUMNS: "
                           f"{sorted(set(rules) ^ set(STAY_COLUMNS))}")

    # 10. Ranks on a narrow table, joined back (a window over the full width is slow)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _stay_rank AS
        SELECT claim_rid,
            ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY {END_ORDER}) AS _end_rank,
            ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY _file_year DESC, claim_rid DESC) AS _file_rank
        FROM stay_claims
    """)
    # 11. One GROUP BY pass: the stay columns plus what the diagnostics and checks need
    agg = ",\n            ".join(f"{rules[c]} AS {c}" for c in STAY_COLUMNS if rules[c] is not None)
    conn.execute(f"""
        CREATE OR REPLACE TEMP TABLE _stay_agg AS
        WITH r AS (
            SELECT s.*, k._end_rank, k._file_rank, COALESCE(({crossover}), FALSE) AS _xover
            FROM stay_claims s JOIN _stay_rank k USING (claim_rid))
        SELECT stay_id,
            {agg},
            MIN(claim_rid) AS _min_rid,
            MIN(_end_rank) FILTER (WHERE drg_rel_group IS NOT NULL) AS _drg_rank,
            COUNT(DISTINCT COALESCE(claim_type_cd, '~')) AS _n_types,
            CAST(SUM(mdcd_cvrd_days) AS BIGINT) AS _days_sum,
            COUNT(DISTINCT billing_npi) AS _n_npi,
            COUNT(DISTINCT bene_id) AS _n_bene,
            MIN(admission_date) AS _adm_reported,
            MAX(_file_year) AS _latest_file_year,
            COUNT(*) - COUNT(claim_id) AS _n_claim_id_null,
            bool_or(len(nonlink_reasons) > 0) AS _has_non_linkable
        FROM r GROUP BY stay_id
    """)
    # 12. Code lists of multi-claim stays (IP-R49 c): distinct (code, flag) pairs at their first
    # (claim, position), NULL codes dropped. Positional flags, so a NULL flags list gives NULL flags
    for codes, flags in CODE_LISTS:
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _stay_codes_{codes} AS
            WITH u AS (
                SELECT stay_id, claim_rid, pos, {codes}[pos] AS code, {flags}[pos] AS flag
                FROM (SELECT stay_id, claim_rid, {codes}, {flags}, unnest(range(1, len({codes}) + 1)) AS pos
                      FROM stay_claims
                      WHERE stay_id IN (SELECT stay_id FROM _stay_agg WHERE n_claims_combined > 1))),
            f AS (
                SELECT stay_id, code, flag, MIN(claim_rid * {CODE_POS_FACTOR} + pos) AS k
                FROM u WHERE code IS NOT NULL GROUP BY stay_id, code, flag),
            g AS (
                SELECT stay_id, list_sort(list({{'k': k, 'c': code, 'f': flag}})) AS l FROM f GROUP BY stay_id)
            SELECT stay_id, list_transform(l, x -> x.c) AS {codes}, list_transform(l, x -> x.f) AS {flags}
            FROM g
        """)
    # 13. Output. A single's code lists are its claim's (s1: stay_id = its smallest claim_rid,
    # checked below); cutover_ind = admitted before Jan 1 of the state's first TAF year (IP-R40)
    alias = {codes: f"x{i}" for i, (codes, _) in enumerate(CODE_LISTS)}
    code_alias = {c: alias[codes] for codes, flags in CODE_LISTS for c in (codes, flags)}
    sel = []
    for c in STAY_COLUMNS:
        if c in code_alias:
            sel.append(f"CASE WHEN a.n_claims_combined = 1 THEN s1.{c} "
                       f"ELSE COALESCE({code_alias[c]}.{c}, []::VARCHAR[]) END AS {c}")
        elif c == "cutover_ind":
            sel.append(f"YEAR(a.admission_date) < {_first_taf_case(first_taf_year, 'a.state')} AS cutover_ind")
        else:
            sel.append(f"a.{c}")
    joins = " ".join(f"LEFT JOIN _stay_codes_{codes} {alias[codes]} ON {alias[codes]}.stay_id = a.stay_id"
                     for codes, _ in CODE_LISTS)
    conn.execute(f"""
        CREATE OR REPLACE TABLE {out_table} AS
        SELECT {", ".join(sel)}
        FROM _stay_agg a {joins}
        LEFT JOIN stay_claims s1 ON s1.claim_rid = a.stay_id
    """)
    diag: dict = {"stays_built": _count(conn, out_table)}
    _aggregate_diagnostics(conn, diag, crossover, file_years, first_taf_year, out_table)
    return diag


def _aggregate_diagnostics(conn: duckdb.DuckDBPyConnection, diag: dict, crossover: str,
                           file_years: list[int], first_taf_year: dict[str, int], out_table: str) -> None:
    """Integrity checks (raise RuntimeError) and run.json counts of step 4 (plan §4)."""
    n_claims, n_stays = conn.execute("SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM stay_claims").fetchone()
    in_stays, bad_len, bad_id, bad_mix, bad_amp, year_null = conn.execute("""
        SELECT CAST(COALESCE(SUM(n_claims_combined), 0) AS BIGINT),
            COUNT(*) FILTER (WHERE len(claim_ids) <> n_claims_combined),
            COUNT(*) FILTER (WHERE stay_id <> _min_rid),
            COUNT(*) FILTER (WHERE n_claims_combined > 1 AND _has_non_linkable),
            COUNT(*) FILTER (WHERE n_claims_combined > 1 AND amp_msis_id_ind),
            COUNT(*) FILTER (WHERE year IS NULL)
        FROM _stay_agg
    """).fetchone()
    diag["claims_in_stays"] = in_stays
    problems = {k: v for k, v in {
        "claims_in_stays minus claims": in_stays - n_claims,
        "stays_built minus stays": diag["stays_built"] - n_stays,
        "stays with len(claim_ids) <> n_claims_combined": bad_len,
        "stays whose stay_id is not their smallest claim_rid": bad_id,
        "multi-claim stays with a non-linkable claim": bad_mix,
        "multi-claim stays with amp_msis_id_ind": bad_amp,
        "stays with NULL year": year_null}.items() if v}
    if problems:
        raise RuntimeError(f"aggregate_stays integrity checks failed: {problems}")

    def by_state(where: str, table: str = "_stay_agg") -> dict:
        return _nested_counts(conn, f"SELECT state, COUNT(*) FROM {table} WHERE {where} GROUP BY ALL ORDER BY ALL")
    # Last claim and DRG (IP-R28)
    diag["stays_status_null"] = by_state("patient_status_cd IS NULL")
    diag["stays_drg_from_earlier_claim"] = by_state("_drg_rank > 1")
    diag["stays_drg_null"] = by_state("drg_rel_group IS NULL")
    diag["stays_mixed_claim_types"] = by_state("_n_types > 1")  # NULL counted as a type (IP-R50 b)
    # Crossover and covered days (IP-R29, IP-R49 d, IP-R52 b)
    diag["stays_crossover"] = by_state("crossover_ind")
    diag["stays_cvrd_days_zeroed"] = by_state("crossover_ind AND _days_sum IS DISTINCT FROM 0")
    diag["stays_cvrd_days_capped"] = by_state(f"NOT crossover_ind AND _days_sum > {COVERED_DAYS_CAP}")
    diag["stays_cvrd_days_negative"] = by_state("_days_sum < 0")
    diag["claims_crossover_clm_ind_null"] = _nested_counts(conn, """
        SELECT state, _file_year, COUNT(*) FROM stay_claims
        WHERE crossover_clm_ind IS NULL GROUP BY ALL ORDER BY ALL""")
    diag["claims_crossover_by_fallback"] = _nested_counts(conn, f"""
        SELECT state, _file_year, COUNT(*) FROM stay_claims
        WHERE crossover_clm_ind IS NULL AND COALESCE(({crossover}), FALSE) GROUP BY ALL ORDER BY ALL""")
    # First-claim and first-non-NULL fields
    diag["stays_multiple_npi"] = by_state("_n_npi > 1")  # IP-R39 a
    diag["stays_multiple_bene_id"] = by_state("_n_bene > 1")
    diag["stays_admission_filled"] = by_state("admission_date IS DISTINCT FROM _adm_reported")
    diag["stays_admission_null"] = by_state("admission_date IS NULL")
    diag["stays_admission_after_srvc_bgn"] = by_state("admission_date > srvc_bgn_dt")
    diag["stays_claim_ids_with_null"] = by_state("_n_claim_id_null > 0")  # IP-R49 c
    diag["stays_primary_diagnosis_not_first_code"] = _count(
        conn, f"{out_table} WHERE n_claims_combined > 1 AND primary_diagnosis IS DISTINCT FROM diagnoses[1]")
    # Year (IP-R05, IP-R49 a, IP-R51 d)
    years = ", ".join(str(int(y)) for y in file_years) or "NULL"
    diag["stays_by_year"] = _nested_counts(conn, f"SELECT year, COUNT(*) FROM {out_table} GROUP BY ALL ORDER BY ALL")
    fallback = _nested_counts(conn, f"""
        SELECT CASE WHEN srvc_end_dt IS NULL THEN 'end_date_null' ELSE 'end_year_without_files' END,
            year, COUNT(*)
        FROM _stay_agg WHERE srvc_end_dt IS NULL OR YEAR(srvc_end_dt) NOT IN ({years})
        GROUP BY ALL ORDER BY ALL""")
    # Both categories always listed (IP-R53 b)
    diag["stays_year_fallback"] = {k: fallback.get(k, {}) for k in ("end_date_null", "end_year_without_files")}
    diag["stays_year_not_latest_file_year"] = _count(conn, "_stay_agg WHERE year <> _latest_file_year")
    table_year = _first_taf_case(first_taf_year, "state")
    diag["stays_year_before_first_taf_year"] = _nested_counts(conn, f"""
        SELECT state, year, COUNT(*) FROM {out_table} WHERE year < {table_year} GROUP BY ALL ORDER BY ALL""")
    # Stays sharing the IP-R35 a admissions key (IP-R41), by why: NULL MSIS ID, a NULL date, other
    key = ", ".join(ADMISSIONS_KEY)
    shared = _nested_counts(conn, f"""
        SELECT CASE WHEN msis_id IS NULL THEN 'msis_id_null'
                    WHEN admission_date IS NULL OR srvc_bgn_dt IS NULL OR srvc_end_dt IS NULL THEN 'dates_null'
                    ELSE 'other' END,
            state, year, COUNT(*)
        FROM (SELECT {key}, COUNT(*) OVER (PARTITION BY {key}) AS n FROM {out_table})
        WHERE n > 1 GROUP BY ALL ORDER BY ALL""")
    diag["stays_sharing_admissions_key"] = {k: shared.get(k, {}) for k in ("msis_id_null", "dates_null", "other")}
    # '&' and other-type stays (IP-R42 12, IP-R38)
    diag["stays_amp_msis_id_ind"] = _nested_counts(conn, """
        SELECT state, CASE WHEN amp_msis_id_ind THEN 'TRUE' ELSE 'NULL' END, COUNT(*) FROM _stay_agg
        WHERE amp_msis_id_ind IS NOT FALSE GROUP BY ALL ORDER BY ALL""")
    diag["stays_all_other_type"] = by_state("n_other_type_claims = n_claims_combined")
    # Cutover (IP-R40)
    diag["stays_cutover"] = by_state("cutover_ind", out_table)
    diag["stays_cutover_state_not_in_table"] = by_state(f"state IS NULL OR {table_year} IS NULL", out_table)
    diag["stays_cutover_admission_null"] = by_state(f"admission_date IS NULL AND {table_year} IS NOT NULL",
                                                    out_table)
    state_years = conn.execute(
        "SELECT DISTINCT state, _file_year FROM stay_claims WHERE state IS NOT NULL").fetchall()
    data_first: dict = {}
    for s, y in state_years:
        data_first[s] = min(y, data_first.get(s, y))
    has = set(state_years)
    diag["cutover_first_taf_year"] = {s: {"table": first_taf_year.get(s), "data": y}
                                      for s, y in sorted(data_first.items())}
    diag["cutover_states_rows_before_table_year"] = sorted(
        s for s, y in data_first.items() if s in first_taf_year and y < first_taf_year[s])
    if diag["cutover_states_rows_before_table_year"]:
        LOGGER.warning(f"Claims in a year file before the state's first TAF year (cutover.yaml): "
                       f"{diag['cutover_states_rows_before_table_year']}")
    diag["cutover_states_no_rows_in_table_year"] = sorted(
        s for s in data_first
        if s in first_taf_year and first_taf_year[s] in file_years and (s, first_taf_year[s]) not in has)
    diag["cutover_states_no_rows"] = sorted(
        s for s, y in first_taf_year.items() if s not in data_first and file_years and y <= max(file_years))

    def total(d: dict) -> int:
        return sum(total(v) if isinstance(v, dict) else v for v in d.values())
    LOGGER.info(f"Stays built: {diag['stays_built']} from {diag['claims_in_stays']} claims; crossover "
                f"{total(diag['stays_crossover'])} (covered days zeroed {total(diag['stays_cvrd_days_zeroed'])}, "
                f"capped {total(diag['stays_cvrd_days_capped'])}); year fallbacks "
                f"{total(diag['stays_year_fallback'])}, year before first TAF year "
                f"{total(diag['stays_year_before_first_taf_year'])}; cutover flagged {total(diag['stays_cutover'])}")


# ---------------------------------------------------------------------------------------------
# Pipeline wiring (IP-R54): build cleanup, stay column types, MAX rows, key sharing
# ---------------------------------------------------------------------------------------------

def drop_prepare_tables(conn: duckdb.DuckDBPyConnection, input_table: str = "stay_input") -> None:
    """
    Drops `input_table` and the PREPARE_TABLES prepare_claims leaves behind, keeping `stay_claims`
    (IP-R54 d: the build drops step-1 tables before linking). Missing tables are skipped.
    """
    if input_table.lower() == "stay_claims":
        raise ValueError("drop_prepare_tables would drop stay_claims")
    for t in (input_table, *PREPARE_TABLES):
        conn.execute(f"DROP TABLE IF EXISTS {t}")


def stay_column_types(conn: duckdb.DuckDBPyConnection, table_cfg: dict) -> dict[str, str]:
    """
    {column: duckdb type name} of the stay output, in STAY_COLUMNS order (IP-R54 d): NEW_STAY_TYPES
    for the stay columns, the table YAML's `type` for the others, normalized by duckdb (INT ->
    INTEGER). Raises ValueError when a STAY_RULE_INPUTS column is missing from the YAML or has no
    `type`.
    """
    yaml_types: dict[str, str | None] = {}
    for col in table_cfg.get("columns") or []:
        name = list(col.keys())[0]
        yaml_types[name] = (col[name] or {}).get("type")
    missing = [c for c in STAY_RULE_INPUTS if c not in yaml_types]
    untyped = [c for c in STAY_RULE_INPUTS if c in yaml_types and not yaml_types[c]]
    if missing or untyped:
        raise ValueError(f"Stay columns missing from the table YAML {missing} or without a `type` {untyped}")
    types = {c: NEW_STAY_TYPES.get(c) or yaml_types[c] for c in STAY_COLUMNS}
    sel = ", ".join(f"CAST(NULL AS {types[c]}) AS {c}" for c in STAY_COLUMNS)
    return {r[0]: r[1] for r in conn.execute(f"DESCRIBE SELECT {sel}").fetchall()}


def max_rows_to_stays(
    conn: duckdb.DuckDBPyConnection,
    in_table: str,
    year: int,
    rollup_cfg: dict,
    first_taf_year: dict[str, int],
    types: dict[str, str],
    out_table: str,
) -> dict:
    """
    MAX rows (already stays) of run year `year` from `in_table` (harmonized table-YAML columns)
    -> table `out_table` with columns STAY_COLUMNS of `types` (`stay_column_types`). Returns
    run.json diagnostics.

    Stay rule inputs pass through unchanged (claim_id, the Illinois family columns and the helper
    columns are dropped; IP-R35 c). claim_ids and n_claims_combined are NULL, n_other_type_claims
    0, amp_msis_id_ind FALSE, crossover_ind MAX_CROSSOVER (IP-R42 12, IP-R38). cutover_ind
    (IP-R30, IP-R40, IP-R54 b), from the run year (= the MAX file year): NULL for a NULL state or
    one not in `first_taf_year`; FALSE outside the state's flagged year (first TAF year - 1); NULL
    with a NULL end date; else end date Dec 31 of that year and status 30 or NULL.

    Raises ValueError when a row passes `rollup_cfg['rows']` (TAF rows belong to the build) or on
    a bad `first_taf_year`.
    """
    year = int(year)
    n_rollup = _count(conn, f"{in_table} WHERE COALESCE(({rollup_cfg['rows']}), FALSE)")
    if n_rollup:
        raise ValueError(f"{n_rollup} rows of the non-rollup files of {year} pass rollup rows "
                         f"`{rollup_cfg['rows']}`: files outside rollup.path_pattern hold rollup rows; "
                         f"check rollup.path_pattern")
    _check_first_taf_year(first_taf_year)
    if list(types) != STAY_COLUMNS:
        raise ValueError("types must list STAY_COLUMNS in order (use stay_column_types)")
    ft = _first_taf_case(first_taf_year, "state")
    cutover = (f"CASE WHEN {ft} IS NULL THEN NULL "
               f"WHEN {year} <> {ft} - 1 THEN FALSE "
               f"WHEN srvc_end_dt IS NULL THEN NULL "
               f"ELSE srvc_end_dt = DATE '{year}-12-31' AND (patient_status_cd = '30' OR patient_status_cd IS NULL) END")
    new = {"claim_ids": "NULL", "n_claims_combined": "NULL", "n_other_type_claims": "0",
           "crossover_ind": MAX_CROSSOVER, "cutover_ind": cutover, "amp_msis_id_ind": "FALSE"}
    if set(new) != set(NEW_STAY_COLUMNS):
        raise RuntimeError("max_rows_to_stays has no rule for some stay columns")
    sel = ",\n            ".join(f"CAST({new.get(c, c)} AS {types[c]}) AS {c}" for c in STAY_COLUMNS)
    conn.execute(f"CREATE OR REPLACE TABLE {out_table} AS SELECT {sel} FROM {in_table}")

    def by_state(where: str) -> dict:
        return _nested_counts(conn, f"SELECT state, COUNT(*) FROM {out_table} WHERE {where} GROUP BY ALL ORDER BY ALL")
    diag: dict = {
        "max_rows": _count(conn, out_table),
        "max_rows_year_not_run_year": _count(conn, f"{out_table} WHERE year IS DISTINCT FROM {year}"),
        "max_crossover": by_state("crossover_ind"),
        "max_cutover": by_state("cutover_ind"),
        "max_cutover_end_date_null": by_state(f"{ft} - 1 = {year} AND srvc_end_dt IS NULL"),
        "max_cutover_state_not_in_table": by_state(f"{ft} IS NULL"),
    }
    present = {r[0] for r in conn.execute(f"SELECT DISTINCT state FROM {out_table}").fetchall()}
    # C6 (IP-R40 Q2, IP-R54 c): a state whose flagged year this is, with no MAX rows
    diag["max_cutover_states_no_rows_in_flagged_year"] = sorted(
        s for s, y in first_taf_year.items() if y - 1 == year and s not in present)
    if diag["max_rows_year_not_run_year"]:
        LOGGER.warning(f"{diag['max_rows_year_not_run_year']} MAX rows have `year` other than the run year "
                       f"{year}; cutover_ind uses the run year")
    return diag


def admissions_key_sharing(conn: duckdb.DuckDBPyConnection, table: str) -> dict:
    """
    Rows of `table` in groups of 2 or more sharing the IP-R35 a admissions key, by the group's mix
    (IP-R54 c): {"max_max": {state: rows}, "max_taf": {...}, "taf_taf": {...}}. MAX means
    source_type IS DISTINCT FROM 'taf'. NULL key parts group together, as in step 4.
    """
    key = ", ".join(ADMISSIONS_KEY)
    shared = _nested_counts(conn, f"""
        SELECT CASE WHEN n_taf = 0 THEN 'max_max' WHEN n_taf = n THEN 'taf_taf' ELSE 'max_taf' END, state, COUNT(*)
        FROM (SELECT state, COUNT(*) OVER w AS n, COUNT(*) FILTER (WHERE source_type = 'taf') OVER w AS n_taf
              FROM {table} WINDOW w AS (PARTITION BY {key}))
        WHERE n > 1 GROUP BY ALL ORDER BY ALL""")
    return {k: shared.get(k, {}) for k in ("max_max", "max_taf", "taf_taf")}
