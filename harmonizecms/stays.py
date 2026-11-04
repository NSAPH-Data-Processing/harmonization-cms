"""
Combine TAF inpatient claims into MAX-style hospital stays (the `rollup:` block of ip.yaml).

Design: docs/max-taf/ip/rollup_plan_2026-10-22.md (IP-R18: MAX sets which claims go in, the CMS
brief 5011 Table 1 rule sets how they link); decisions in docs/max-taf/ip/decisions.md.

Built in steps (IP-R26). This file so far has steps 1-3: the claim preparation that runs before
linking (1-2), and the linking of claims into stays (3).

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

Input: a table of harmonized TAF claim rows (the ip.yaml columns) plus `_file_year`, the year
file each row came from. It must hold rollup rows only (`rollup.rows`, TAF); other rows stop the
run. Output: table `stay_claims`, one row per final-action claim.
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
    cmp_sql = ", ".join(compare)
    conn.execute(f"""
        CREATE OR REPLACE TABLE stay_claims AS
        (SELECT DISTINCT ON ({cmp_sql}, _file_year) * FROM stay_filtered
         WHERE claim_id IS NULL
         ORDER BY {cmp_sql}, _file_year, raw_filename)
        UNION ALL BY NAME
        (SELECT DISTINCT ON (_file_year, claim_id) * FROM stay_filtered
         WHERE claim_id IS NOT NULL
         ORDER BY _file_year, claim_id, raw_filename)
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
