"""Run the metadata-only Part B Outpatient harmonizer smoke test.

All generated rows and identifiers are synthetic. No CMS source data is read.
Run from the repository root with the project dependencies installed:

    python context_test/run_smoke.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "context_test" / "partb_outpatient_smoke.yaml"


def make_input(conn, path: Path, year: int) -> None:
    """Create one synthetic row with a different physical schema by year."""
    path.parent.mkdir(parents=True, exist_ok=True)
    values = {
        "bene_id": f"SYNTH-BENE-{year}",
        "claim_id": 1000 + year,
        "from_date": f"{year}-01-02",
        "thru_date": f"{year}-01-03",
        "payment": 12.5 + (year - 2016),
    }
    if year == 2016:
        query = """SELECT
            ?::VARCHAR AS BENE_ID,
            ?::BIGINT AS CLM_ID,
            ?::VARCHAR AS CLM_FROM_DT,
            ?::VARCHAR AS CLM_THRU_DT,
            ?::VARCHAR AS CLM_PMT_AMT
        """
    elif year == 2017:
        query = """SELECT
            ?::VARCHAR AS BENE_ID,
            ?::BIGINT AS CLM_ID,
            ?::DATE AS CLM_FROM_DT,
            ?::DATE AS CLM_THRU_DT,
            ?::DOUBLE AS CLM_PMT_AMT
        """
    else:
        query = """SELECT
            ?::VARCHAR AS BENE_ID,
            ?::VARCHAR AS CLM_ID,
            (?::DATE)::TIMESTAMP AS CLM_FROM_DT,
            (?::DATE)::TIMESTAMP AS CLM_THRU_DT,
            ?::DECIMAL(10,2) AS CLM_PMT_AMT
        """
    frame = conn.execute(
        query,
        [
            values["bene_id"],
            values["claim_id"] if year != 2018 else str(values["claim_id"]),
            values["from_date"],
            values["thru_date"],
            str(values["payment"]),
        ]
        if year == 2016
        else [
            values["bene_id"],
            values["claim_id"] if year == 2017 else str(values["claim_id"]),
            values["from_date"],
            values["thru_date"],
            values["payment"],
        ],
    ).fetchdf()
    conn.register("fixture_input", frame)
    conn.execute(f"COPY fixture_input TO '{path.as_posix()}' (FORMAT PARQUET)")
    conn.unregister("fixture_input")


def main() -> None:
    with CONFIG.open() as f:
        config = yaml.safe_load(f)
    assert config["table_name"] == "partb_outpatient_smoke"
    assert len(config["columns"]) == 5

    expected_columns = {
        "bene_id": ("BENE_ID", "VARCHAR"),
        "claim_id": ("CLM_ID", "VARCHAR"),
        "claim_from_date": ("CLM_FROM_DT", "DATE"),
        "claim_thru_date": ("CLM_THRU_DT", "DATE"),
        "claim_payment_amt": ("CLM_PMT_AMT", "DOUBLE"),
    }
    for item in config["columns"]:
        name, definition = next(iter(item.items()))
        assert (definition["source"], definition["type"]) == expected_columns[name]
        assert definition.get("reference", "").startswith("https://resdac.org/")

    try:
        import duckdb
        from harmonizecms import harmonize_table_year
    except ModuleNotFoundError as exc:
        if exc.name != "duckdb":
            raise
        print("PASS: YAML parses and fixture mappings have the expected fields, types, and ResDAC references.")
        print("SKIP: end-to-end smoke needs the project dependency duckdb, unavailable in this environment.")
        return

    with tempfile.TemporaryDirectory(prefix="partb-outpatient-smoke-") as tmp:
        temp_root = Path(tmp)
        basepath = temp_root / "input"
        output = temp_root / "output"

        with duckdb.connect() as conn:
            for year in (2016, 2017, 2018):
                make_input(conn, basepath / str(year) / "op" / "part-000.parquet", year)

        for year in (2016, 2017, 2018):
            harmonize_table_year(
                table_config_path=str(CONFIG),
                basepath=str(basepath),
                output_path=str(output),
                year=year,
            )
            run = json.loads((output / f"partb_outpatient_smoke_{year}.run.json").read_text())
            assert run["status"] == "ok", run
            assert run["n_input_files"] == 1, run

            with duckdb.connect() as conn:
                actual = conn.execute(
                    "SELECT * FROM read_parquet(?)",
                    [str(output / f"partb_outpatient_smoke_{year}.parquet")],
                ).fetchone()
            expected = (
                f"SYNTH-BENE-{year}",
                str(1000 + year),
                f"{year}-01-02",
                f"{year}-01-03",
                12.5 + (year - 2016),
            )
            actual = (actual[0], actual[1], actual[2].isoformat(), actual[3].isoformat(), actual[4])
            assert actual == expected, (year, actual, expected)

    print("PASS: synthetic YAML harmonization succeeded for 2016, 2017, and 2018.")
    print("PASS: identifier, date, and payment casts normalized the three fixture schemas.")
    print("No CMS records were accessed; outputs were created under a temporary directory and removed.")


if __name__ == "__main__":
    main()
