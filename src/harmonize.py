import argparse
import yaml
import duckdb
import glob
import os
import re
import pandas as pd

# build input parquet path based upon pattern and basepath in yaml
def get_parquet_files(basepath, year, path_patterns):
    """
    Get all Parquet files for the given basepath, year, and path patterns.
    """
    all_parquet_files = []

    for pattern in path_patterns:
        # Replace placeholders in the pattern
        pattern = pattern.replace("{basepath}", basepath).replace("{year}", str(year))
        dir_pattern = os.path.dirname(pattern)
        # Find directories that match the directory pattern
        matched_dirs = [d for d in glob.glob(dir_pattern) if os.path.isdir(d)]

        # List full paths of each chunk
        for directory in matched_dirs:
            all_parquet_files.extend(sorted(glob.glob(os.path.join(directory, "part-*.parquet"))))

    return all_parquet_files

# build query based on yaml for a given table 
def construct_query(table_config, parquet_files):
    columns = []

    # Extract both column names and their DuckDB-inferred types
    query = f"DESCRIBE SELECT * FROM read_parquet('{parquet_files[0]}') LIMIT 0"
    schema_df = duckdb.query(query).to_df()
    file_schema = {row['column_name'].lower(): row['column_type'].upper() for _, row in schema_df.iterrows()}

    for col in table_config.get("columns", []):
        col_name = list(col.keys())[0]
        col_def = col[col_name]

        if not col_def:
            print(f"Warning: column definition for '{col_name}' is None. Skipping.")
            continue

        cast_dict = col_def.get("cast", {})
        cast_dict = {k.upper(): v for k, v in cast_dict.items()}

        source_expr = col_def.get("source")
        selected_source = None

        if isinstance(source_expr, list):
            for candidate in source_expr:
                if "{m}" not in candidate and candidate.lower() in file_schema:
                    selected_source = candidate
                    break
        elif isinstance(source_expr, str):
            if any(tok in source_expr.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"]):
                selected_source = source_expr
            elif source_expr.lower() in file_schema:
                selected_source = source_expr

        if not selected_source and not any("{m}" in s for s in source_expr if isinstance(source_expr, list)):
            print(f"'{col_name}' - No valid source found in schema. Creating column with NULL.")
            columns.append(f"NULL AS {col_name}")
            continue

        if col_name == "hmo_indicators":
            monthly_cols = []
            found_explicit_array = False

            for candidate in source_expr:
                if "{m}" in candidate:
                    for m in range(1, 13):
                        m_str = f"{m:02d}"
                        column_name = candidate.format(m=m_str).lower()
                        if column_name in file_schema:
                            monthly_cols.append(f"CAST({column_name} AS VARCHAR)")
                        else:
                            monthly_cols.append("NULL")
                elif candidate.lower() in file_schema:
                    # Treat known "array-like" varchar fields (e.g., '000000000000')
                    found_explicit_array = True
                    expr = f"list_transform(range(12), i -> CAST(substr({candidate}, i + 1, 1) AS VARCHAR))"
                    columns.append(f"{expr} AS {col_name}")
                    break

            if not found_explicit_array:
                if monthly_cols:
                    array_expr = f"array_value({', '.join(monthly_cols)})"
                    columns.append(f"{array_expr} AS {col_name}")
                else:
                    print(f"'{col_name}' - No valid columns found for array construction. NULL inserted.")
                    columns.append(f"NULL AS {col_name}")
            continue

        # --- Default logic ---
        is_sql_expr = any(tok in selected_source.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"])
        detected_type = file_schema.get(selected_source.lower()) if not is_sql_expr else None
        col_type = detected_type or str(col_def.get("type", "")).upper()
        cast_template = cast_dict.get(col_type) or cast_dict.get("*") or "{column_name}"

        if is_sql_expr:
            expr = selected_source
        else:
            expr = cast_template.format(column_name=selected_source)

        columns.append(f"{expr} AS {col_name}")

    columns_str = ", ".join(columns)
    files_str = ", ".join([f"'{file}'" for file in parquet_files])

    return f"""
        CREATE OR REPLACE TABLE {table_config['name']} AS
        SELECT {columns_str}
        FROM read_parquet([{files_str}]);
    """
def construct_query(table_config, parquet_files):
    columns = []

    # Extract both column names and their DuckDB-inferred types
    query = f"DESCRIBE SELECT * FROM read_parquet('{parquet_files[0]}') LIMIT 0"
    schema_df = duckdb.query(query).to_df()
    file_schema = {row['column_name'].lower(): row['column_type'].upper() for _, row in schema_df.iterrows()}

    for col in table_config.get("columns", []):
        col_name = list(col.keys())[0]
        col_def = col[col_name]

        if not col_def:
            print(f"Warning: column definition for '{col_name}' is None. Skipping.")
            continue

        cast_dict = col_def.get("cast", {})
        cast_dict = {k.upper(): v for k, v in cast_dict.items()}

        source_expr = col_def.get("source")
        selected_source = None

        if isinstance(source_expr, list):
            for candidate in source_expr:
                if "{m}" not in candidate and candidate.lower() in file_schema:
                    selected_source = candidate
                    break
        elif isinstance(source_expr, str):
            if any(tok in source_expr.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"]):
                selected_source = source_expr
            elif source_expr.lower() in file_schema:
                selected_source = source_expr

        if not selected_source and not any("{m}" in s for s in source_expr if isinstance(source_expr, list)):
            print(f"'{col_name}' - No valid source found in schema. Creating column with NULL.")
            columns.append(f"NULL AS {col_name}")
            continue

        if col_name == "hmo_indicators":
            monthly_cols = []
            found_explicit_array = False

            for candidate in source_expr:
                if "{m}" in candidate:
                    for m in range(1, 13):
                        m_str = f"{m:02d}"
                        column_name = candidate.format(m=m_str).lower()
                        if column_name in file_schema:
                            monthly_cols.append(f"CAST({column_name} AS VARCHAR)")
                        else:
                            monthly_cols.append("NULL")
                elif candidate.lower() in file_schema:
                    # Treat known "array-like" varchar fields (e.g., '000000000000')
                    found_explicit_array = True
                    expr = f"list_transform(range(12), i -> CAST(substr({candidate}, i + 1, 1) AS VARCHAR))"
                    columns.append(f"{expr} AS {col_name}")
                    break

            if not found_explicit_array:
                if monthly_cols:
                    array_expr = f"array_value({', '.join(monthly_cols)})"
                    columns.append(f"{array_expr} AS {col_name}")
                else:
                    print(f"'{col_name}' - No valid columns found for array construction. NULL inserted.")
                    columns.append(f"NULL AS {col_name}")
            continue

        # --- Default logic ---
        is_sql_expr = any(tok in selected_source.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"])
        detected_type = file_schema.get(selected_source.lower()) if not is_sql_expr else None
        col_type = detected_type or str(col_def.get("type", "")).upper()
        cast_template = cast_dict.get(col_type) or cast_dict.get("*") or "{column_name}"

        if is_sql_expr:
            expr = selected_source
        else:
            expr = cast_template.format(column_name=selected_source)

        columns.append(f"{expr} AS {col_name}")

    columns_str = ", ".join(columns)
    files_str = ", ".join([f"'{file}'" for file in parquet_files])

    return f"""
        CREATE OR REPLACE TABLE {table_config['name']} AS
        SELECT {columns_str}
        FROM read_parquet([{files_str}]);
    """


def process_tables(config, output_path, table_to_run=None, year_to_run=None):
    """
    Process tables based on the configuration and save the output as Parquet files.
    """
    print(f"Processing tables with config: {config}")
    conn = duckdb.connect(database=':memory:')
    os.makedirs(output_path, exist_ok=True)

    years = [int(year_to_run)]

    for table_name, table_config in config['tables'].items():
        if table_to_run is not None and table_name != table_to_run:
            continue

        table_config['name'] = table_name

        for year in years:
            # Handle multiple basepaths or path patterns
            path_patterns = table_config.get('path_pattern', [])
            if isinstance(path_patterns, str):
                path_patterns = [path_patterns]

            parquet_files = []
            for basepath in config.get('basepaths', [config['basepath']]):
                parquet_files.extend(get_parquet_files(basepath, year, path_patterns))

            print(f"Found Parquet files for table '{table_name}', year {year}: {parquet_files}")
            if parquet_files:
                print(f"Processing table: {table_name}, year: {year}")
                query = construct_query(table_config, parquet_files)
                conn.execute(query)
                output_file = os.path.join(output_path, f"{table_name}_{year}.parquet")
                conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_file}' (FORMAT 'parquet')")
                print(f"Saved output to: {output_file}")

    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Process tables using harmonization rules.")
    parser.add_argument(
        "--config_path",
        type=str,
        default="../harmonization-cms/utils/medicare.yml",
        help="Path to the YAML configuration file containing harmonization rules."
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="../harmonization-cms/output",
        help="Path to the directory where output files will be saved."
    )
    parser.add_argument(
        "--table_to_run",
        type=str,
        default="ps",
        help="Name of the specific table to process (optional)."
    )
    parser.add_argument(
        "--year_to_run",
        type=int,
        default=2011,
        help="Year to process (optional)."
    )

    args = parser.parse_args()

    with open(args.config_path, 'r') as file:
        config = yaml.safe_load(file)
        print("Loaded config")

    process_tables(
        config=config,
        output_path=args.output_path,
        table_to_run=args.table_to_run,
        year_to_run=args.year_to_run
    )

if __name__ == "__main__":
    main()