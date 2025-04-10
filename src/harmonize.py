mport argparse
import yaml
import duckdb
import glob
import os
import re

config_path = '/n/dominici_nsaph_l3/Lab/data_processing/shreya_synthetic-cms/synthetic-cms/utils/medicare.yml'
# read in yaml containing harmonization rules
with open(config_path, 'r') as file:
    config = yaml.safe_load(file)

def get_parquet_files(basepath, year, path_patterns):
    all_parquet_files = []

    for pattern in path_patterns:
        pattern = pattern.replace("{basepath}", basepath).replace("{year}", str(year))
        dir_pattern = os.path.dirname(pattern)
        matched_dirs = [d for d in glob.glob(dir_pattern) if os.path.isdir(d)]

        for directory in matched_dirs:
            all_parquet_files.extend(sorted(glob.glob(os.path.join(directory, "part-*.parquet"))))

    return all_parquet_files

def construct_query(table_config, parquet_files):
    columns = []
     
    query = f"SELECT * FROM read_parquet('{parquet_files[0]}') LIMIT 0"
    df = duckdb.query(query).to_df()
    
    file_schema = set(col.lower() for col in df.columns)

    for col in table_config.get("columns", []):
        # --- Case 0: Simple string column name ---
        if not isinstance(col, dict):
            if file_schema is None or col.lower() in file_schema:
                columns.append(col)
            else:
                print(f"Skipping column '{col}' (not found in schema).")
            continue

        col_name = list(col.keys())[0]
        col_def = col[col_name]

        if not col_def:
            print(f"Warning: column definition for '{col_name}' is None. Skipping.")
            continue

        cast_dict = col_def.get("cast", {})
        col_type = str(col_def.get("type", "")).lower()

        # --- Case 1: Multi-source column with 'm' expansions ---
        if isinstance(col_def.get("source"), list) and "m" in col_def:
            expanded_sources = [
                col_def["source"][0].replace("{m}", m) for m in col_def["m"]
            ]

            # Filter sources to those that actually exist in file_schema
            expanded_sources = [
                src for src in expanded_sources
                if file_schema is None or src.lower() in file_schema
            ]

            if not expanded_sources:
                print(f"Skipping '{col_name}': none of the expanded sources found in schema.")
                continue

            cast_template = cast_dict.get("*", "[{columns}]")
            cast_expr = cast_template.format(columns=", ".join(expanded_sources))
            columns.append(f"{cast_expr} AS {col_name}")
            continue

        # --- Case 2: Column with single or multiple source options ---
        source_expr = col_def.get("source")
        selected_source = None

        if isinstance(source_expr, list):
            for candidate in source_expr:
                if file_schema is None or candidate.lower() in file_schema:
                    selected_source = candidate
                    break
        elif isinstance(source_expr, str):
            # Allow passthrough SQL expressions even if not in schema
            if any(tok in source_expr.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"]):
                selected_source = source_expr
            elif file_schema is None or source_expr.lower() in file_schema:
                selected_source = source_expr

        if not selected_source:
            print(f"Skipping '{col_name}': no valid source found in schema.")
            continue

        cast_template = (
            cast_dict.get(col_type) or
            cast_dict.get("*") or
            "{column_name}"
        )

        if any(tok in selected_source.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"]):
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
     
    query = f"SELECT * FROM read_parquet('{parquet_files[0]}') LIMIT 0"
    df = duckdb.query(query).to_df()
    
    file_schema = set(col.lower() for col in df.columns)

    for col in table_config.get("columns", []):
        # --- Case 0: Simple string column name ---
        if not isinstance(col, dict):
            if file_schema is None or col.lower() in file_schema:
                columns.append(col)
            else:
                print(f"Skipping column '{col}' (not found in schema).")
            continue

        col_name = list(col.keys())[0]
        col_def = col[col_name]

        if not col_def:
            print(f"Warning: column definition for '{col_name}' is None. Skipping.")
            continue

        cast_dict = col_def.get("cast", {})
        col_type = str(col_def.get("type", "")).lower()

        # --- Case 1: Multi-source column with 'm' expansions ---
        if isinstance(col_def.get("source"), list) and "m" in col_def:
            expanded_sources = [
                col_def["source"][0].replace("{m}", m) for m in col_def["m"]
            ]

            # Filter sources to those that actually exist in file_schema
            expanded_sources = [
                src for src in expanded_sources
                if file_schema is None or src.lower() in file_schema
            ]

            if not expanded_sources:
                print(f"Skipping '{col_name}': none of the expanded sources found in schema.")
                continue

            cast_template = cast_dict.get("*", "[{columns}]")
            cast_expr = cast_template.format(columns=", ".join(expanded_sources))
            columns.append(f"{cast_expr} AS {col_name}")
            continue

        # --- Case 2: Column with single or multiple source options ---
        source_expr = col_def.get("source")
        selected_source = None

        if isinstance(source_expr, list):
            for candidate in source_expr:
                if file_schema is None or candidate.lower() in file_schema:
                    selected_source = candidate
                    break
        elif isinstance(source_expr, str):
            # Allow passthrough SQL expressions even if not in schema
            if any(tok in source_expr.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"]):
                selected_source = source_expr
            elif file_schema is None or source_expr.lower() in file_schema:
                selected_source = source_expr

        if not selected_source:
            print(f"Skipping '{col_name}': no valid source found in schema.")
            continue
        #TODO: Fix casting, without basis of template 
        cast_template = (
            cast_dict.get(col_type) or
            cast_dict.get("*") or
            "{column_name}"
        )

        if any(tok in selected_source.upper() for tok in ['(', ')', 'CASE', 'SELECT', '"', "'"]):
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
    import duckdb
    import os

    conn = duckdb.connect(database=':memory:')
    basepath = config['basepath']
    os.makedirs(output_path, exist_ok=True)

    # List of years to process
    if year_to_run is not None:
        years = [int(year_to_run)]
    else:
        years = [int(d) for d in os.listdir(basepath) if d.isdigit()]
        years.sort()

    # Process only the specified table or all tables
    for table_name, table_config in config['tables'].items():
        if table_to_run is not None and table_name != table_to_run:
            continue  # Skip tables not specified

        table_config['name'] = table_name

        for year in years:
            parquet_files = get_parquet_files(basepath, year, table_config['path_pattern'])
            if parquet_files:
                print(f"Processing table: {table_name}, year: {year}")
                print(f"Input files: {parquet_files}")
                query = construct_query(table_config, parquet_files)
                conn.execute(query)
                output_file = os.path.join(output_path, f"{table_name}_{year}.parquet")
                conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{output_file}' (FORMAT 'parquet')")
                print(f"Saved output to: {output_file}")

    conn.close()

output_path = "/n/dominici_nsaph_l3/Lab/data_processing/shreya_synthetic-cms/synthetic-cms/output"
process_tables(config, output_path, table_to_run= "ps", year_to_run= 2012)