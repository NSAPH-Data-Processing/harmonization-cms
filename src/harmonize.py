import argparse
import yaml
import duckdb
import glob
import os
import re
import pandas as pd

#TODO: handle cases when multiple parts for raw file. We want to concatenate them
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

    # Get the schema of the first Parquet file
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

        # Handle dynamic columns like diag[{m}=1:25]
        if "{m}" in col_name:
            base_name, range_part = col_name.split("[{m}=")
            range_start, range_end = map(int, range_part.rstrip("]").split(":"))
            for m in range(range_start, range_end + 1):
                dynamic_col_name = f"{base_name}{m}"
                dynamic_sources = [src.replace("{m}", str(m)) for src in source_expr]
                selected_source = None

                for candidate in dynamic_sources:
                    if candidate.lower() in file_schema:
                        selected_source = candidate
                        break

                if selected_source:
                    cast_template = cast_dict.get("*", "{column_name}")
                    expr = cast_template.format(column_name=selected_source)
                    columns.append(f"{expr} AS {dynamic_col_name}")
                else:
                    print(f"'{dynamic_col_name}' - No valid source found in schema. Creating column with NULL.")
                    columns.append(f"NULL AS {dynamic_col_name}")
            continue
        # elif isinstance(source_expr, list) and any("{m}" in src for src in source_expr):
        #     # Handle cases where source_expr contains dynamic columns like hmoind{m}
        #     dynamic_columns = []
        #     for src in source_expr:
        #         if "{m}" in src:
        #             base_name, range_part = col_name.split("[{m}=")
        #             range_start, range_end = map(int, range_part.rstrip("]").split(":"))
        #             for m in range(range_start, range_end + 1):
        #                 dynamic_source = src.replace("{m}", str(m))
        #                 if dynamic_source.lower() in file_schema:
        #                     dynamic_columns.append(dynamic_source)
        #             else:
        #                 dynamic_source = src.replace("{m}", str(m))
        #                 print(f"'{dynamic_source}' - No valid source found in schema. Adding NULL.")
        #                 dynamic_columns.append("NULL")
        #         elif src.lower() in file_schema:
        #             # Handle cases where the source is already in array format
        #             dynamic_columns.append(src)
            
        #     if dynamic_columns:
        #         cast_template = cast_dict.get("*", "{columns}")
        #         expr = cast_template.format(columns=", ".join(dynamic_columns))
        #         columns.append(f"{expr} AS {col_name}")
        #     else:
        #         print(f"'{col_name}' - No valid sources found for dynamic array. Creating column with NULL.")
        #         columns.append(f"NULL AS {col_name}")
        #     continue
        
        elif isinstance(source_expr, list) and any("{m}" in src for src in source_expr):
            dynamic_columns = []

            for src in source_expr:
                if "{m}" in src:

                    # Fetch month values from the YAML config, falling back to a default if not defined
                    month_values = col_def.get("m", [str(i).zfill(2) for i in range(1, 13)])

                    dynamic_columns.extend(
                        src.replace("{m}", str(m)) if src.replace("{m}", str(m)).lower() in file_schema else "NULL"
                        for m in month_values
                    )
                elif src.lower() in file_schema:
                    dynamic_columns.append(src)

            # Remove redundant NULLs if any valid columns exist
            valid_columns = [col for col in dynamic_columns if col != "NULL"]
            columns.append(
                f"{cast_dict.get('*', '{columns}').format(columns=', '.join(valid_columns))} AS {col_name}" 
                if valid_columns 
                else f"NULL AS {col_name}"
            )
            continue


        # Handle regular columns
        if isinstance(source_expr, list):
            for candidate in source_expr:
                if candidate.lower() in file_schema:
                    selected_source = candidate
                    break
        elif isinstance(source_expr, str):
            if source_expr.lower() in file_schema:
                selected_source = source_expr

        if not selected_source:
            print(f"'{col_name}' - No valid source found in schema. Creating column with NULL.")
            columns.append(f"NULL AS {col_name}")
            continue

        col_type = file_schema.get(selected_source.lower(), col_def.get("type", "").upper())
        cast_template = cast_dict.get(col_type) or cast_dict.get("*") or "{column_name}"
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

    years = [year_to_run]

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

