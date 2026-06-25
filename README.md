# Harmonization-CMS (`harmonizecms`)

This repository harmonizes CMS-derived parquet datasets into consistent table outputs using **DuckDB** and **config-driven column mapping**. It supports running a single table/year directly, and running many table/years via **Snakemake**. It also generates lightweight QA/QC artifacts (schema + mapping + null counts + timing), including **optional crosswalk QA/QC**.

## Purpose

- Harmonize “same concept, different raw column names/types” into a single consistent output schema per table.
- Produce deterministic outputs per `(table, year)`.
- Provide transparent QA/QC outputs to document:
  - which raw columns were used for each output variable
  - input and output data types
  - basic completeness (null counts)
  - run time and basic run metadata
  - (optional) crosswalk join coverage / match rates

## Repository structure

```text
.
├── conf/
│   ├── config.yaml                    # Hydra config used by run_harmonizer.py and run_qc.py
│   ├── snakemake.yaml                 # Tables/years + paths for Snakemake
│   ├── datapaths/
│   │   └── <profile>.yaml             # Folder/symlink layout used by create_dir_paths.py
│   └── datasets/
│       └── <dataset>/
│           ├── ps.yaml                # Per-table harmonization rules (+ optional crosswalks)
│           ├── ip.yaml
│           └── ...
├── harmonizecms/
│   ├── harmonizer.py                  # Core harmonization logic (DuckDB) + optional crosswalk joins
│   ├── qc.py                          # QC generation (DuckDB) + optional crosswalk QC
│   ├── io.py                          # Input parquet discovery
│   ├── create_dir_paths.py            # Create local data links + folder structure
│   └── __init__.py
├── run_harmonizer.py                  # Hydra runner: harmonize one table/year
├── run_qc.py                          # Hydra runner: QC one table/year
├── Snakefile                          # Orchestrate harmonize -> QC for table/year matrix
├── data/                              # Local data root (created/used by create_dir_paths)
├── logs/                              # Snakemake logs (and Hydra run dirs if configured via Snakefile)
├── pyproject.toml
└── README.md
```

## Setup

### 1) Activate environment

Example (lab environment):

```bash
micromamba activate nsaph_data_cms_data_prep
```

Or install locally:

```bash
pip install -e .
```

### 2)  Data paths setup 

This repo creates a local `data/<profile>/...` directory with symlinks and output subfolders.

Example config: `conf/datapaths/medicare_red.yaml` (or `medicaid_max_red.yaml`)

Run:

```bash
python -m harmonizecms.create_dir_paths
```

This uses `conf/config.yaml` defaults and will create something like:

```text
data/
  <profile>/
    input/                             # symlinks and/or subfolders created by datapaths
      max_files -> /.../your_raw_data_root
      crosswalks -> /.../your_crosswalks_root
    output/
      ps/
      ip/
      mbsf_d/
```

You can then run harmonization/QC using:
- `paths.basepath=data/<profile>/input/max_files`
- `paths.output_path=data/<profile>/output/<table>`
- `paths.crosswalks_path=data/<profile>/input/crosswalks` (only needed if your table yaml uses crosswalks)

> Note: datapaths is a **separate step** and is not run automatically by Snakemake or the runners.

## How tables are built

For a given `(table, year)` run:

1. The per-table YAML defines one or more `path_pattern` entries that expand to a set of parquet “part” files under the input basepath.
2. `harmonizecms` loads the *schema* of the first parquet file and uses it to decide which raw columns exist.
3. For each output variable in `columns`, the system:
   - picks the first matching raw column from the `source` list (scalar fields), or
   - builds an array from either:
     - a pre-packed array/string column (`source_array`), or
     - a chosen component schema (`source_component_sets`)
4. **Optional:** if the table yaml includes `crosswalks`, the harmonizer will join one or more crosswalk tables in DuckDB to add derived columns.
5. It executes one DuckDB query to read all matching parquet parts and write a single harmonized parquet.

This approach keeps the mapping logic declarative in YAML and uses DuckDB for fast parquet reads, casting, joining, and writing.

## How to write a per-table YAML

Per-table configs live under `conf/datasets/<dataset>/`. Each YAML defines:

- `table_name`: output table name (used in filenames)
- `description`: free text (optional)
- `path_pattern`: string or list of strings with placeholders `{basepath}` and `{year}`
- `columns`: ordered list of output variables and how to build each
- `crosswalks` (optional): post-harmonization joins to enrich the output

### Minimal example (scalar-only)

```yaml
table_name: example
description: "Example table"
path_pattern:
  - "{basepath}/{year}/example*/part-*.parquet"

columns:
  - bene_id:
      source: 
        - bene_id 
        - intbid
      type: VARCHAR
      cast:
        "*": "TRY_CAST({column_name} AS VARCHAR)"

  - year:
      source: 
        - rfrnc_yr
        - enrolyr
      type: INT
      cast:
        "*": "TRY_CAST({column_name} AS INT)"
```

### Scalar columns

A scalar column chooses the **first** `source` candidate found in the input schema for that year:

```yaml
  - zip:
      source: 
        - bene_zip
        - bene_zip_cd
        - zip
        - zipcode
      type: INT
      cast:
        "*": "TRY_CAST({column_name} AS INT)"
```

#### Casting
`cast` is a dict keyed by input type (as reported by DuckDB `DESCRIBE`), plus `"*"` as a fallback.

Example:

```yaml
  - dob:
      type: DATE
      source:
        - bene_dob
        - dob
      cast:
        VARCHAR: "CASE WHEN {column_name} ~ '^[0-9]{8}$' THEN ... END"
        DOUBLE:  "CASE WHEN {column_name} >= 19000000 THEN ... END"
        "*":      "TRY_CAST({column_name} AS DATE)"
```

### Array columns (monthly indicators, etc.)

To produce arrays use:

- `kind: array`
- `source_array` ( pre-packed array/string columns)
- `source_component_sets` (one or more alternative schemas)
- `element_cast` (applied to each component when building arrays)
- `cast` for handling the case when `source_array` is a packed string that must be split into elements

Example:

```yaml
  - hmo_indicators:
      kind: array
      type: "VARCHAR[]"

      source_array:
        - hmoind
        - hmoind12

      source_component_sets:
        - [hmoind01, hmoind02, hmoind03, hmoind04, hmoind05, hmoind06, hmoind07, hmoind08, hmoind09, hmoind10, hmoind11, hmoind12]
        - [bene_hmo_ind_01, bene_hmo_ind_02, bene_hmo_ind_03, bene_hmo_ind_04, bene_hmo_ind_05, bene_hmo_ind_06, bene_hmo_ind_07, bene_hmo_ind_08, bene_hmo_ind_09, bene_hmo_ind_10, bene_hmo_ind_11, bene_hmo_ind_12]

      element_cast: "CAST({column_name} AS VARCHAR)"

      cast:
        VARCHAR: "ARRAY(SELECT CAST(ch AS VARCHAR) FROM UNNEST(STRING_SPLIT({column_name}, '')) AS t(ch))"
        "*": "{column_name}"
```

**Important:** `source_component_sets` should contain complete component lists (e.g., exactly 12 month variables) so the array length is stable.

## Crosswalks (optional)

Crosswalks let you enrich harmonized outputs with small lookup tables (parquet or csv). Crosswalk joins are performed **inside DuckDB** after base harmonization.

### Enabling crosswalks

1) Provide a `paths.crosswalks_path` (recommended via datapaths):

- `data/<profile>/input/crosswalks`

2) Add `crosswalks:` to your table yaml.

### Crosswalk YAML syntax

```yaml
crosswalks:
  - name: state_fips
    path: "{crosswalks_path}/state_fips.parquet"
    format: parquet
    join:
      how: left
      on:
        state: state_cd
    select:
      add:
        state_alpha: fips
        state_name: state_name
```

Meaning:
- reads the crosswalk table from `path` (`format: parquet` or `csv`)
- joins using the `on:` mapping:
  - left side uses **harmonized** output column names (from `columns:`)
  - right side uses crosswalk column names
- adds selected crosswalk columns to the output, renamed as specified by `select.add`

Supported patterns:
- **static**: `path: "{crosswalks_path}/foo.parquet"`
- **year-specific**: `path: "{crosswalks_path}/foo/{year}.parquet"`
- **effective-dated**: include `join.effective_date`:

```yaml
join:
  how: left
  on:
    taxonomy: taxonomy
  effective_date:
    left_date: srvc_bgn_dt
    start_col: start_date
    end_col: end_date
```

## How to run (single table/year)

### Harmonize one table/year

```bash
python run_harmonizer.py \
  table=ps year=2011 \
  paths.basepath="data/<profile>/input/max_files" \
  paths.output_path="data/<profile>/output/ps"
```

If your table yaml uses crosswalks:

```bash
python run_harmonizer.py \
  table=ps year=2011 \
  paths.basepath="data/<profile>/input/max_files" \
  paths.crosswalks_path="data/<profile>/input/crosswalks" \
  paths.output_path="data/<profile>/output/ps"
```

This writes:
- `data/<profile>/output/<table>/<table>_<year>.parquet`
- `data/<profile>/output/<table>/<table>_<year>.run.json`

### QC one table/year (separate step)

```bash
python run_qc.py \
  table=ps year=2011 \
  paths.basepath="data/<profile>/input/max_files" \
  paths.output_path="data/<profile>/output/ps"
```

If your table yaml uses crosswalks, pass `paths.crosswalks_path` so QC can locate the crosswalk files:

```bash
python run_qc.py \
  table=ps year=2011 \
  paths.basepath="data/<profile>/input/max_files" \
  paths.crosswalks_path="data/<profile>/input/crosswalks" \
  paths.output_path="data/<profile>/output/ps"
```

This writes a QC directory:
- `data/<profile>/output/<table>/<table>_<year>.qc/`

Containing:
- `qc.json`
- `mapping.csv`
- `schema_before.json`
- `schema_after.json`
- `sample.parquet`

QC is **never a hard fail**: errors are recorded in `qc.json`.

## How to run (batch via Snakemake)

Edit `conf/snakemake.yaml` to set:
- input basepath
- output path
- (optional) crosswalks path
- list of tables
- list of years

Run:

```bash
snakemake -cores 1
```

Dry-run:

```bash
snakemake --dry-run
```

## QC outputs explained

For each `(table, year)`, QC produces:

### `<table>_<year>.qc/qc.json`
Summary + metadata:

- `status`: `"ok"` or `"error"`
- `row_count`, `n_columns`
- `null_counts`: `{column -> null_count}`
- `warnings`: non-fatal issues (e.g., missing expected output columns)
- `errors`: exceptions encountered during QC
- `harmonize_run`: contents of `<table>_<year>.run.json` if present
- `crosswalk_qc` (optional): per-crosswalk match coverage metrics

### Crosswalk QC (`crosswalk_qc`)
If the table yaml includes `crosswalks`, QC adds:
- crosswalk file existence checks (warn if missing)
- join key presence in output schema
- for each `select.add` column:
  - `null_fraction_overall`
  - `unmatched_fraction_among_key_present` (keys present but added column is NULL)

### `<table>_<year>.qc/mapping.csv`
“Condensed mapping” report.

### `schema_before.json` and `schema_after.json`
DuckDB `DESCRIBE` snapshots.

### `sample.parquet`
First `N` rows (default 100) from the output parquet.

## Configuration

### `conf/config.yaml` (Hydra runner config)

Key fields:
- `table`, `year`
- `paths.basepath`
- `paths.output_path`
- `paths.crosswalks_path` (optional)
- `tables`: mapping of `table -> per-table yaml path`

### `conf/snakemake.yaml`

Defines batch runs:
- `paths.basepath`
- `paths.output_path`
- `paths.crosswalks_path` (optional)
- `tables` list
- `years` list
