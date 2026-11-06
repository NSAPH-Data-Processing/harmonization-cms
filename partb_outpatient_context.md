# Medicare Part B Outpatient Harmonization Context

## 1. Objective and scope

Use this file to guide an AI coding agent developing and reviewing the per-table harmonization YAML requested by [issue #39](https://github.com/NSAPH-Data-Processing/harmonization-cms/issues/39). The target deliverable is a reviewed table YAML and any necessary runner, batch, or datapath configuration. This context is guidance; it is not the mapping YAML and does not establish that any mapping or validation has passed.

The requested file-year scope is exactly **2016, 2017, and 2018**. Do not expand the years or substitute claim year or service-date year for file year without an explicit, documented decision. First identify what year the source files represent.

All data-dependent processing and QC for this work must run on the FISMA-compliant HPC cluster with approved data access. Keep credentials, cluster secrets, private data, PHI, and row-level records out of this repository context.

## 2. Repository conventions

This repository harmonizes CMS-derived parquet data with DuckDB and declarative YAML. Read `README.md`, `harmonizecms/harmonizer.py`, `harmonizecms/io.py`, and `harmonizecms/qc.py` before implementation. Follow the closest examples:

- `conf/datasets/medicare_medpar/ip.yaml`
- `conf/datasets/medicare_partd/partd_pde.yaml`
- `conf/datasets/medicare_mbsf/ps.yaml`

Table YAMLs live in `conf/datasets/<dataset>/` and define `table_name`, optional `description`, `path_pattern`, and ordered `columns`; `crosswalks` are optional. Scalar columns use `source`, `type`, and optional `cast`. The harmonizer selects the first source candidate present in the schema, so candidate order changes behavior and is allowed only when the candidates are documented equivalents. Cast rules are keyed by DuckDB input type with `"*"` as a fallback. Array columns use `kind: array`, `source_array` and/or `source_component_sets` (or `source_components`), and optional `element_cast`/`cast`. Use complete, ordered component lists for fixed-width arrays. Confirm actual syntax and behavior in the implementation; do not add unsupported keys or assume behavior not documented there.

The table mapping is separate from execution configuration:

- `conf/config.yaml` maps runner table names to table YAML paths and supplies paths for a single-table/year run.
- `conf/snakemake.yaml` supplies input/output paths, table names, and years for a batch run; the `Snakefile` runs harmonization followed by QC.
- `conf/datapaths/<profile>.yaml` describes local links and output folders. Creating those paths is a separate setup step (`python -m harmonizecms.create_dir_paths`); it does not validate or configure source mappings.

Propose the dataset directory, table name, and YAML path before editing. Keep the name consistent in the YAML `table_name`, `conf/config.yaml` table mapping, Snakemake table list, and output paths as applicable. Add or change batch/datapath configuration only when needed for the requested workflow. Any batch years must be exactly `[2016, 2017, 2018]`.

## 3. Source data specification — owner confirmation required

Do not map fields based on the label “Part B Outpatient.” The project owner must identify the exact CMS product and claim setting: for example, Carrier/Physician and Supplier claims, institutional Outpatient claims, or another explicitly named product. These products and their grains are not interchangeable.

Before proposing mappings, record:

- CMS product, file/table names, and unit of observation (claim, claim line, or other), including whether multiple files represent different record types.
- For **each** of 2016, 2017, and 2018: available files and columns, an approved schema inventory or safe de-identified schema extract, and authoritative CMS data dictionary/variable documentation references.
- Secure HPC input file layout and path pattern, using an owner-approved non-sensitive path or placeholder such as `<approved_HPC_input_root>`. Do not guess a path pattern.
- Whether inputs are raw, preprocessed, or already parquet. Describe preprocessing that changes names, types, row grain, date encodings, or code systems.
- The year field and its meaning (file year, claim year, service year, etc.) and known gaps or differences in coverage.

No row-level data or PHI belongs in this file. If source schemas or authoritative definitions are not available, list them as blockers rather than infer them.

### ResDAC documentation and data access

Use ResDAC as the starting point for public CMS file descriptions and variable documentation. First identify the exact product from its file page, then open that product's **Data Documentation** page and follow variable-definition links. Use the associated CCW codebook/record layout when a field's storage type, values, sentinel codes, file placement, or year-specific behavior is not clear from the variable page. Record the page title, URL, SAS name, definition, and access date in the mapping ledger. Prefer a variable page or codebook entry for field-level claims over a general file overview.

For example, the ResDAC pages for [Original Medicare (FFS) Carrier](https://resdac.org/cms-data/files/carrier-ffs) and its [Data Documentation](https://resdac.org/cms-data/files/carrier-ffs/data-documentation) describe the professional Carrier claim product and its base-claim, line, and other file sections. These pages are discovery references, not proof that this project uses Carrier data. Compare them with the separate [Original Medicare (FFS) Outpatient](https://resdac.org/cms-data/files/op-ffs) product page and its [Data Documentation](https://resdac.org/cms-data/files/op-ffs/data-documentation) when the project owner uses the term “outpatient”; confirm the intended product with the owner before selecting either mapping.

Public ResDAC descriptions and dictionaries do **not** grant access to Research Identifiable File (RIF) data. Do not attempt to obtain, download, or bypass access to restricted records. Use source files only when the project owner confirms that the data and years are covered by the applicable CMS approval/DUA and that the agent's work is authorized in the FISMA-compliant HPC environment. The context may provide an owner-approved HPC path placeholder and schema-only inventory. If authorization, approved HPC access, or the requested year/file coverage is unclear, stop and report the exact blocker; do not probe restricted locations. See ResDAC's [RIF request guidance](https://resdac.org/research-identifiable-files-rif-requests) for access-process context.

## 4. Required metrics and output schema — owner confirmation required

The project owner must list the exact output variables and define each. Do not infer an “all useful fields” scope from the product name. For every requested output, maintain a mapping ledger with:

| Item to specify | Required detail |
| --- | --- |
| Output | Canonical column name, business definition, and observation level |
| Role | Raw field, derived metric, or identifier; required, optional, or intentionally omitted |
| Source | CMS variable(s), authoritative reference, and availability in each of 2016–2018 |
| Representation | Expected type, units, code system/valid values, and sentinel/null conventions |
| Derivation | Formula or normalization and date basis (file year, claim year, service date, etc.) |
| Unavailable year | Whether to emit null, omit the column, or stop for owner review; do not silently change meaning |

Explicitly settle whether the output needs beneficiary identifiers; claim or line identifiers; service dates; diagnosis/procedure fields; provider fields; and payment/utilization measures. Include these only when the owner requests them and supplies definitions. Document privacy-safe handling for any identifiers in the approved HPC workflow.

## 5. Mapping and YAML instructions

1. Confirm source product, grain, input layout, year definition, schemas, data dictionaries, and requested outputs before writing mappings.
2. Inspect all three years independently. The harmonizer uses schema presence to resolve scalar source candidates and array inputs; do not assume columns or types are stable across years. Check implementation behavior for schema discovery and multi-file reads.
3. Map only evidence-backed fields. Cite the CMS documentation and schema evidence in the mapping ledger; keep proposed mappings distinct from verified mappings. Preserve ordered source alternatives only when equivalence is documented.
4. Choose explicit output types and casts where input types can vary. Specify date parsing, code normalization, sentinel handling, and array construction only from confirmed source representations. Ensure fixed-width arrays have a full, ordered component list and stable length across supported layouts.
5. For each output, record its per-year source name(s), observed input type, cast or derivation, definition, reference, and supporting schema evidence. Review the generated mapping report against this ledger.
6. Add only supported YAML fields and documented runner/config changes. A missing required source or unresolved difference in meaning is a question/blocker, not a reason to fabricate a mapping.

## 6. Workflow and validation

Prepare and review code/config changes locally. Do not run data-dependent harmonization, inspect row-level data, or run QC against restricted inputs locally; execute those steps only on the FISMA-compliant HPC cluster with approved access. Use the repository's documented `run_harmonizer.py`, `run_qc.py`, and Snakemake patterns, substituting only approved secure paths. For batch execution, confirm the configured table list and the exact year list before running.

Expected artifacts, as documented in `README.md`, include `<table>_<year>.parquet`, `<table>_<year>.run.json`, and `<table>_<year>.qc/` with `qc.json`, `mapping.csv`, `schema_before.json`, `schema_after.json`, and `sample.parquet`. Treat samples as sensitive unless an approved review process says otherwise; do not copy them into this repository or context.

For each of 2016, 2017, and 2018, capture and review:

- Input file discovery and schema/column availability, plus any year-specific gaps.
- YAML parsing and runner/config loading evidence.
- The mapping report and its agreement with the mapping ledger.
- Output schema and row count.
- QC status, warnings, errors, and per-column null counts; investigate expected-versus-unexpected missingness.
- Review of representative aggregates or a sample only through an approved, privacy-safe HPC process.

Do not say validation passed until results are available and reviewed. In project notes, record assumptions, unresolved items and owners, commands actually run, environment and repository revision, per-year evidence, and validation results. Use placeholders in reusable commands; never include credentials or private cluster paths.

## 7. Open questions and stop condition

Resolve and record these with the project owner or authoritative source before mapping:

- Which exact CMS product, claim setting, file/table types, and record grain are in scope?
- What approved HPC input layout and file pattern applies to each of 2016–2018?
- What does the year directory/field mean, and are there coverage gaps or preprocessing differences?
- Where are the per-year schemas and authoritative dictionaries, including code and sentinel definitions?
- What exact output columns/metrics are required, and what are their definitions, types, date basis, missingness rules, and requiredness?
- What target dataset/table name and config profile should be used?

**Stop before generating or asserting mappings** if product identity, grain, source availability, a requested field's definition, or the meaning of a year-specific source candidate remains unsupported. Return the specific question, evidence needed, and owner/source who can resolve it. Clearly label all unconfirmed decisions as assumptions or proposals; do not claim YAML, harmonization, or QC validation has already succeeded.
