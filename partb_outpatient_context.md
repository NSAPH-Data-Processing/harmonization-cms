# Medicare Part B Outpatient Harmonization Context

## 1. Objective and scope

Use this file to guide an AI coding agent developing and reviewing the Medicare Part B outpatient harmonization configuration requested by [issue #39](https://github.com/NSAPH-Data-Processing/harmonization-cms/issues/39). The workflow must produce two YAML files inside this repository's `conf/` directory:

- Datapaths profile: `conf/datapaths/medicare_partb.yaml`.
- Per-table dataset mapping: `conf/datasets/medicare_partb/partb.yaml` (with `table_name: partb`).

These are separate files with separate purposes: the datapaths YAML describes local input/output folders and links; the dataset YAML describes the source path pattern and output column mappings. Do not put either deliverable at the repository root or outside `conf/`. This context is guidance; it is not either YAML file and does not establish that any mapping or validation has passed.

The requested file-year scope is exactly **2016, 2017, and 2018**. Do not expand the years or substitute claim year or service-date year for file year without an explicit, documented decision. First identify what year the source files represent.

All data-dependent processing and QC for this work must run on the FISMA-compliant HPC cluster with approved data access. Keep credentials, cluster secrets, private data, PHI, and row-level records out of this repository context.

## 2. Repository conventions

This repository harmonizes CMS-derived parquet data with DuckDB and declarative YAML. Read `README.md`, `harmonizecms/harmonizer.py`, `harmonizecms/io.py`, and `harmonizecms/qc.py` before implementation. Follow the closest examples:

- `conf/datasets/medicare_medpar/ip.yaml`
- `conf/datasets/medicare_partd/partd_pde.yaml`
- `conf/datasets/medicare_mbsf/ps.yaml`

Table YAMLs live in `conf/datasets/<dataset>/` and define `table_name`, optional `description`, `path_pattern`, and ordered `columns`; `crosswalks` are optional. Scalar columns use `source`, `type`, and optional `cast`. The harmonizer selects the first source candidate present in the schema, so candidate order changes behavior and is allowed only when the candidates are documented equivalents. Cast rules are keyed by DuckDB input type with `"*"` as a fallback. Array columns use `kind: array`, `source_array` and/or `source_component_sets` (or `source_components`), and optional `element_cast`/`cast`. Use complete, ordered component lists for fixed-width arrays. Confirm actual syntax and behavior in the implementation; do not add unsupported keys or assume behavior not documented there.

The two required YAML deliverables are separate from each other and from execution configuration:

- `conf/config.yaml` maps runner table names to table YAML paths and supplies paths for a single-table/year run.
- `conf/snakemake.yaml` supplies input/output paths, table names, and years for a batch run; the `Snakefile` runs harmonization followed by QC.
- `conf/datapaths/medicare_partb.yaml` describes local links and output folders. Creating those paths is a separate setup step (`python -m harmonizecms.create_dir_paths`); it does not validate or configure source mappings.
- `conf/datasets/medicare_partb/partb.yaml` defines the `partb` table mapping and belongs in the `medicare_partb` dataset directory.

Draft both required deliverables under the paths above using the evidence available, and mark unresolved source details in the files. Finalize and run them only after owner confirmation of the source product/grain, approved input layout, and per-year schemas. Keep `partb` consistent in the dataset YAML `table_name`, `conf/config.yaml` table mapping, Snakemake table list, and output paths as applicable. Update runner or batch configuration only when needed to make the requested workflow usable; those files are not substitutes for either required deliverable. Any batch years must be exactly `[2016, 2017, 2018]`.

## 3. Source data specification — owner confirmation required

The supplied requested-field list is a strong match for the **Original Medicare (Fee-for-Service) Outpatient base claim file**: ResDAC's [Outpatient data documentation](https://resdac.org/cms-data/files/op-ffs/data-documentation) lists BENE_ID, CLM_ID, CLM_FROM_DT, CLM_THRU_DT, CLM_FAC_TYPE_CD, CLM_FREQ_CD, PTNT_DSCHRG_STUS_CD, PRNCPAL_DGNS_CD, and ICD_DGNS_CD1 through ICD_DGNS_CD25 together in its base claim section. This identifies a proposed product and claim-level grain from the field list, but the project owner must confirm that this is the intended source; do not silently substitute Carrier/Physician and Supplier claims or Medicare Advantage encounter data.

The same documentation lists revenue-center rows separately, linked by BENE_ID and CLM_ID. The requested fields are in the base claim section, so the proposed output is claim-level, not revenue-center/claim-line-level. Confirm this with the owner before treating the proposal as final.

Variable-level references reviewed on 2026-11-23: [BENE_ID](https://resdac.org/cms-data/variables/encrypted-ccw-beneficiary-id), [CLM_ID](https://resdac.org/cms-data/variables/claim-id), [CLM_FROM_DT](https://resdac.org/cms-data/variables/claim-date-ffs), [CLM_THRU_DT](https://resdac.org/cms-data/variables/claim-through-date-ffs), [CLM_FAC_TYPE_CD](https://resdac.org/cms-data/variables/claim-facility-type-code-ffs), [CLM_FREQ_CD](https://resdac.org/cms-data/variables/claim-frequency-code-ffs), [PTNT_DSCHRG_STUS_CD](https://resdac.org/cms-data/variables/patient-discharge-status-code-ffs), [PRNCPAL_DGNS_CD](https://resdac.org/cms-data/variables/claim-principal-diagnosis-code-ffs), and [ICD_DGNS_CD1](https://resdac.org/cms-data/variables/claim-diagnosis-code-i-ffs). ResDAC describes the principal diagnosis as also stored in ICD_DGNS_CD1 and documents up to 25 diagnosis slots for institutional claims.

Before finalizing mappings, record:

- CMS product, file/table names, and unit of observation (claim, claim line, or other), including whether multiple files represent different record types.
- For **each** of 2016, 2017, and 2018: available files and columns, an approved schema inventory or safe de-identified schema extract, and authoritative CMS data dictionary/variable documentation references.
- Secure HPC input file layout and path pattern, using an owner-approved non-sensitive path or placeholder such as `<approved_HPC_input_root>`. Do not guess a path pattern.
- Whether inputs are raw, preprocessed, or already parquet. Describe preprocessing that changes names, types, row grain, date encodings, or code systems.
- The year field and its meaning (file year, claim year, service year, etc.) and known gaps or differences in coverage.

No row-level data or PHI belongs in this file. If source schemas or authoritative definitions are not available, list them as blockers rather than infer them.

### ResDAC documentation and data access

Use ResDAC as the starting point for public CMS file descriptions and variable documentation. First identify the exact product from its file page, then open that product's **Data Documentation** page and follow variable-definition links. Use the associated CCW codebook/record layout when a field's storage type, values, sentinel codes, file placement, or year-specific behavior is not clear from the variable page. Record the page title, URL, SAS name, definition, and access date in the mapping ledger. Prefer a variable page or codebook entry for field-level claims over a general file overview.

For example, the ResDAC pages for [Original Medicare (FFS) Carrier](https://resdac.org/cms-data/files/carrier-ffs) and its [Data Documentation](https://resdac.org/cms-data/files/carrier-ffs/data-documentation) describe the professional Carrier claim product and its base-claim, line, and other file sections. These pages are discovery references, not proof that this project uses Carrier data. Compare them with the separate [Original Medicare (FFS) Outpatient](https://resdac.org/cms-data/files/op-ffs) product page and its [Data Documentation](https://resdac.org/cms-data/files/op-ffs/data-documentation); the supplied field list matches the Outpatient base claim section, but the owner must confirm that product before the mapping is final.

Public ResDAC descriptions and dictionaries do **not** grant access to Research Identifiable File (RIF) data. Do not attempt to obtain, download, or bypass access to restricted records. Use source files only when the project owner confirms that the data and years are covered by the applicable CMS approval/DUA and that the agent's work is authorized in the FISMA-compliant HPC environment. The context may provide an owner-approved HPC path placeholder and schema-only inventory. If authorization, approved HPC access, or the requested year/file coverage is unclear, stop and report the exact blocker; do not probe restricted locations. See ResDAC's [RIF request guidance](https://resdac.org/research-identifiable-files-rif-requests) for access-process context.

## 4. Requested fields and output schema — proposal pending confirmation

The user supplied these requested source fields in a screenshot. Use only these fields unless the owner adds to the scope; do not infer an “all useful fields” scope from the product name. The Outpatient documentation lists ICD_DGNS_CD1 through ICD_DGNS_CD25. Interpret the request for “all the diagnosis codes ... and icd_dgns_cd” as the principal diagnosis field plus all 25 numbered ICD diagnosis fields; retain this interpretation as a proposal pending owner confirmation:

| Proposed output column | Source field(s) | Notes |
| --- | --- | --- |
| `bene_id` | `BENE_ID` | Encrypted CCW beneficiary identifier; sensitive identifier. |
| `clm_id` | `CLM_ID` | CCW claim identifier. |
| `clm_from_dt` | `CLM_FROM_DT` | Statement covers from date; confirm source type and date encoding. |
| `clm_thru_dt` | `CLM_THRU_DT` | Statement covers through date; confirm source type and date encoding. |
| `clm_fac_type_cd` | `CLM_FAC_TYPE_CD` | Facility component of institutional type of bill. |
| `clm_freq_cd` | `CLM_FREQ_CD` | Frequency/sequence component of institutional type of bill. |
| `ptnt_dschrg_stus_cd` | `PTNT_DSCHRG_STUS_CD` | Patient status as of CLM_THRU_DT. |
| `prncpal_dgns_cd` | `PRNCPAL_DGNS_CD` | Principal diagnosis; ResDAC says this duplicates `ICD_DGNS_CD1`. |
| `icd_dgns_cd` | `ICD_DGNS_CD1` ... `ICD_DGNS_CD25` | Ordered array proposal for all institutional diagnosis slots; preserve codes as strings. |

For every requested output, maintain a mapping ledger with:

| Item to specify | Required detail |
| --- | --- |
| Output | Canonical column name, business definition, and observation level |
| Role | Raw field, derived metric, or identifier; required, optional, or intentionally omitted |
| Source | CMS variable(s), authoritative reference, and availability in each of 2016–2018 |
| Representation | Expected type, units, code system/valid values, and sentinel/null conventions |
| Derivation | Formula or normalization and date basis (file year, claim year, service date, etc.) |
| Unavailable year | Whether to emit null, omit the column, or stop for owner review; do not silently change meaning |

The screenshot requests beneficiary and claim identifiers, statement dates, facility/frequency/discharge codes, principal diagnosis, and all diagnosis slots. It does not request line identifiers, procedure codes, provider fields, or payment/utilization measures; do not add them. Confirm the proposed ICD array output shape and privacy-safe handling for identifiers in the approved HPC workflow.

## 5. Mapping and YAML instructions

1. Record the proposed source as Original Medicare FFS Outpatient base claim based on the supplied field list and ResDAC Outpatient data documentation; obtain owner confirmation before marking product/grain final. The requested fields and proposed outputs are recorded in section 4.
2. Inspect all three years independently. The harmonizer uses schema presence to resolve scalar source candidates and array inputs; do not assume columns or types are stable across years. Check implementation behavior for schema discovery and multi-file reads.
3. Map only evidence-backed fields. Cite the CMS documentation and schema evidence in the mapping ledger; keep proposed mappings distinct from verified mappings. ResDAC supports the requested SAS field names, including ICD_DGNS_CD1-25, but the 2016-2018 parquet schemas must still confirm actual column availability and input types. Preserve ordered source alternatives only when equivalence is documented.
4. Choose explicit output types and casts after reviewing the per-year parquet schemas. Preserve identifiers and diagnosis/code values as strings to retain leading zeros and alphanumeric values. Specify date parsing, sentinel handling, and array construction only from confirmed source representations. Ensure the 25 diagnosis slots use a complete, ordered component list in every supported layout.
5. For each output, record its per-year source name(s), observed input type, cast or derivation, definition, reference, and supporting schema evidence. Review the generated mapping report against this ledger.
6. Write the proposed per-table mapping to `conf/datasets/medicare_partb/partb.yaml` and the datapaths profile to `conf/datapaths/medicare_partb.yaml`. Set the dataset YAML's `table_name` to `partb`. Keep `path_pattern` empty until the approved HPC parquet layout/pattern is supplied; do not invent real paths or glob patterns. Add only supported YAML fields and documented runner/config changes. A missing required source or unresolved difference in meaning is a question/blocker, not a reason to fabricate a mapping.

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

Resolve and record these with the project owner or authoritative source before marking the mapping final or running it:

- Confirm that the proposed Original Medicare FFS Outpatient base claim file and claim-level grain are the intended source.
- What approved HPC input layout and file pattern applies to each of 2016–2018?
- What does the year directory/field mean, and are there coverage gaps or preprocessing differences?
- Where are the approved 2016–2018 parquet schema inventories, including source types, date encodings, and code/sentinel conventions?
- Confirm the proposed diagnosis representation: separate principal-diagnosis scalar plus an ordered 25-slot ICD diagnosis array, where slot 1 duplicates the principal code.
- Confirm required output types, date semantics, missingness rules, and privacy-safe treatment of identifiers.

**Stop before marking mappings final or running harmonization** if product identity, grain, source availability, a requested field's definition, or the meaning of a year-specific source candidate remains unsupported. Return the specific question, evidence needed, and owner/source who can resolve it. Clearly label all unconfirmed decisions as assumptions or proposals; do not claim YAML, harmonization, or QC validation has already succeeded.
