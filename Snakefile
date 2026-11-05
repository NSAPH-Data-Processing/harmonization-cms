# Snakefile
import os

import yaml

configfile: "conf/snakemake.yaml"

# Optional: the shell (e.g. Git bash on Windows) and the Python interpreter of the jobs
if config.get("shell_executable"):
    shell.executable(config["shell_executable"])
PYTHON = config.get("python", "python")
# Optional duckdb settings of the stay build (memory_limit, threads, temp_directory)
DUCKDB = config.get("duckdb") or {}

wildcard_constraints:
    year = r"\d+"

TABLES = config["tables"]
YEARS = list(map(int, config["years"]))

BASEPATH = config["paths"]["basepath"]
OUTDIR = config["paths"]["output_path"]
CROSSWALKS = config["paths"]["crosswalks_path"]


# Table -> YAML as run_harmonizer.py resolves it: conf/config.yaml `tables` (local, IP-R17). A table missing there
# (or a missing config.yaml / YAML file) has no rollup here (IP-R54 e), so its DAG is today's. A `tables.<t>=...`
# Hydra override on the command line is not seen here; the yearly run's stale-build check then fails loudly.
def _table_yamls():
    try:
        with open("conf/config.yaml") as f:
            return (yaml.safe_load(f) or {}).get("tables") or {}
    except FileNotFoundError:
        return {}

TABLE_YAMLS = _table_yamls()


def rollup_of(table):
    path = TABLE_YAMLS.get(table)
    if not path or not os.path.exists(path):
        return None
    with open(path) as f:
        return (yaml.safe_load(f) or {}).get("rollup")


def build_files(table):
    return [f"{OUTDIR}/{table}/_build/{table}_stays.parquet", f"{OUTDIR}/{table}/_build/{table}_stays.run.json"]


def stays_input(wc):
    # A table with a `rollup:` block: each year reads the stays built once over all YEARS (IP-R54 a)
    return build_files(wc.table) if rollup_of(wc.table) else []


def build_inputs(wc):  # IP-R54 f
    path = TABLE_YAMLS[wc.table]
    cut = rollup_of(wc.table)["cutover"]
    cut = cut if os.path.isabs(cut) else os.path.join(os.path.dirname(path), cut)
    return {"table_yaml": path, "cutover": cut,
            "code": ["harmonizecms/stays.py", "harmonizecms/harmonizer.py", "harmonizecms/io.py"]}  # IP-R55 d


def builds_check(table):
    return f"{OUTDIR}/{table}/{table}_builds.json"


rule all:
    input:
        expand(f"{OUTDIR}" + "/{table}/{table}_{year}.parquet", table=TABLES, year=YEARS),
        expand(f"{OUTDIR}" + "/{table}/{table}_{year}.run.json", table=TABLES, year=YEARS),
        expand(f"{OUTDIR}" + "/{table}/{table}_{year}.qc", table=TABLES, year=YEARS),
        # rollup tables only: all years from one stay build, checked after every year is done (IP-R56 b)
        [builds_check(t) for t in TABLES if rollup_of(t)]

rule build_stays:
    input:
        unpack(build_inputs)
    output:
        parquet=f"{OUTDIR}" + "/{table}/_build/{table}_stays.parquet",
        runjson=f"{OUTDIR}" + "/{table}/_build/{table}_stays.run.json"
    log:
        "logs/build_stays_{table}.log"
    # duckdb.threads when set, else the cores Snakemake grants (IP-R56 a): 64 is only an upper bound that
    # Snakemake scales down to --cores. A fixed number, not workflow.cores, which raises without --cores
    # (e.g. `snakemake -n`) and whose API differs across Snakemake versions (RR-1).
    threads: int(DUCKDB["threads"]) if DUCKDB.get("threads") else 64
    params:
        basepath=BASEPATH,
        crosswalks=CROSSWALKS,
        outdir=lambda wc: f"{OUTDIR}/{wc.table}",
        years="[" + ",".join(str(y) for y in YEARS) + "]",
        # quoted, so paths with spaces or `\` survive the shell (WPR-11)
        duckdb=" ".join(f"'++duckdb.{k}={v}'" for k, v in DUCKDB.items() if k != "threads"),
        hydra_dir=lambda wc: f"logs/hydra/build_stays/{wc.table}"
    shell:
        r"""
        {PYTHON} run_harmonizer.py \
          table={wildcards.table} ++mode=stays '++years={params.years}' ++duckdb.threads={threads} {params.duckdb} \
          paths.basepath="{params.basepath}" \
          paths.crosswalks_path="{params.crosswalks}" \
          paths.output_path="{params.outdir}" \
          hydra.run.dir="{params.hydra_dir}" \
          hydra.job.name="build_stays_{wildcards.table}" \
          > {log} 2>&1
        """

rule harmonize:
    input:
        stays=stays_input
    output:
        parquet=f"{OUTDIR}" + "/{table}/{table}_{year}.parquet",
        runjson=f"{OUTDIR}" + "/{table}/{table}_{year}.run.json"
    log:
        "logs/harmonize_{table}_{year}.log"
    params:
        basepath=BASEPATH,
        crosswalks=CROSSWALKS,
        outdir=lambda wc: f"{OUTDIR}/{wc.table}",
        hydra_dir=lambda wc: f"logs/hydra/harmonize/{wc.table}/{wc.year}"
    shell:
        r"""
        {PYTHON} run_harmonizer.py \
          table={wildcards.table} year={wildcards.year} \
          paths.basepath="{params.basepath}" \
          paths.crosswalks_path="{params.crosswalks}" \
          paths.output_path="{params.outdir}" \
          hydra.run.dir="{params.hydra_dir}" \
          hydra.job.name="harmonize_{wildcards.table}_{wildcards.year}" \
          > {log} 2>&1
        """

rule qc:
    input:
        parquet=f"{OUTDIR}" + "/{table}/{table}_{year}.parquet",
        runjson=f"{OUTDIR}" + "/{table}/{table}_{year}.run.json"
    output:
        qcdir=directory(f"{OUTDIR}" + "/{table}/{table}_{year}.qc")
    log:
        "logs/qc_{table}_{year}.log"
    params:
        basepath=BASEPATH,
        crosswalks=CROSSWALKS,
        outdir=lambda wc: f"{OUTDIR}/{wc.table}",
        hydra_dir=lambda wc: f"logs/hydra/qc/{wc.table}/{wc.year}"
    shell:
        r"""
        {PYTHON} run_qc.py \
          table={wildcards.table} year={wildcards.year} \
          paths.basepath="{params.basepath}" \
          paths.crosswalks_path="{params.crosswalks}" \
          paths.output_path="{params.outdir}" \
          hydra.run.dir="{params.hydra_dir}" \
          hydra.job.name="qc_{wildcards.table}_{wildcards.year}" \
          > {log} 2>&1
        """

rule qc_builds:
    # One check over all years of a rollup table, after every year is done: every yearly output
    # comes from the current stay build (IP-R55 e, IP-R56 b). Each yearly qc checks only its own year.
    input:
        runjson=lambda wc: expand(f"{OUTDIR}/{wc.table}/{wc.table}_{{year}}.run.json", year=YEARS),
        qc=lambda wc: expand(f"{OUTDIR}/{wc.table}/{wc.table}_{{year}}.qc", year=YEARS),
        stays=lambda wc: build_files(wc.table)
    output:
        f"{OUTDIR}" + "/{table}/{table}_builds.json"
    log:
        "logs/qc_builds_{table}.log"
    params:
        outdir=lambda wc: f"{OUTDIR}/{wc.table}",
        years="[" + ",".join(str(y) for y in YEARS) + "]",
        hydra_dir=lambda wc: f"logs/hydra/qc_builds/{wc.table}"
    shell:
        r"""
        {PYTHON} run_qc.py \
          table={wildcards.table} ++mode=builds '++years={params.years}' \
          paths.output_path="{params.outdir}" \
          hydra.run.dir="{params.hydra_dir}" \
          hydra.job.name="qc_builds_{wildcards.table}" \
          > {log} 2>&1
        """
