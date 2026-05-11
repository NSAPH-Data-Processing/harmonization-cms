# Snakefile
configfile: "conf/snakemake.yaml"

wildcard_constraints:
    year = r"\d+"

TABLES = config["tables"]
YEARS = list(map(int, config["years"]))

BASEPATH = config["paths"]["basepath"]
OUTDIR = config["paths"]["output_path"]
CROSSWALKS = config["paths"]["crosswalks_path"]

rule all:
    input:
        expand(f"{OUTDIR}" + "/{table}/{table}_{year}.parquet", table=TABLES, year=YEARS),
        expand(f"{OUTDIR}" + "/{table}/{table}_{year}.run.json", table=TABLES, year=YEARS),
        expand(f"{OUTDIR}" + "/{table}/{table}_{year}.qc", table=TABLES, year=YEARS)

rule harmonize:
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
        python run_harmonizer.py \
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
        python run_qc.py \
          table={wildcards.table} year={wildcards.year} \
          paths.basepath="{params.basepath}" \
          paths.crosswalks_path="{params.crosswalks}" \
          paths.output_path="{params.outdir}" \
          hydra.run.dir="{params.hydra_dir}" \
          hydra.job.name="qc_{wildcards.table}_{wildcards.year}" \
          > {log} 2>&1
        """

