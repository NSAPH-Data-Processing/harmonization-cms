from __future__ import annotations

import logging
import os

import hydra
from omegaconf import DictConfig, OmegaConf

import yaml

from harmonizecms.qc import check_rollup_builds, run_qc

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    table = str(cfg.table)
    table_cfg_path = cfg.tables[table]

    # ++mode=builds: after every year is done, check all yearly outputs of a rollup table come from
    # the current stay build (IP-R56 b). Default: QC one year.
    mode = str(cfg.get("mode", "year"))
    if mode == "builds":
        with open(table_cfg_path, encoding="utf-8") as f:
            table_cfg = yaml.safe_load(f) or {}
        table_name = table_cfg.get("table_name") or table_cfg.get("name") or table
        outdir = str(cfg.paths.output_path)
        years = [int(y) for y in cfg.years] if cfg.get("years") else None
        result = check_rollup_builds(outdir, table_name, years,
                                     out_file=os.path.join(outdir, f"{table}_builds.json"))
        for w in result["warnings"]:
            LOGGER.warning(w)
        LOGGER.info("Build check status=%s", result["status"])
        return
    if mode != "year":
        raise ValueError(f"mode must be 'year' or 'builds', not {mode!r}")

    year = int(cfg.year)

    # IMPORTANT:
    # cfg.paths.output_path is treated as the *actual* output directory for this run.
    # Under Snakemake, we pass paths.output_path="{OUTDIR}/{table}" already.
    outdir = str(cfg.paths.output_path)

    out_parquet = os.path.join(outdir, f"{table}_{year}.parquet")
    qc_dir = os.path.join(outdir, f"{table}_{year}.qc")

    qc = run_qc(
        table_config_path=table_cfg_path,
        basepath=cfg.paths.basepath,
        output_parquet_path=out_parquet,
        year=int(cfg.year),
        qc_dir=qc_dir,
        sample_n=100,
        crosswalks_path=getattr(cfg.paths, "crosswalks_path", None),
    )

    LOGGER.info("QC status=%s. Wrote %s", qc.get("status"), os.path.join(qc_dir, "qc.json"))


if __name__ == "__main__":
    main()