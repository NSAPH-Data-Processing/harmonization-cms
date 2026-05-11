from __future__ import annotations

import logging
import os

import hydra
from omegaconf import DictConfig, OmegaConf

from harmonizecms.qc import run_qc

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    table = str(cfg.table)
    year = int(cfg.year)

    table_cfg_path = cfg.tables[table]

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