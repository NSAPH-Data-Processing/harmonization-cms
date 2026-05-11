from __future__ import annotations

import logging
import os

import hydra
from omegaconf import DictConfig, OmegaConf

from harmonizecms import harmonize_table_year

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Example:
      python run_harmonizer.py table=ps year=2011
    """
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    table_cfg_path = cfg.tables[cfg.table]  # maps name -> per-table yaml path
    out_file = harmonize_table_year(
        table_config_path=table_cfg_path,
        basepath=cfg.paths.basepath,
        output_path=cfg.paths.output_path,
        year=int(cfg.year),
        crosswalks_path=getattr(cfg.paths, "crosswalks_path", None),
    )
    LOGGER.info("Wrote %s", out_file)


if __name__ == "__main__":
    main()


