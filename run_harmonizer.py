from __future__ import annotations

import logging
import os

import hydra
from omegaconf import DictConfig, OmegaConf

from harmonizecms import build_table_stays, harmonize_table_year

LOGGER = logging.getLogger(__name__)


@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig) -> None:
    """
    Examples:
      python run_harmonizer.py table=ps year=2011
      python run_harmonizer.py table=ip ++mode=stays '++years=[2016,2017,2018]' ++duckdb.memory_limit=4GB

    `mode` (default "year") is "year" (one year's output) or "stays" (the stay build of a table with
    a `rollup:` block, over `years`; run it before the yearly runs). Optional `++duckdb.memory_limit`,
    `++duckdb.threads`, `++duckdb.temp_directory` are SET on the duckdb connection.
    """
    LOGGER.info("Config:\n%s", OmegaConf.to_yaml(cfg))

    mode = str(cfg.get("mode", "year"))
    duckdb_settings = OmegaConf.to_container(cfg.get("duckdb"), resolve=True) if cfg.get("duckdb") else None
    table_cfg_path = cfg.tables[cfg.table]  # maps name -> per-table yaml path
    common = dict(
        basepath=cfg.paths.basepath,
        output_path=cfg.paths.output_path,
        crosswalks_path=getattr(cfg.paths, "crosswalks_path", None),
        duckdb_settings=duckdb_settings,
    )
    if mode == "stays":
        if not cfg.get("years"):
            raise ValueError("mode=stays needs ++years=[...]")
        out_file = build_table_stays(table_cfg_path, years=[int(y) for y in cfg.years], **common)
    elif mode == "year":
        out_file = harmonize_table_year(table_config_path=table_cfg_path, year=int(cfg.year), **common)
    else:
        raise ValueError(f"mode must be 'year' or 'stays', not {mode!r}")
    LOGGER.info("Wrote %s", out_file)


if __name__ == "__main__":
    main()
