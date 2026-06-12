# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""xlma_stat — post-process enrichment node (pipeline_phase = POSTPROCESS_PHASE).

Lightning science only: reads xLMA flash-sorted NetCDF files (``*.nc``) from
``input_dir`` — raw LYLOUT ASCII must be converted in xLMA/pyxlma first
(pyxlma is not on conda and not a dependency); non-NetCDF files are ignored.
Each flash is matched to the minute-resolution cell mask of *its own minute*
(masks are produced upstream by the projection module and injected by the
PostProcessor as ``minute_masks``; this node never projects cells, interpolates
masks, or estimates motion). Two extension tables result:

- ``xlma_stat_minutes`` — the authoritative association, one row per
  ``(cell_uid, minute)``, with the mask's scan provenance.
- ``xlma_stat_scan`` — radar-aligned aggregate, one row per
  ``(cell_uid, scan)``, derived only from the minute rows.

Run only by the PostProcessor (``adapt postprocess --module xlma_stat``)
against a completed run. Core tables are never modified.
"""

import glob
import logging
import os

from adapt.contracts import check_xlma_stat_minutes, check_xlma_stat_scan
from adapt.execution.module_registry import registry
from adapt.modules.base import POSTPROCESS_PHASE, BaseModule
from adapt.modules.xlma_stat.config import XlmaStatConfig
from adapt.modules.xlma_stat.module import MinuteMask, XlmaStatistics
from adapt.modules.xlma_stat.reader import read_flash_sorted_frames
from adapt.persistence.module_output import OutputTableSpec

logger = logging.getLogger(__name__)


class XlmaStatModule(BaseModule):
    name = "xlma_stat"
    summary = "Lightning (xLMA) statistics on minute-projected cells; needs --input-dir"
    pipeline_phase = POSTPROCESS_PHASE
    config_class = XlmaStatConfig
    inputs = ["xlma_stat_config", "minute_masks", "radar_origin", "run_id"]
    outputs = ["xlma_stat_minutes_rows", "xlma_stat_scan_rows"]
    output_contracts = {
        "xlma_stat_minutes_rows": check_xlma_stat_minutes,
        "xlma_stat_scan_rows": check_xlma_stat_scan,
    }
    output_tables = {
        "xlma_stat_minutes_rows": OutputTableSpec(
            name="xlma_stat_minutes",
            primary_key=("run_id", "time", "cell_uid"),
            index_columns=("cell_uid", "time"),
        ),
        "xlma_stat_scan_rows": OutputTableSpec(
            name="xlma_stat_scan",
            primary_key=("run_id", "scan_time", "cell_uid"),
            index_columns=("cell_uid", "scan_time"),
        ),
    }

    @classmethod
    def build_config(cls, cfg) -> XlmaStatConfig:
        params = cfg.module_params.get("xlma_stat", {})
        return XlmaStatConfig(**params)

    def run(self, context: dict) -> dict:
        config = context["xlma_stat_config"]
        if not config.input_dir:
            raise ValueError(
                "xlma_stat requires an input directory; pass --input-dir or set "
                "xlma_stat.input_dir."
            )
        masks = [MinuteMask(**m) for m in context["minute_masks"]]
        radar_origin = context["radar_origin"]
        run_id = context["run_id"]

        files = sorted(glob.glob(os.path.join(config.input_dir, "*")))
        nc_files = [f for f in files if f.endswith(".nc")]
        if not nc_files:
            raise ValueError(
                f"No flash-sorted NetCDF (*.nc) files found in xlma_stat input_dir "
                f"'{config.input_dir}'. Convert raw LYLOUT ASCII sources to "
                "flash-sorted NetCDF in xLMA/pyxlma first."
            )
        ignored = len(files) - len(nc_files)
        if ignored:
            logger.info(
                "xlma_stat: ignoring %d non-NetCDF file(s) in %s", ignored, config.input_dir
            )
        flashes, sources = read_flash_sorted_frames(nc_files)

        minutes_df, scan_df = XlmaStatistics(config).compute(flashes, sources, masks, radar_origin)
        minutes_df.insert(0, "run_id", run_id)
        scan_df.insert(0, "run_id", run_id)
        logger.info(
            "xlma_stat: %d flashes -> %d (cell, minute) rows, %d (cell, scan) rows",
            len(flashes),
            len(minutes_df),
            len(scan_df),
        )
        return {
            "xlma_stat_minutes_rows": minutes_df,
            "xlma_stat_scan_rows": scan_df,
        }


registry.register(XlmaStatModule)
