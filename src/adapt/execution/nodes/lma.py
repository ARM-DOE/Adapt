# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""lma — post-process enrichment node (pipeline_phase = POSTPROCESS_PHASE).

Reads LMA ASCII files (PyXLMA), clusters flashes globally, attributes each flash
to a storm cell by its initiation point, bins lightning to 1-minute accumulation
windows, and writes two extension tables: ``lma_cell_stats`` (one row per
``(cell_uid, time_bin)``) and ``lma_flash_attribution`` (one row per flash).

Run only by the PostProcessor (``adapt postprocess --module lma``). The cell
masks, the label→cell_uid lookup, the projected grid, and the radar origin are
injected by the PostProcessor — this node never touches the repository directly.
"""

import glob
import logging
import os

from adapt.contracts import check_lma_cell_stats, check_lma_flash_attribution
from adapt.execution.module_registry import registry
from adapt.modules.base import POSTPROCESS_PHASE, BaseModule
from adapt.modules.lma.config import LMAConfig
from adapt.modules.lma.module import LMACellStatistics, ScanMask
from adapt.modules.lma.reader import (
    cluster_flashes_with_stats,
    events_to_frame,
    flashes_to_frame,
    read_event_dataset,
)
from adapt.persistence.module_output import OutputTableSpec

logger = logging.getLogger(__name__)


class LMACellStatisticsModule(BaseModule):
    name = "lma"
    summary = "Lightning (LMA) per-cell flash statistics; needs --input-dir"
    pipeline_phase = POSTPROCESS_PHASE
    config_class = LMAConfig
    inputs = ["lma_config", "scan_masks", "radar_origin", "run_id"]
    outputs = ["lma_cell_stats_rows", "lma_flash_attribution_rows"]
    output_contracts = {
        "lma_cell_stats_rows": check_lma_cell_stats,
        "lma_flash_attribution_rows": check_lma_flash_attribution,
    }
    output_tables = {
        "lma_cell_stats_rows": OutputTableSpec(
            name="lma_cell_stats",
            primary_key=("cell_uid", "time_bin"),
            index_columns=("cell_uid", "time_bin"),
        ),
        "lma_flash_attribution_rows": OutputTableSpec(
            name="lma_flash_attribution",
            primary_key=("flash_id",),
            index_columns=("cell_uid", "time_bin"),
        ),
    }

    @classmethod
    def build_config(cls, cfg) -> LMAConfig:
        params = cfg.module_params.get("lma", {})
        return LMAConfig(**params)

    def run(self, context: dict) -> dict:
        config = context["lma_config"]
        if not config.input_dir:
            raise ValueError(
                "lma requires an input directory; pass --input-dir or set lma.input_dir."
            )
        masks = [ScanMask(**m) for m in context["scan_masks"]]
        radar_origin = context["radar_origin"]
        run_id = context["run_id"]

        files = sorted(glob.glob(os.path.join(config.input_dir, "*")))
        events = read_event_dataset(files)
        flashes_ds = cluster_flashes_with_stats(
            events, config.flash_distance_m, config.flash_time_s
        )
        flashes = flashes_to_frame(flashes_ds)
        sources = events_to_frame(flashes_ds)

        cell_stats, flash_attr = LMACellStatistics(config).compute(
            flashes, sources, masks, radar_origin
        )
        cell_stats.insert(0, "run_id", run_id)
        flash_attr.insert(0, "run_id", run_id)
        logger.info("lma: %d flashes -> %d (cell, bin) rows", len(flash_attr), len(cell_stats))
        return {
            "lma_cell_stats_rows": cell_stats,
            "lma_flash_attribution_rows": flash_attr,
        }


registry.register(LMACellStatisticsModule)
