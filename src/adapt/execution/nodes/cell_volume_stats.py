# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""cell_volume_stats — post-persistence enrichment node (pipeline_phase = 3).

Computes per-cell 3D statistics from the stored gridded volume and writes one row
per (run_id, scan_time, cell_uid) to the cell_volume_stats table, joinable to
cells_by_scan. Requires regridder.save_netcdf: true so grid_ds_3d is available.

Output columns (authoritative — documented here, schema inferred from the frame):
  run_id, scan_time, cell_uid, cell_label                     : index
  cell_area_km2, cell_volume_km3, cell_top_m/base_m/depth_m   : geometry
  dbz_{max,mean,std,min}, z_{mean,max,std}                    : reflectivity
  vol_{20,30,40,50,60}dbz_km3                                 : volume-by-threshold
  cell_eth{10,20,30,40,50}_*                                  : echo-top features
  dbz_{max,com}_height_m                                      : height stats
  {zdr,kdp,rhohv}_{max,mean,std,min}                          : polarimetric (if present)
  multilayer_fraction, mean_nlayers, max_nlayers              : storm structure
"""

import logging

import pandas as pd

from adapt.contracts import SqliteTable, check_cell_volume_stats
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.cell_volume_stats.config import CellVolumeStatsConfig
from adapt.modules.cell_volume_stats.module import CellVolumeStatsAlgorithm

logger = logging.getLogger(__name__)


class CellVolumeStatsModule(BaseModule):
    name = "cell_volume_stats"
    summary = "3D volume stats (cloud-top height); needs regridder.save_netcdf"
    pipeline_phase = 3
    required_history = 1
    config_class = CellVolumeStatsConfig
    # Names filled from the shared global section by build_config; the remaining
    # config_class fields (thresholds, gap_tolerance_m, structure_threshold, and
    # polarimetric var names) are the module's owned, user-tunable params.
    injected_global_fields = frozenset(
        {"reflectivity_var", "labels_var", "z_coord", "y_coord", "x_coord", "time_coord"}
    )
    inputs = [
        "cell_volume_stats_config",
        "grid_ds_3d",
        "segmented_ds",
        "tracked_cells",
        "run_id",
        "scan_time",
    ]
    outputs = ["cell_volume_stats_rows"]
    output_contracts = {"cell_volume_stats_rows": check_cell_volume_stats}
    persistence = (
        SqliteTable(
            key="cell_volume_stats_rows",
            table="cell_volume_stats",
            primary_key=("run_id", "scan_time", "cell_uid"),
            index_columns=("scan_time", "cell_uid"),
        ),
    )

    @classmethod
    def build_config(cls, cfg) -> CellVolumeStatsConfig:
        # Only inject names that exist in the canonical global var_names; polarimetric
        # var names fall back to config defaults (or user module_params).
        params = cfg.module_params.get("cell_volume_stats", {})
        return CellVolumeStatsConfig(
            reflectivity_var=cfg.global_.var_names.reflectivity,
            labels_var=cfg.global_.var_names.cell_labels,
            z_coord=cfg.global_.coord_names.z,
            y_coord=cfg.global_.coord_names.y,
            x_coord=cfg.global_.coord_names.x,
            time_coord=cfg.global_.coord_names.time,
            **params,
        )

    def run(self, context: dict) -> dict:
        config = context["cell_volume_stats_config"]
        grid_3d = context.get("grid_ds_3d")
        segmented = context.get("segmented_ds")
        tracked = context.get("tracked_cells")

        if grid_3d is None:
            logger.warning(
                "cell_volume_stats: grid_ds_3d unavailable — set regridder.save_netcdf: true"
            )
            return {"cell_volume_stats_rows": pd.DataFrame()}
        if segmented is None or tracked is None or tracked.empty:
            return {"cell_volume_stats_rows": pd.DataFrame()}

        cell_labels_2d = segmented[config.labels_var].values
        algo = CellVolumeStatsAlgorithm(config)
        rows = [
            algo.compute_cell(
                grid_3d,
                cell_labels_2d,
                r.cell_label,
                context["run_id"],
                context["scan_time"],
                r.cell_uid,
            )
            for _, r in tracked.iterrows()
        ]
        return {"cell_volume_stats_rows": pd.DataFrame(rows)}


registry.register(CellVolumeStatsModule)
