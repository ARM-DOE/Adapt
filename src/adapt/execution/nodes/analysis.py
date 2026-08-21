# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

from adapt.contracts import (
    CELL_LABELS_VAR,
    ProductTableWrite,
    check_cell_adjacency,
    check_cell_stats,
)
from adapt.execution.module_registry import registry
from adapt.modules.analysis.config import AnalysisConfig
from adapt.modules.analysis.module import RadarCellAnalyzer
from adapt.modules.base import BaseModule


class AnalysisModule(BaseModule):
    """BaseModule wrapper for RadarCellAnalyzer.

    Extracts per-cell statistics (area, reflectivity, motion, centroids)
    from a segmented/projected 2D dataset. Pure compute — no I/O.
    Persistence is declared below and routed by the OutputRouter.

    Context inputs
    --------------
    projected_ds : xr.Dataset
        2D dataset with projections (output of ProjectionModule).
    config : InternalConfig
        Runtime configuration.
    scan_time : datetime
        Radar scan timestamp (from LoadModule).

    Context outputs
    ---------------
    cell_stats : pd.DataFrame
        Per-cell statistics DataFrame.
    cell_adjacency : pd.DataFrame
        Touching-cell pairs DataFrame.
    """

    name = "analysis"
    summary = "2D per-cell statistics"
    required_history = 2
    pipeline_phase = 0
    inputs = ["projected_ds", "analysis_config", "scan_time"]
    outputs = ["cell_stats", "cell_adjacency"]
    output_contracts = {
        "cell_stats": check_cell_stats,
        "cell_adjacency": check_cell_adjacency,
    }
    config_class = AnalysisConfig
    persistence = (
        ProductTableWrite(
            key="cell_stats",
            table="cell_stats",
            primary_key=("run_id", "scan_id", "cell_label"),
            index_columns=("cell_label",),
        ),
        ProductTableWrite(
            key="cell_adjacency",
            table="cell_adjacency",
            primary_key=("run_id", "scan_id", "cell_label_a", "cell_label_b"),
        ),
    )

    @classmethod
    def build_config(cls, cfg) -> AnalysisConfig:
        return AnalysisConfig(
            radar_variables=tuple(cfg.analyzer.radar_variables),
            exclude_fields=tuple(cfg.analyzer.exclude_fields),
            adjacency_min_touching=cfg.analyzer.adjacency_min_touching_boundary_pixels,
            max_projection_steps=cfg.projector.max_projection_steps,
            reflectivity_var=cfg.global_.tracking_field,
            labels_var=CELL_LABELS_VAR,
            z_level=cfg.global_.z_level,
        )

    def __init__(self) -> None:
        self._analyzer: RadarCellAnalyzer | None = None

    def run(self, context: dict) -> dict:
        config = context["analysis_config"]
        ds_2d = context["projected_ds"]

        if self._analyzer is None:
            self._analyzer = RadarCellAnalyzer(config)

        df_cells = self._analyzer.extract(ds_2d, z_level=config.z_level)
        df_adjacency = self._analyzer.extract_adjacency(ds_2d)

        return {"cell_stats": df_cells, "cell_adjacency": df_adjacency}


registry.register(AnalysisModule)
