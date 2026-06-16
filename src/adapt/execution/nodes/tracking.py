# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

from adapt.contracts import (
    NetcdfArtifact,
    TrackTablesWrite,
    check_cell_events,
    check_projected_ds,
    check_tracked_cells,
)
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.tracking.config import TrackingConfig
from adapt.modules.tracking.lut import attach_cell_uid_lut, attach_registration_uid_lut
from adapt.modules.tracking.module import RadarCellTracker


class TrackingModule(BaseModule):
    """Assign stable `cell_uid` identities to convective cells across consecutive radar scans.

    Produces scan-local tracking outputs. Any higher-level grouping/aggregation
    is outside this module's scope.

    Context inputs
    --------------
    projected_ds : xr.Dataset
        2D dataset with projections (output of ProjectionModule).
    cell_stats : pd.DataFrame
        Per-cell statistics (output of AnalysisModule).
    tracking_config : TrackingModuleConfig
        Runtime configuration for the tracker.
    scan_time : datetime
        Radar scan timestamp.

    Context outputs
    ---------------
    tracked_cells : pd.DataFrame
        Per-cell observations for the current scan with cell_uid/cell_label.
    cell_events : pd.DataFrame
        Explicit event rows for CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION.
    analysis_ds : xr.Dataset
        ``projected_ds`` plus the cell_uid LUTs (``cell_uid`` for this scan's
        labels, ``registration_cell_uid`` for the previous scan's) — the dataset
        persisted as the analysis NetCDF.
    """

    name = "tracking"
    summary = "link cells across scans"
    required_history = 2
    pipeline_phase = 0
    inputs = ["projected_ds", "cell_stats", "tracking_config", "scan_time"]
    outputs = ["tracked_cells", "cell_events", "analysis_ds"]
    input_contracts = {"projected_ds": check_projected_ds}
    output_contracts = {
        "tracked_cells": check_tracked_cells,
        "cell_events": check_cell_events,
        "analysis_ds": check_projected_ds,
    }
    config_class = TrackingConfig
    # DEBT: TrackTablesWrite encodes tracking science that lives in
    # TrackStore.write_scan. Follow-up ticket: tracking emits final row
    # DataFrames so this decomposes into plain SqliteTable specs.
    persistence = (
        NetcdfArtifact(
            key="analysis_ds",
            product_type="segmentation2d",
            producer="processor",
            description="Radar analysis with segmentation and projections",
        ),
        TrackTablesWrite(
            tracked_key="tracked_cells",
            events_key="cell_events",
            stats_key="cell_stats",
            adjacency_key="cell_adjacency",
        ),
    )

    @classmethod
    def build_config(cls, cfg) -> TrackingConfig:
        return TrackingConfig(
            match_cost=cfg.tracker.match_cost_threshold,
            keep_cost=cfg.tracker.keep_cost_threshold,
            unmatch_cost=cfg.tracker.unmatch_cost_threshold,
            split_overlap=cfg.tracker.split_overlap_threshold,
            core_reflectivity_threshold=cfg.tracker.core_reflectivity_threshold,
            uid_time_step_s=cfg.tracker.cell_uid.time_step_s,
            uid_latlon_step_deg=cfg.tracker.cell_uid.latlon_step_deg,
            uid_area_step_km2=cfg.tracker.cell_uid.area_step_km2,
            uid_width=cfg.tracker.cell_uid.width,
            reflectivity_var=cfg.global_.var_names.reflectivity,
            labels_var=cfg.global_.var_names.cell_labels,
            max_gap_minutes=cfg.tracker.max_gap_minutes,
            expected_speed_ms=cfg.tracker.expected_speed_ms,
        )

    def __init__(self) -> None:
        self._tracker: RadarCellTracker | None = None
        # Previous scan's tracked cells: maps the prev-scan labels carried by
        # registration_minutes to global uids on the next scan's analysis_ds.
        self._prev_tracked_cells = None

    def run(self, context: dict) -> dict:
        config = context["tracking_config"]
        ds_2d = context["projected_ds"]
        cell_stats = context["cell_stats"]

        if self._tracker is None:
            self._tracker = RadarCellTracker(config)

        tracked_cells, cell_events = self._tracker.track(
            ds_projected=ds_2d,
            cell_stats_df=cell_stats,
        )

        analysis_ds = attach_cell_uid_lut(ds_2d, tracked_cells)
        analysis_ds = attach_registration_uid_lut(analysis_ds, self._prev_tracked_cells)
        if tracked_cells is not None and not tracked_cells.empty:
            self._prev_tracked_cells = tracked_cells

        return {
            "tracked_cells": tracked_cells,
            "cell_events": cell_events,
            "analysis_ds": analysis_ds,
        }


registry.register(TrackingModule)
