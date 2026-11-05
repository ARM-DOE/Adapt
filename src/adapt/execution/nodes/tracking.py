# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

from adapt.contracts import (
    CELL_LABELS_VAR,
    DecisionTableWrite,
    NetcdfArtifact,
    TrackTablesWrite,
    check_cell_events,
    check_projected_ds,
    check_tracked_cells,
    check_tracking_decisions,
)
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.tracking.config import TrackingConfig
from adapt.modules.tracking.lut import attach_cell_uid_lut, attach_registration_uid_lut
from adapt.modules.tracking.module import CellTracker


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
        Explicit event rows for CONTINUE, SPLIT, MERGE, INITIATION, TERMINATION,
        LATENT (a track kept for resumption) and RESUMED.
    analysis_ds : xr.Dataset
        ``projected_ds`` plus the cell_uid LUTs (``cell_uid`` for this scan's
        labels, ``registration_cell_uid`` for the previous scan's) — the dataset
        persisted as the analysis NetCDF.
    """

    name = "tracking"
    summary = "link cells across scans"
    required_history = 2
    pipeline_phase = 0
    inputs = ["projected_ds", "cell_stats", "tracking_config", "scan_time", "scan_id"]
    # tracking_decisions: every candidate considered this scan and why each
    # cell continued or ended — analysis-only, written to decisions.db.
    outputs = ["tracked_cells", "cell_events", "analysis_ds", "tracking_decisions"]
    input_contracts = {"projected_ds": check_projected_ds}
    output_contracts = {
        "tracked_cells": check_tracked_cells,
        "cell_events": check_cell_events,
        "analysis_ds": check_projected_ds,
        "tracking_decisions": check_tracking_decisions,
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
        DecisionTableWrite(key="tracking_decisions"),
    )

    @classmethod
    def build_config(cls, cfg) -> TrackingConfig:
        t = cfg.tracker
        return TrackingConfig(
            uid_width=t.cell_uid.width,
            field_var=cfg.global_.tracking_field,
            labels_var=CELL_LABELS_VAR,
            max_tracking_gap_minutes=cfg.global_.max_scan_gap_minutes,
            cell_threshold=t.cell_threshold,
            polarity=t.polarity,
            growth_max_km=t.growth_max_km,
            growth_size_km=t.growth_size_km,
            max_overlap_mismatch=t.max_overlap_mismatch,
            max_link_cost=t.max_link_cost,
            residual_weight=t.residual_weight,
            heading_weight=t.heading_weight,
            max_speed_ms=t.max_speed_ms,
            max_acceleration_ms2=t.max_acceleration_ms2,
            split_overlap=t.split_overlap_threshold,
            merge_overlap=t.merge_overlap_threshold,
            latent_scans=t.latent_scans,
            identity_intensity_weight=t.identity_intensity_weight,
            identity_score_margin=t.identity_score_margin,
            core_field_threshold=t.core_field_threshold,
        )

    def __init__(self) -> None:
        self._tracker: CellTracker | None = None
        # Previous scan's tracked cells: maps the prev-scan labels carried by
        # registration_minutes to global uids on the next scan's analysis_ds.
        self._prev_tracked_cells = None

    def run(self, context: dict) -> dict:
        config = context["tracking_config"]
        ds_2d = context["projected_ds"]
        cell_stats = context["cell_stats"]

        if self._tracker is None:
            self._tracker = CellTracker(config)

        tracked_cells, cell_events = self._tracker.track(
            ds_projected=ds_2d,
            cell_stats_df=cell_stats,
            scan_id=context["scan_id"],
        )

        analysis_ds = attach_cell_uid_lut(ds_2d, tracked_cells)
        analysis_ds = attach_registration_uid_lut(analysis_ds, self._prev_tracked_cells)
        if tracked_cells is not None and not tracked_cells.empty:
            self._prev_tracked_cells = tracked_cells

        return {
            "tracked_cells": tracked_cells,
            "cell_events": cell_events,
            "analysis_ds": analysis_ds,
            "tracking_decisions": self._tracker.decisions(),
        }


registry.register(TrackingModule)
