# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

from adapt.contracts import check_projected_ds, check_scan_history, check_segmented_ds
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.projection.config import ProjectionConfig
from adapt.modules.projection.module import RadarCellProjector


class ProjectionModule(BaseModule):
    """BaseModule wrapper for RadarCellProjector.

    Computes optical flow between consecutive radar frames and projects
    cell positions forward in time. Receives the scan pair via
    ``scan_history`` (list of prior scan context dicts).

    Context inputs
    --------------
    segmented_ds : xr.Dataset
        2D segmented dataset for the current frame (output of DetectModule).
    scan_history : list[dict]
        Rolling history of scan context dicts supplied by the processor.
        Each dict must contain ``segmented_ds`` and ``scan_time``.
        Requires at least 2 entries (required_history=2).
    projection_config : ProjectionModuleConfig
        Runtime configuration.

    Context outputs
    ---------------
    projected_ds : xr.Dataset
        2D dataset with heading_x, heading_y, and cell_projections added.
    """

    name = "projection"
    summary = "optical-flow projection between scans"
    required_history = 2
    pipeline_phase = 0
    inputs = ["segmented_ds", "scan_history", "projection_config"]
    outputs = ["projected_ds"]
    input_contracts = {"segmented_ds": check_segmented_ds, "scan_history": check_scan_history}
    output_contracts = {"projected_ds": check_projected_ds}
    config_class = ProjectionConfig

    @classmethod
    def build_config(cls, cfg) -> ProjectionConfig:
        return ProjectionConfig(
            method=cfg.projector.method,
            nan_fill_value=cfg.projector.nan_fill_value,
            max_time_interval_minutes=cfg.projector.max_time_interval_minutes,
            max_projection_steps=cfg.projector.max_projection_steps,
            pyr_scale=cfg.projector.flow_params.pyr_scale,
            levels=cfg.projector.flow_params.levels,
            winsize=cfg.projector.flow_params.winsize,
            iterations=cfg.projector.flow_params.iterations,
            poly_n=cfg.projector.flow_params.poly_n,
            poly_sigma=cfg.projector.flow_params.poly_sigma,
            flags=cfg.projector.flow_params.flags,
            min_motion_threshold=cfg.projector.min_motion_threshold,
            max_flow_magnitude=cfg.projector.max_flow_magnitude,
            registration_step_minutes=cfg.projector.registration_step_minutes,
            reflectivity_var=cfg.global_.var_names.reflectivity,
        )

    def __init__(self) -> None:
        self._projector: RadarCellProjector | None = None

    def run(self, context: dict) -> dict:
        config = context["projection_config"]
        scan_history = context["scan_history"]  # list of scan context dicts

        if self._projector is None:
            self._projector = RadarCellProjector(config)

        ds_list = [ctx["segmented_ds"] for ctx in scan_history]
        projected = self._projector.project(ds_list)
        return {"projected_ds": projected}


registry.register(ProjectionModule)
