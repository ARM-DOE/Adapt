# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

from adapt.contracts import CELL_LABELS_VAR, check_grid_ds_2d, check_segmented_ds
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.detection.config import DetectionConfig
from adapt.modules.detection.module import RadarCellSegmenter


class DetectModule(BaseModule):
    """BaseModule wrapper for RadarCellSegmenter.

    Segments convective cells from a 2D reflectivity field using
    threshold and morphological filtering.

    Context inputs
    --------------
    grid_ds_2d : xr.Dataset
        2D Cartesian dataset (output of LoadModule).
    grid_ds : xr.Dataset
        Full 3D Cartesian dataset (also an ingest output). The pyart
        convective/stratiform methods classify it; ``threshold`` ignores it.
    config : InternalConfig
        Runtime configuration.

    Context outputs
    ---------------
    segmented_ds : xr.Dataset
        2D dataset with cell_labels variable added.
    num_cells : int
        Number of detected cells.
    """

    name = "detection"
    summary = "segment cells from the grid"
    required_history = 1
    pipeline_phase = 0
    inputs = ["grid_ds_2d", "grid_ds", "detection_config"]
    outputs = ["segmented_ds", "num_cells"]
    input_contracts = {"grid_ds_2d": check_grid_ds_2d}
    output_contracts = {"segmented_ds": check_segmented_ds}
    config_class = DetectionConfig

    @classmethod
    def build_config(cls, cfg) -> DetectionConfig:
        seg = cfg.segmenter
        method_params = getattr(seg, f"{seg.method}_params").model_dump()
        return DetectionConfig(
            method=seg.method,
            method_params=method_params,
            closing_kernel=seg.closing_kernel,
            filter_by_size=seg.filter_by_size,
            min_cellsize_gridpoint=seg.min_cellsize_gridpoint,
            max_cellsize_gridpoint=seg.max_cellsize_gridpoint,
            h_maxima=seg.h_maxima,
            reflectivity_var=cfg.global_.tracking_field,
            labels_var=CELL_LABELS_VAR,
            z_level=cfg.global_.z_level,
        )

    def __init__(self) -> None:
        self._segmenter: RadarCellSegmenter | None = None

    def run(self, context: dict) -> dict:
        config = context["detection_config"]
        ds_2d = context["grid_ds_2d"]

        if self._segmenter is None:
            self._segmenter = RadarCellSegmenter(config)

        segmented = self._segmenter.segment(ds_2d, context["grid_ds"])
        num_cells = int(segmented[config.labels_var].max().item())

        return {"segmented_ds": segmented, "num_cells": num_cells}


registry.register(DetectModule)
