# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

import logging

import numpy as np

from adapt.contracts import (
    CELL_LABELS_VAR,
    DecisionTableWrite,
    check_grid_ds_2d,
    check_prior_scan,
    check_seed_carry,
    check_segmentation_decisions,
    check_segmented_ds,
    require,
)
from adapt.execution.module_registry import registry
from adapt.modules.base import BaseModule
from adapt.modules.detection.config import DetectionConfig
from adapt.modules.detection.module import CarriedLabels, RadarCellSegmenter

logger = logging.getLogger(__name__)


def _projection_enabled(cfg) -> bool:
    """Whether the run enables ``projection``, by ``resolve_enabled_modules``' precedence.

    Mirrored here rather than imported: the resolver needs the module
    registry, which this file is being registered into.
    """
    if cfg.only_modules:
        return "projection" in cfg.only_modules
    if cfg.exclude_modules and "projection" in cfg.exclude_modules:
        return False
    return cfg.modules is None or "projection" in cfg.modules


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
    prior_scan : dict | None
        The previous completed scan's context, seeded by the processor (None
        on the first scan or after a too-large time gap). With
        ``segmenter.seed_carry`` on, its ``projected_ds`` (the previous
        frame's labels advected one flow step forward) and ``seed_carry``
        are handed to the segmenter as carried labels.
    config : InternalConfig
        Runtime configuration.

    Context outputs
    ---------------
    segmented_ds : xr.Dataset
        2D dataset with cell_labels variable added.
    num_cells : int
        Number of detected cells.
    seed_carry : tuple[int, ...]
        Carry age per cell label (index i -> label i + 1): 0 when seeded
        independently this scan, k when carried k consecutive scans. Empty
        with the flag off.
    """

    name = "detection"
    summary = "segment cells from the grid"
    required_history = 1
    pipeline_phase = 0
    inputs = ["grid_ds_2d", "grid_ds", "prior_scan", "detection_config"]
    # detection_decisions: every seed and carried claim this scan and what
    # became of it — analysis-only, written to decisions.db.
    outputs = ["segmented_ds", "num_cells", "seed_carry", "detection_decisions"]
    input_contracts = {"grid_ds_2d": check_grid_ds_2d, "prior_scan": check_prior_scan}
    output_contracts = {
        "segmented_ds": check_segmented_ds,
        "seed_carry": check_seed_carry,
        "detection_decisions": check_segmentation_decisions,
    }
    config_class = DetectionConfig
    persistence = (DecisionTableWrite(key="detection_decisions"),)

    @classmethod
    def build_config(cls, cfg) -> DetectionConfig:
        seg = cfg.segmenter
        # Checked at startup: at run time "prior scan has no projected_ds" is
        # also the legitimate state of the second scan, so it cannot be told
        # apart from a disabled projection module there.
        if seg.seed_carry_frames > 0 and not _projection_enabled(cfg):
            raise ValueError(
                "segmenter.seed_carry_frames > 0 needs the projection module (its advected "
                "labels are the seeds carried forward) — enable projection or set it to 0"
            )
        method_params = getattr(seg, f"{seg.method}_params").model_dump()
        return DetectionConfig(
            method=seg.method,
            method_params=method_params,
            closing_radius=seg.closing_radius,
            filter_by_size=seg.filter_by_size,
            min_cellsize_gridpoint=seg.min_cellsize_gridpoint,
            max_cellsize_gridpoint=seg.max_cellsize_gridpoint,
            h_maxima=seg.h_maxima,
            seed_carry_frames=seg.seed_carry_frames,
            seed_carry_min_separation=seg.seed_carry_min_separation,
            carried_exempt_size_filter=seg.carried_exempt_size_filter,
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

        carried = self._carried(context["prior_scan"], config)
        segmented = self._segmenter.segment(ds_2d, context["grid_ds"], carried)
        labels = segmented[config.labels_var]
        num_cells = int(labels.max().item())
        seed_carry = (
            tuple(int(age) for age in np.atleast_1d(labels.attrs["carry_age"]))
            if config.seed_carry_frames > 0 and num_cells
            else ()
        )

        return {
            "segmented_ds": segmented,
            "num_cells": num_cells,
            "seed_carry": seed_carry,
            "detection_decisions": self._segmenter.decisions(),
        }

    @staticmethod
    def _carried(prior: dict | None, config: DetectionConfig) -> CarriedLabels | None:
        """The previous scan's one-step-ahead projected labels with their carry ages.

        None with the flag off, without a prior scan, or when the prior has
        no motion field yet (projection first runs at the second scan and is
        skipped after a too-large gap): the carry is undefined without one.
        """
        if config.seed_carry_frames == 0 or prior is None:
            return None
        if "projected_ds" not in prior:
            logger.debug("seed_carry: prior scan has no projected_ds; nothing carried")
            return None

        ages = prior["seed_carry"]
        require(
            len(ages) == prior["num_cells"],
            f"seed_carry: prior scan carries {len(ages)} ages for {prior['num_cells']} cells",
        )
        projections = prior["projected_ds"]["cell_projections"]
        require(
            projections.sizes["frame_offset"] >= 2,
            "seed_carry needs projector.max_projection_steps >= 1 (no one-step-ahead labels)",
        )
        advected = np.asarray(projections.isel(frame_offset=1).values)
        require(
            int(advected.max()) <= len(ages),
            f"seed_carry: advected label {int(advected.max())} exceeds prior cell count",
        )
        return CarriedLabels(labels=advected, ages=tuple(ages))


registry.register(DetectModule)
