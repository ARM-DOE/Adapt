# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Config schema for the cell_volume_stats enrichment module.

Holds exactly what CellVolumeStatsAlgorithm consumes. Frozen. Built at startup by
the node's build_config from the resolved InternalConfig. Only pydantic + stdlib.
"""

from pydantic import BaseModel, ConfigDict, Field


class CellVolumeStatsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Variable names. reflectivity/labels are injected from global; the polarimetric
    # names are module-owned (not a global knob), so they default here.
    reflectivity_var: str = "reflectivity"
    zdr_var: str = "differential_reflectivity"
    kdp_var: str = "specific_differential_phase"
    rhohv_var: str = "cross_correlation_ratio"
    labels_var: str = "cell_labels"
    z_coord: str = "z"
    y_coord: str = "y"
    x_coord: str = "x"
    time_coord: str = "time"

    # dBZ thresholds for echo-top / volume features.
    thresholds: tuple[float, ...] = (10.0, 20.0, 30.0, 40.0, 50.0)
    # Vertical gaps (m) no larger than this are bridged when finding echo regions.
    gap_tolerance_m: float = Field(500.0, ge=0.0)
    # Threshold used for the storm-structure (layering) summary.
    structure_threshold: float = Field(30.0, ge=0.0)
