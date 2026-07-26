# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Projection module config schema.

Holds exactly the fields RadarCellProjector consumes (flow params flattened).
Built once at startup by ProjectionModule.build_config() from the resolved
InternalConfig. Frozen.
"""

from pydantic import BaseModel, ConfigDict


class ProjectionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    method: str
    nan_fill_value: float
    max_time_interval_minutes: int
    max_projection_steps: int
    pyr_scale: float
    levels: int
    winsize: int
    iterations: int
    poly_n: int
    poly_sigma: float
    flags: int
    min_motion_threshold: float
    max_flow_magnitude: float
    registration_step_minutes: int
    projection_horizon_minutes: int
    reflectivity_var: str
