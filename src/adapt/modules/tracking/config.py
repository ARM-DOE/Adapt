# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tracking module config schema.

Holds exactly the fields RadarCellTracker consumes (cost thresholds and
cell_uid params flattened). Built once at startup by TrackingModule.build_config()
from the resolved InternalConfig. Frozen.
"""

from pydantic import BaseModel, ConfigDict


class TrackingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    match_cost: float
    keep_cost: float
    unmatch_cost: float
    split_overlap: float
    core_reflectivity_threshold: float
    uid_time_step_s: int
    uid_latlon_step_deg: float
    uid_area_step_km2: float
    uid_width: int
    reflectivity_var: str
    labels_var: str
    max_gap_minutes: float
    expected_speed_ms: float
    max_tracking_gap_minutes: float
    projection_horizon_minutes: float
    projection_interval_minutes: float
    max_speed_ms: float
    max_speed_multiplier: float
    overlap_match_threshold: float
    heading_change_penalty_weight: float
