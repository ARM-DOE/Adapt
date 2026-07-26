# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tracking module config schema.

Holds exactly the fields CellTracker consumes (geometry-first gate/buffer/
length-scale params + cell_uid params flattened). Built once at startup by
TrackingModule.build_config() from the resolved InternalConfig. Frozen.
"""

from pydantic import BaseModel, ConfigDict


class TrackingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    split_overlap: float
    core_field_threshold: float
    uid_time_step_s: int
    uid_latlon_step_deg: float
    uid_area_step_km2: float
    uid_width: int
    field_var: str
    labels_var: str
    max_tracking_gap_minutes: float
    max_speed_ms: float
    max_speed_multiplier: float
    heading_change_penalty_weight: float
    projected_hull_buffer_km: float
    minimum_candidate_overlap: float
    minimum_projected_overlap: float
    length_scale: str
    geometry_length_scale_km: float
