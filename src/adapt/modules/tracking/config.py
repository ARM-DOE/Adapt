# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tracking module config schema.

Holds exactly the fields CellTracker consumes (tracker v2 rule parameters
+ cell_uid params flattened). Built once at startup by
TrackingModule.build_config() from the resolved InternalConfig. Frozen.
"""

from pydantic import BaseModel, ConfigDict


class TrackingConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    uid_width: int
    field_var: str
    labels_var: str
    max_tracking_gap_minutes: float
    cell_threshold: float  # f_thr: excess e = polarity·(f − f_thr)₊
    polarity: int  # +1 above threshold (reflectivity), −1 below (brightness temperature)
    growth_max_km: float  # r_max
    growth_size_km: float  # ℓ0
    max_overlap_mismatch: float  # u_max
    max_link_cost: float  # c_max
    residual_weight: float  # w_d
    heading_weight: float  # w_h
    max_speed_ms: float  # v_max
    max_acceleration_ms2: float  # a_max
    split_overlap: float  # τ_s on O^c
    merge_overlap: float  # τ_m on O^h
    latent_scans: int  # N
    identity_intensity_weight: float  # γ
    identity_score_margin: float  # δ_S
    core_field_threshold: float  # diagnostic core area only
