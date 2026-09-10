# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Ingest module config schema.

Holds exactly the fields RadarDataLoader consumes. Built once at startup by
LoadModule.build_config() from the resolved InternalConfig. Frozen.
"""

from pydantic import BaseModel, ConfigDict


class IngestConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    file_format: str
    grid_shape: tuple[int, int, int]
    grid_limits: tuple[tuple[float, float], tuple[float, float], tuple[float, float]]
    roi_func: str
    min_radius: float
    weighting_function: str
    radar: str
    z_level: float
    z_coord: str
    time_coord: str
