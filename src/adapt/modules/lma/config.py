# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Config schema for the LMA cell-statistics post-process module.

Holds exactly what the reader and the science class consume. Frozen. Built at
startup by the node's build_config from the resolved InternalConfig. Only pydantic
+ stdlib.
"""

from pydantic import BaseModel, ConfigDict, Field


class LMAConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Directory of LMA ASCII source files. Optional at config-build time so config
    # resolution never fails for an unselected module; the node raises loudly at
    # run() if it is missing — no silent default.
    input_dir: str | None = None
    # pyXLMA flash-clustering knobs (its documented defaults).
    flash_distance_m: float = Field(3000.0, gt=0.0)
    flash_time_s: float = Field(0.15, gt=0.0)
    # Nearest-cell attribution radius for a just-outside flash initiation point.
    search_radius_m: float = Field(500.0, ge=0.0)
