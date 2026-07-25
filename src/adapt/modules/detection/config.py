# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Detection module config schema.

Holds exactly the fields RadarCellSegmenter consumes. Built once at startup by
DetectModule.build_config() from the resolved InternalConfig. Frozen.

`method_params` carries the resolved parameters for the *selected* method as a
plain dict (the pyart methods splat it as keyword arguments; `threshold` reads
`method_params["threshold"]`). It is a dict rather than a typed model so this
module stays decoupled from configuration/schemas.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict


class DetectionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    method: str
    method_params: dict[str, Any]
    closing_kernel: tuple[int, int]
    filter_by_size: bool
    min_cellsize_gridpoint: int
    max_cellsize_gridpoint: int | None
    h_maxima: float
    reflectivity_var: str
    labels_var: str
    z_level: float
