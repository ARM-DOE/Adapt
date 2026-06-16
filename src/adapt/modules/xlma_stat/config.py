# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Config schema for the xlma_stat post-process module.

Holds exactly what the reader and the science class consume. Frozen. Built at
startup by the node's build_config from the resolved InternalConfig. Only pydantic
+ stdlib.
"""

from pydantic import BaseModel, ConfigDict, Field


class XlmaStatConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    # Directory containing xLMA flash-sorted NetCDF files (*.nc). Raw LYLOUT
    # ASCII must be converted to flash-sorted NetCDF in xLMA/pyxlma first;
    # non-NetCDF files in the directory are ignored.
    # Optional at config-build time so config resolution never fails for an
    # unselected module; the node raises loudly at run() if it is missing — no
    # silent default.
    input_dir: str | None = None
    # Nearest-cell attribution radius for a just-outside flash initiation point.
    search_radius_m: float = Field(500.0, ge=0.0)
