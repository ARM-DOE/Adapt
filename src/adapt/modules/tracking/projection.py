# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Registration-based projected hulls.

The projection module already advects the previous scan's cell labels forward at
minute resolution (``registration_minutes`` + ``interpolation_fraction``). The
tracker consumes the minute frame closest to the actual scan gap so matching uses
a hull registered to the real elapsed time, not a fixed single-step projection.
Falls back to ``cell_projections[0]`` (the whole-step registration) when the
minute frames are absent. Pure xarray access — no state.
"""

import numpy as np
import xarray as xr

__all__ = ["select_registration_labels"]


def select_registration_labels(ds: xr.Dataset, dt_s: float) -> np.ndarray:
    """Previous-scan cell labels registered to the current frame.

    Picks the ``registration_minutes`` frame whose minute is closest to ``dt_s``;
    falls back to ``cell_projections[0]`` if no minute frames are present.
    """
    if "registration_minutes" in ds.data_vars and ds["registration_minutes"].sizes.get("minute", 0):
        minutes = np.asarray(ds["minute"].values, dtype=float)
        nearest = int(np.argmin(np.abs(minutes - dt_s / 60.0)))
        return np.asarray(ds["registration_minutes"].values[nearest])
    return np.asarray(ds["cell_projections"].values[0])
