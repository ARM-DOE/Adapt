# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Grid stage contract.

Enforces that after regridding, the dataset is a valid 2D Cartesian grid
suitable for downstream segmentation and projection.
"""

import xarray as xr

from adapt.contracts.pipeline import require


def assert_gridded(ds: xr.Dataset, reflectivity_var: str) -> None:
    """Enforce grid stage contract.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset from loader.load_and_regrid()
    reflectivity_var : str
        Name of reflectivity variable (from config)

    Raises
    ------
    ContractViolation
        If any invariant is violated
    """
    require("x" in ds.coords, "Grid contract violated: missing 'x' coordinate")
    require("y" in ds.coords, "Grid contract violated: missing 'y' coordinate")
    require(
        reflectivity_var in ds.data_vars,
        f"Grid contract violated: missing '{reflectivity_var}' variable",
    )
    refl = ds[reflectivity_var]
    require(
        refl.ndim == 2,
        f"Grid contract violated: '{reflectivity_var}' has {refl.ndim} dims, expected 2",
    )


def check_grid_ds_2d(ds: xr.Dataset) -> None:
    """Bound contract for the standard 2D grid output.

    Structural only: x/y present, dataset non-empty. Which FIELD must be
    present depends on config (global_.tracking_field) and is validated by
    the detection module at its own boundary — bound contracts have no
    config access by design.
    """
    require("x" in ds.coords, "Grid contract violated: missing 'x' coordinate")
    require("y" in ds.coords, "Grid contract violated: missing 'y' coordinate")
    require(len(ds.data_vars) > 0, "Grid contract violated: no data variables")
