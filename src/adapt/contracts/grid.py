"""Grid stage contract.

Enforces the guarantee that after regridding, the dataset is valid
for downstream scientific processing.
"""

import xarray as xr
from adapt.contracts.base import require


def assert_gridded(ds: xr.Dataset, reflectivity_var: str) -> None:
    """Enforce grid stage contract.

    Called immediately after regridding. Verifies that the loader/regridder
    produced a valid Cartesian grid.

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
    require(
        "x" in ds.coords,
        "Grid contract violated: missing 'x' coordinate"
    )
    require(
        "y" in ds.coords,
        "Grid contract violated: missing 'y' coordinate"
    )
    require(
        reflectivity_var in ds.data_vars,
        f"Grid contract violated: missing '{reflectivity_var}' variable"
    )

    # Verify 2D structure (should be sliced at z-level already)
    refl = ds[reflectivity_var]
    require(
        refl.ndim == 2,
        f"Grid contract violated: '{reflectivity_var}' has {refl.ndim} dims, expected 2"
    )
