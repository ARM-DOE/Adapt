"""Projection stage contract.

Enforces the guarantee that when projections are computed (2+ frames),
the flow fields and projection arrays are present and well-formed.
"""

import xarray as xr
from adapt.contracts.base import require


def assert_projected(ds: xr.Dataset, max_steps: int = 5) -> None:
    """Enforce projection stage contract.

    Called after projection computation (when 2+ frames available).
    Verifies that optical flow and projected labels are present.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset from projector.project()

    max_steps : int, optional
        Maximum number of projection steps allowed (from config, default 5)

    Raises
    ------
    ContractViolation
        If any invariant is violated
    """
    require(
        "heading_x" in ds.data_vars,
        "Projection contract violated: missing 'heading_x' "
    )
    require(
        "heading_y" in ds.data_vars,
        "Projection contract violated: missing 'heading_y' "
    )

    # If projections are included, verify their structure
    if "cell_projections" in ds.data_vars:
        projections = ds["cell_projections"]
        require(
            projections.ndim == 3,
            f"Projection contract violated: 'cell_projections' has {projections.ndim} dims, expected 3 (step, y, x)"
        )

        num_steps = projections.shape[0]
        expected_steps = max_steps + 1
        require(
            num_steps == expected_steps,
            f"Projection contract violated: found {num_steps} steps, expected {expected_steps} (1 registration + {max_steps} projections)"
        )
