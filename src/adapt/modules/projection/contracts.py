# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Output contracts for the projection module.

The projection module produces cell projections with motion vectors. This
module defines the contract that validates the projection output structure.

Enforces the guarantee that when projections are computed (2+ frames),
the flow fields and projection arrays are present and well-formed.
"""

import xarray as xr

from adapt.modules.base import require


def assert_projected(ds: xr.Dataset, max_steps: int = 5) -> None:
    """Enforce projection stage contract.

    Called after projection computation (when 2+ frames available).
    Verifies that optical flow and projected labels are present and that
    projection count matches runtime config (read from dataset attributes).

    Parameters
    ----------
    ds : xr.Dataset
        Dataset from projector.project()

    max_steps : int, optional
        Maximum number of projection steps (default 5). If dataset has
        'max_projection_steps' in attrs, that value is used instead.
        This enables config-aware validation without breaking validator isolation.

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

        # Use stored config value if available (self-describing data pattern)
        # This allows validators to access runtime config without context coupling
        max_steps_actual = ds.attrs.get("max_projection_steps", max_steps)

        num_steps = projections.shape[0]
        expected_steps = max_steps_actual + 1  # 1 registration + N future
        require(
            num_steps == expected_steps,
            f"Projection contract violated: found {num_steps} steps, expected {expected_steps} (1 registration + {max_steps_actual} projections from config)"
        )
