# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Projection stage contract.

Enforces that after optical flow computation, motion vectors and optional
projection arrays are present and structurally valid.
"""

import xarray as xr

from adapt.contracts.pipeline import require


def assert_projected(ds: xr.Dataset, max_steps: int = 5) -> None:
    """Enforce projection stage contract.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset from projector.project()
    max_steps : int, optional
        Maximum number of projection steps (default 5). If dataset has
        'max_projection_steps' in attrs, that value is used instead.

    Raises
    ------
    ContractViolation
        If any invariant is violated
    """
    require(
        "heading_x" in ds.data_vars,
        "Projection contract violated: missing 'heading_x' ",
    )
    require(
        "heading_y" in ds.data_vars,
        "Projection contract violated: missing 'heading_y' ",
    )

    if "cell_projections" in ds.data_vars:
        projections = ds["cell_projections"]
        require(
            projections.ndim == 3,
            f"Projection contract violated: 'cell_projections' has {projections.ndim} dims, "
            "expected 3 (step, y, x)",
        )
        max_steps_actual = ds.attrs.get("max_projection_steps", max_steps)
        num_steps = projections.shape[0]
        expected_steps = max_steps_actual + 1
        require(
            num_steps == expected_steps,
            f"Projection contract violated: found {num_steps} steps, expected {expected_steps} "
            f"(1 registration + {max_steps_actual} projections from config)",
        )
        _assert_registration_minutes(ds)
        _assert_projection_minutes(ds)


def _assert_registration_minutes(ds: xr.Dataset) -> None:
    """Minute-resolution registration masks must accompany cell_projections."""
    require(
        "registration_minutes" in ds.data_vars,
        "Projection contract violated: missing 'registration_minutes' — the "
        "minute-resolution registration masks must be produced with the projections",
    )
    masks = ds["registration_minutes"]
    require(
        masks.dims == ("minute", "y", "x"),
        f"Projection contract violated: 'registration_minutes' dims {masks.dims}, "
        "expected ('minute', 'y', 'x')",
    )
    require(
        "interpolation_fraction" in ds.coords,
        "Projection contract violated: missing 'interpolation_fraction' coordinate "
        "on the minute dimension",
    )
    if masks.sizes["minute"] == 0:
        return
    fractions = ds["interpolation_fraction"].values
    require(
        bool((fractions > 0).all() and (fractions <= 1).all()),
        "Projection contract violated: interpolation_fraction values must lie in (0, 1]",
    )
    minutes = ds["minute"].values
    require(
        bool((minutes[1:] > minutes[:-1]).all()),
        "Projection contract violated: 'minute' coordinate must be strictly increasing",
    )


def _assert_projection_minutes(ds: xr.Dataset) -> None:
    """Minute-resolution forward projection masks must accompany cell_projections."""
    require(
        "projection_minutes" in ds.data_vars,
        "Projection contract violated: missing 'projection_minutes' — the "
        "minute-resolution forward projection masks must be produced with the projections",
    )
    masks = ds["projection_minutes"]
    require(
        masks.dims == ("projection_minute", "y", "x"),
        f"Projection contract violated: 'projection_minutes' dims {masks.dims}, "
        "expected ('projection_minute', 'y', 'x')",
    )
    require(
        "projection_fraction" in ds.coords,
        "Projection contract violated: missing 'projection_fraction' coordinate "
        "on the projection_minute dimension",
    )
    if masks.sizes["projection_minute"] == 0:
        return
    fractions = ds["projection_fraction"].values
    require(
        bool((fractions > 0).all()),
        "Projection contract violated: projection_fraction values must be positive "
        "(they may exceed 1 for horizons beyond one scan gap)",
    )
    minutes = ds["projection_minute"].values
    require(
        bool((minutes[1:] > minutes[:-1]).all()),
        "Projection contract violated: 'projection_minute' coordinate must be strictly increasing",
    )


def check_projected_ds(ds: xr.Dataset) -> None:
    """Bound contract for the standard projected dataset."""
    assert_projected(ds)
