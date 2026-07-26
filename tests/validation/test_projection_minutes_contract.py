# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Projection contract: projection_minutes accompanies cell_projections.

Unlike registration (fractions bounded to (0, 1]), forward-projection fractions
are only required to be positive — they exceed 1 for horizons beyond one scan gap.
"""

import numpy as np
import pytest
import xarray as xr

from adapt.contracts import ContractViolation, check_projected_ds

pytestmark = pytest.mark.unit


def _projected_ds(n_minutes=3, proj_fractions=None, proj_minutes=None) -> xr.Dataset:
    shape = (4, 4)
    coords = {"y": np.arange(4), "x": np.arange(4)}
    max_steps = 1
    reg_minutes = np.array(
        ["2024-05-18T19:01", "2024-05-18T19:02", "2024-05-18T19:03"],
        dtype="datetime64[ns]",
    )[:n_minutes]
    reg_fractions = np.linspace(1 / 3, 1.0, num=n_minutes, dtype=np.float32)
    if proj_minutes is None:
        proj_minutes = np.array(
            ["2024-05-18T19:07", "2024-05-18T19:08", "2024-05-18T19:09"],
            dtype="datetime64[ns]",
        )[:n_minutes]
    if proj_fractions is None:
        # forward fractions legitimately exceed 1
        proj_fractions = np.linspace(0.5, 1.5, num=n_minutes, dtype=np.float32)

    ds = xr.Dataset(
        {
            "heading_x": (("y", "x"), np.zeros(shape, np.float32)),
            "heading_y": (("y", "x"), np.zeros(shape, np.float32)),
            "cell_projections": (
                ("frame_offset", "y", "x"),
                np.zeros((max_steps + 1, *shape), np.int32),
            ),
            "registration_minutes": (
                ("minute", "y", "x"),
                np.zeros((n_minutes, *shape), np.int32),
            ),
            "projection_minutes": (
                ("projection_minute", "y", "x"),
                np.zeros((n_minutes, *shape), np.int32),
            ),
        },
        coords={
            **coords,
            "frame_offset": [0, 1],
            "minute": reg_minutes,
            "projection_minute": proj_minutes,
        },
        attrs={"max_projection_steps": max_steps},
    )
    ds = ds.assign_coords(interpolation_fraction=("minute", reg_fractions))
    return ds.assign_coords(projection_fraction=("projection_minute", proj_fractions))


class TestProjectionMinutesContract:
    def test_passes_on_valid_ds(self):
        check_projected_ds(_projected_ds())

    def test_fractions_above_one_pass(self):
        """Forward extrapolation past one scan gap is valid — must NOT raise."""
        check_projected_ds(_projected_ds(proj_fractions=np.array([0.5, 1.0, 1.5], np.float32)))

    def test_passes_on_empty_projection_minute_dimension(self):
        check_projected_ds(_projected_ds(n_minutes=0))

    def test_fails_when_missing_alongside_projections(self):
        ds = _projected_ds().drop_vars("projection_minutes")
        with pytest.raises(ContractViolation, match="projection_minutes"):
            check_projected_ds(ds)

    def test_fails_without_projection_fraction(self):
        ds = _projected_ds().drop_vars("projection_fraction")
        with pytest.raises(ContractViolation, match="projection_fraction"):
            check_projected_ds(ds)

    def test_fails_on_non_positive_fraction(self):
        ds = _projected_ds(proj_fractions=np.array([0.0, 0.5, 1.0], np.float32))
        with pytest.raises(ContractViolation, match="projection_fraction"):
            check_projected_ds(ds)

    def test_fails_on_non_increasing_projection_minutes(self):
        proj_minutes = np.array(
            ["2024-05-18T19:09", "2024-05-18T19:08", "2024-05-18T19:07"],
            dtype="datetime64[ns]",
        )
        with pytest.raises(ContractViolation, match="projection_minute"):
            check_projected_ds(_projected_ds(proj_minutes=proj_minutes))
