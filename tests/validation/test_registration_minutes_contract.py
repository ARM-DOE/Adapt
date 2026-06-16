# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Projection contract: registration_minutes accompanies cell_projections."""

import numpy as np
import pytest
import xarray as xr

from adapt.contracts import ContractViolation, check_projected_ds

pytestmark = pytest.mark.unit


def _projected_ds(n_minutes=3, fractions=None, minutes=None) -> xr.Dataset:
    shape = (4, 4)
    coords = {"y": np.arange(4), "x": np.arange(4)}
    max_steps = 1
    if minutes is None:
        minutes = np.array(
            ["2024-05-18T19:01", "2024-05-18T19:02", "2024-05-18T19:03"],
            dtype="datetime64[ns]",
        )[:n_minutes]
    if fractions is None:
        fractions = np.linspace(1 / 3, 1.0, num=n_minutes, dtype=np.float32)

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
        },
        coords={**coords, "frame_offset": [0, 1], "minute": minutes},
        attrs={"max_projection_steps": max_steps},
    )
    return ds.assign_coords(interpolation_fraction=("minute", fractions))


class TestRegistrationMinutesContract:
    def test_passes_on_valid_ds(self):
        check_projected_ds(_projected_ds())

    def test_passes_on_empty_minute_dimension(self):
        check_projected_ds(_projected_ds(n_minutes=0))

    def test_fails_when_missing_alongside_projections(self):
        ds = _projected_ds().drop_vars("registration_minutes")
        with pytest.raises(ContractViolation, match="registration_minutes"):
            check_projected_ds(ds)

    def test_fails_without_interpolation_fraction(self):
        ds = _projected_ds().drop_vars("interpolation_fraction")
        with pytest.raises(ContractViolation, match="interpolation_fraction"):
            check_projected_ds(ds)

    def test_fails_on_fraction_outside_unit_interval(self):
        ds = _projected_ds(fractions=np.array([0.5, 1.0, 1.5], dtype=np.float32))
        with pytest.raises(ContractViolation, match="interpolation_fraction"):
            check_projected_ds(ds)

    def test_fails_on_non_increasing_minutes(self):
        minutes = np.array(
            ["2024-05-18T19:03", "2024-05-18T19:02", "2024-05-18T19:01"],
            dtype="datetime64[ns]",
        )
        with pytest.raises(ContractViolation, match="minute"):
            check_projected_ds(_projected_ds(minutes=minutes))
