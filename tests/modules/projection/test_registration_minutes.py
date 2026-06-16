# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""registration_minutes: previous scan's labels advected minute by minute.

The flow field is patched to a known constant so every assertion is analytic:
a uniform flow of u px/frame over a Δt-minute gap must displace the previous
labels by u·(minutes_elapsed/Δt) px at each whole minute in (t_prev, t_curr].
"""

from datetime import datetime
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.modules.projection import module as projection_module
from adapt.modules.projection.module import RadarCellProjector

pytestmark = pytest.mark.unit

_SHAPE = (20, 20)
_BLOCK = (slice(5, 8), slice(5, 8))  # 3x3 cell


def _frame(time: str, with_cell: bool) -> xr.Dataset:
    labels = np.zeros(_SHAPE, dtype=np.int32)
    refl = np.zeros(_SHAPE, dtype=np.float32)
    if with_cell:
        labels[_BLOCK] = 1
        refl[_BLOCK] = 40.0
    ds = xr.Dataset(
        {"reflectivity": (("y", "x"), refl), "cell_labels": (("y", "x"), labels)},
        coords={"y": np.arange(_SHAPE[0]), "x": np.arange(_SHAPE[1])},
    )
    return ds.assign_coords(time=np.datetime64(time))


def _pair(t_prev: str, t_curr: str) -> list[xr.Dataset]:
    """Previous frame holds the cell; current frame is empty (registration only)."""
    return [_frame(t_prev, with_cell=True), _frame(t_curr, with_cell=False)]


@pytest.fixture
def constant_flow(monkeypatch):
    """Patch Farneback with a uniform 6 px/frame flow in +x."""

    def fake_flow(prev, curr, _none, **kwargs):
        flow = np.zeros((*prev.shape, 2), dtype=np.float32)
        flow[:, :, 0] = 6.0
        return flow

    monkeypatch.setattr(projection_module.cv2, "calcOpticalFlowFarneback", fake_flow)


def _project(pair, projection_module_config):
    return RadarCellProjector(projection_module_config).project(pair)


def test_minutes_cover_the_open_closed_scan_interval(constant_flow, projection_module_config):
    """Whole minutes m with t_prev < m <= t_curr, even for off-minute scan times."""
    out = _project(_pair("2024-05-18T19:00:23", "2024-05-18T19:07:09"), projection_module_config)

    minutes = pd.to_datetime(out["registration_minutes"]["minute"].values)
    expected = pd.date_range("2024-05-18T19:01:00", "2024-05-18T19:07:00", freq="1min")
    assert list(minutes) == list(expected)

    fractions = out["interpolation_fraction"].values
    assert (fractions > 0).all() and (fractions <= 1).all()
    assert (np.diff(fractions) > 0).all()
    assert out.attrs["registration_source_scan_time"] == "2024-05-18T19:00:23"
    assert out.attrs["registration_target_scan_time"] == "2024-05-18T19:07:09"


def test_uniform_flow_displaces_labels_fractionally(constant_flow, projection_module_config):
    """6 px/frame over 6 min ⇒ the cell sits k px right of its origin at minute k."""
    out = _project(_pair("2024-05-18T19:00:00", "2024-05-18T19:06:00"), projection_module_config)

    frames = out["registration_minutes"]
    assert frames.sizes["minute"] == 6
    for k in range(1, 7):
        expected = np.zeros(_SHAPE, dtype=np.int32)
        expected[_BLOCK[0], slice(5 + k, 8 + k)] = 1
        np.testing.assert_array_equal(
            frames.isel(minute=k - 1).values,
            expected,
            err_msg=f"minute {k}: block should be displaced {k} px in +x",
        )


def test_final_minute_frame_equals_registration(constant_flow, projection_module_config):
    """fraction=1 advection is the registration: identical to cell_projections[0]."""
    out = _project(_pair("2024-05-18T19:00:00", "2024-05-18T19:06:00"), projection_module_config)

    np.testing.assert_array_equal(
        out["registration_minutes"].isel(minute=-1).values,
        out["cell_projections"].sel(frame_offset=0).values,
    )


def test_subminute_gap_yields_empty_minute_dimension(constant_flow, projection_module_config):
    out = _project(_pair("2024-05-18T19:00:00", "2024-05-18T19:00:30"), projection_module_config)

    assert "registration_minutes" in out
    assert out["registration_minutes"].sizes["minute"] == 0


def test_step_minutes_follows_epoch_aligned_grid(constant_flow, make_projection_config):
    """step=2 keeps a stable 2-minute grid (19:02, 19:04, 19:06), not per-scan offsets."""
    from adapt.configuration.schemas.user import UserProjectorConfig

    config = make_projection_config(projector=UserProjectorConfig(registration_step_minutes=2))
    out = RadarCellProjector(config).project(_pair("2024-05-18T19:00:00", "2024-05-18T19:06:00"))

    minutes = pd.to_datetime(out["registration_minutes"]["minute"].values)
    expected = pd.to_datetime(["2024-05-18T19:02", "2024-05-18T19:04", "2024-05-18T19:06"])
    assert list(minutes) == list(expected)


def _cftime_value():
    """cftime calendar datetime — how xarray decodes pyart grid NetCDF times."""
    cftime = pytest.importorskip("cftime")
    return cftime.DatetimeGregorian(2025, 5, 2, 16, 4, 35)


@pytest.mark.parametrize(
    "make_raw_time",
    [
        lambda: np.array(datetime(2025, 5, 2, 16, 4, 35), dtype=object),  # 0-d object
        lambda: np.array("2025-05-02T16:04:35", dtype="datetime64[s]"),  # 0-d non-ns
        lambda: np.array(["2025-05-02T16:04:35"], dtype="datetime64[ns]"),  # length-1
        lambda: np.datetime64("2025-05-02T16:04:35"),  # plain scalar
        _cftime_value,  # cftime calendar object (KHTX live failure 2026-06-11)
        lambda: np.array(_cftime_value(), dtype=object),  # 0-d array of cftime
    ],
    ids=["0d-object", "0d-datetime64s", "len1-array", "scalar", "cftime", "0d-cftime"],
)
def test_frame_time_unwraps_every_time_coord_shape(make_raw_time):
    """Production grids carry time in several shapes; pd.Timestamp rejects 0-d
    ndarrays and cftime objects directly (both seen live on KHTX data).
    _frame_time must normalize them all."""
    ds = SimpleNamespace(time=SimpleNamespace(values=make_raw_time()))

    assert RadarCellProjector._frame_time(ds) == pd.Timestamp("2025-05-02T16:04:35")
