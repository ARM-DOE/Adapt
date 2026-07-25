# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""projection_minutes: current scan's labels advected forward minute by minute.

The flow field is patched to a known constant so every assertion is analytic:
a uniform flow of u px/frame over a Δt-minute gap must displace the current
labels by u·(minutes_ahead/Δt) px at each whole minute in (t_curr, t_curr+H].
Forward fractions exceed 1 once the horizon reaches beyond one scan gap.
"""

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
    """Current frame holds the cell (forward projection advects current labels)."""
    return [_frame(t_prev, with_cell=False), _frame(t_curr, with_cell=True)]


@pytest.fixture
def constant_flow(monkeypatch):
    """Patch Farneback with a uniform 6 px/frame flow in +x."""

    def fake_flow(prev, curr, _none, **kwargs):
        flow = np.zeros((*prev.shape, 2), dtype=np.float32)
        flow[:, :, 0] = 6.0
        return flow

    monkeypatch.setattr(projection_module.cv2, "calcOpticalFlowFarneback", fake_flow)


def _project(pair, config):
    return RadarCellProjector(config).project(pair)


def test_minutes_cover_the_open_closed_forward_interval(constant_flow, projection_module_config):
    """Whole minutes m with t_curr < m <= t_curr + horizon, even off-minute scans."""
    out = _project(_pair("2024-05-18T19:00:23", "2024-05-18T19:07:09"), projection_module_config)

    minutes = pd.to_datetime(out["projection_minutes"]["projection_minute"].values)
    # default horizon = 15 min -> (19:07:09, 19:22:09] on the epoch-aligned 1-min grid
    expected = pd.date_range("2024-05-18T19:08:00", "2024-05-18T19:22:00", freq="1min")
    assert list(minutes) == list(expected)


def test_uniform_flow_extrapolates_labels_fractionally(constant_flow, projection_module_config):
    """6 px/frame over a 6-min gap ⇒ the cell sits k px right of origin at minute k."""
    out = _project(_pair("2024-05-18T19:00:00", "2024-05-18T19:06:00"), projection_module_config)

    frames = out["projection_minutes"]
    # k px displacement stays in-frame while 8+k <= 20, i.e. k <= 12.
    for k in range(1, 13):
        expected = np.zeros(_SHAPE, dtype=np.int32)
        expected[_BLOCK[0], slice(5 + k, 8 + k)] = 1
        np.testing.assert_array_equal(
            frames.isel(projection_minute=k - 1).values,
            expected,
            err_msg=f"minute {k}: block should be displaced {k} px in +x",
        )


def test_fraction_exceeds_one_beyond_one_scan_gap(constant_flow, projection_module_config):
    """Horizon (15) > gap (6): minutes past the gap carry fractions > 1."""
    out = _project(_pair("2024-05-18T19:00:00", "2024-05-18T19:06:00"), projection_module_config)

    fractions = out["projection_fraction"].values
    assert (fractions > 0).all()
    assert (np.diff(fractions) > 0).all()
    assert (fractions > 1).any()
    # minute 7 (past the 6-min gap) advects the cell more than one whole gap (6 px).
    seven = out["projection_minutes"].isel(projection_minute=6).values
    assert int(np.argwhere(seven).min(axis=0)[1]) == 5 + 7


def test_empty_projection_minute_dimension(constant_flow, make_projection_config):
    """A grid step larger than the horizon window yields an empty projection dim."""
    from adapt.configuration.schemas.user import UserProjectorConfig

    config = make_projection_config(
        projector=UserProjectorConfig(registration_step_minutes=5, projection_horizon_minutes=3)
    )
    out = RadarCellProjector(config).project(_pair("2024-05-18T19:00:00", "2024-05-18T19:01:00"))

    assert "projection_minutes" in out
    assert out["projection_minutes"].sizes["projection_minute"] == 0


def test_epoch_aligned_forward_grid(constant_flow, make_projection_config):
    """step=2 keeps a stable 2-minute grid (19:08, 19:10, 19:12), not per-scan offsets."""
    from adapt.configuration.schemas.user import UserProjectorConfig

    config = make_projection_config(
        projector=UserProjectorConfig(registration_step_minutes=2, projection_horizon_minutes=6)
    )
    out = RadarCellProjector(config).project(_pair("2024-05-18T19:00:00", "2024-05-18T19:06:00"))

    minutes = pd.to_datetime(out["projection_minutes"]["projection_minute"].values)
    expected = pd.to_datetime(["2024-05-18T19:08", "2024-05-18T19:10", "2024-05-18T19:12"])
    assert list(minutes) == list(expected)
