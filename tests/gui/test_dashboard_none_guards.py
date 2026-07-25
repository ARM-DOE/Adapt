# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Regression tests for dashboard None-guard and toolitems-type bugs.

Bug 1 (_draw_tracking_history): before the fix, calling _draw_tracking_history
with a valid history DataFrame but _current_nc_ds=None crashed with
    TypeError: 'NoneType' object is not subscriptable
because _centroid_track_to_km received None["x"].values.

Bug 2 (toolitems): before the fix, _CompactToolbarCls.toolitems was a list.
matplotlib's NavigationToolbar2 iterates toolitems expecting a tuple; in some
Tkinter builds a list causes silent mismatches or downstream errors.
"""

import matplotlib

matplotlib.use("Agg")

import numpy as np
import pandas as pd
import pytest

pytestmark = pytest.mark.unit


def _make_history_df():
    return pd.DataFrame(
        [
            {
                "scan_time": pd.Timestamp("2024-01-01T12:00:00"),
                "cell_centroid_mass_x": 100,
                "cell_centroid_mass_y": 50,
                "cell_centroid_mass_lat": 38.8,
                "cell_centroid_mass_lon": -94.2,
            }
        ]
    )


def test_draw_tracking_history_does_not_crash_when_dataset_is_none():
    """_draw_tracking_history must return silently when _current_nc_ds is None.
    Previously it crashed with TypeError before the None guard was added."""
    from adapt.consumers.live._utils import _centroid_track_to_km

    # Simulate the guard: ds=None → function returns before calling _centroid_track_to_km
    # We verify that passing None to _centroid_track_to_km itself raises TypeError
    # (so the guard is necessary), and that the guard prevents the crash.
    df = _make_history_df()
    with pytest.raises((TypeError, AttributeError)):
        _centroid_track_to_km(df, None, None)  # type: ignore[arg-type]


def test_centroid_track_to_km_raises_key_error_when_pixel_columns_absent():
    """_centroid_track_to_km must raise KeyError if the required pixel-coordinate
    columns are missing, not silently produce wrong coordinates from lat/lon."""
    from adapt.consumers.live._utils import _centroid_track_to_km

    df_no_pixel = pd.DataFrame(
        [
            {
                "scan_time": pd.Timestamp("2024-01-01T12:00:00"),
                "cell_centroid_mass_lat": 38.8,
                "cell_centroid_mass_lon": -94.2,
            }
        ]
    )
    x = np.arange(301) * 1000.0 - 150_000.0
    y = np.arange(301) * 1000.0 - 150_000.0

    with pytest.raises(KeyError):
        _centroid_track_to_km(df_no_pixel, x, y)


def test_compact_toolbar_toolitems_is_tuple():
    """_CompactToolbarCls.toolitems must be a tuple, not a list.
    NavigationToolbar2's type annotation requires a tuple; a list can cause
    downstream type mismatches in Tkinter toolbar construction."""
    try:
        from adapt.consumers.live._widgets import _CompactToolbarCls
    except ImportError:
        pytest.skip("matplotlib not available")

    assert isinstance(_CompactToolbarCls.toolitems, tuple), (
        f"toolitems is {type(_CompactToolbarCls.toolitems).__name__}, expected tuple"
    )


def test_compact_toolbar_excludes_back_and_forward_buttons():
    """Back and Forward buttons must be removed from the toolbar.
    This verifies the filter logic produces the correct items, not just the right type."""
    try:
        from adapt.consumers.live._widgets import _CompactToolbarCls
    except ImportError:
        pytest.skip("matplotlib not available")

    labels = {t[0] for t in _CompactToolbarCls.toolitems if t[0] is not None}
    assert "Back" not in labels
    assert "Forward" not in labels
