# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

import numpy as np
import pandas as pd
import pytest

from adapt.consumers.live._utils import _centroid_track_to_km

pytestmark = pytest.mark.unit

# 301×301 grid, 1 km spacing, centred on radar
_X = np.arange(301) * 1000.0 - 150_000.0  # -150 000 … +150 000 m
_Y = np.arange(301) * 1000.0 - 150_000.0


def _row(col: int, row: int, lat: float = 38.0, lon: float = -94.0) -> dict:
    return {
        "scan_time": pd.Timestamp("2024-01-01T12:00:00"),
        "cell_centroid_mass_x": col,
        "cell_centroid_mass_y": row,
        "cell_centroid_mass_lat": lat,
        "cell_centroid_mass_lon": lon,
    }


def test_centroid_at_radar_origin_is_zero():
    # pixel (150, 150) is exactly the radar site: x=0, y=0
    df = pd.DataFrame([_row(150, 150)])
    x_km, y_km = _centroid_track_to_km(df, _X, _Y)
    assert x_km[0] == pytest.approx(0.0)
    assert y_km[0] == pytest.approx(0.0)


def test_centroid_matches_dataset_grid_exactly():
    # pixel col=25, row=30 → x=-125 km, y=-120 km
    df = pd.DataFrame([_row(25, 30)])
    x_km, y_km = _centroid_track_to_km(df, _X, _Y)
    assert x_km[0] == pytest.approx(-125.0)
    assert y_km[0] == pytest.approx(-120.0)


def test_centroid_does_not_use_lat_lon_approximation():
    # Far-range cell where lat/lon flat-Earth gives ~1.5 km error.
    # col=30, row=45 → x=-120 km, y=-105 km (exact dataset values).
    # A realistic lat/lon for that pixel gives a DIFFERENT value via the
    # flat-Earth formula, so this test fails if lat/lon is used.
    col, row = 30, 45
    expected_x_km = _X[col] / 1000.0  # -120.0 km exactly
    expected_y_km = _Y[row] / 1000.0  # -105.0 km exactly

    # Lat/lon that corresponds to this grid point for KEAX-like radar:
    # flat-Earth back-projection from these coords gives ≈ -118.4, -105.9
    df = pd.DataFrame([_row(col, row, lat=37.858, lon=-95.631)])
    x_km, y_km = _centroid_track_to_km(df, _X, _Y)

    assert x_km[0] == pytest.approx(expected_x_km), (
        f"Expected x={expected_x_km:.1f} km from pixel index, "
        f"got {x_km[0]:.3f} km — lat/lon approximation was used instead"
    )
    assert y_km[0] == pytest.approx(expected_y_km), (
        f"Expected y={expected_y_km:.1f} km from pixel index, "
        f"got {y_km[0]:.3f} km — lat/lon approximation was used instead"
    )


def test_centroid_multiple_positions():
    # History of three positions
    df = pd.DataFrame(
        [
            _row(50, 50),  # x=-100, y=-100
            _row(100, 100),  # x=-50,  y=-50
            _row(150, 150),  # x=0,    y=0
        ]
    )
    x_km, y_km = _centroid_track_to_km(df, _X, _Y)
    assert x_km == pytest.approx([-100.0, -50.0, 0.0])
    assert y_km == pytest.approx([-100.0, -50.0, 0.0])
