# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Scientific correctness tests for RadarCellAnalyzer area and lat/lon outputs.

These tests verify that area computation and geographic centroid outputs are
numerically correct, not just present. All datasets are constructed inline
with analytically known ground-truth values.
"""

import numpy as np
import pytest
import xarray as xr

pytestmark = pytest.mark.unit

# Grid parameters shared across tests:
#   10×10 grid, 1 km pixel spacing (1000 m), cell at rows/cols 4:6 (4 pixels)
_H, _W = 10, 10
_SPACING_M = 1000.0  # 1 km pixels
_PIXEL_AREA_KM2 = 1.0  # (1000 × 1000) / 1e6


def _make_ds(
    refl_override: np.ndarray | None = None,
    with_latlon: bool = True,
    with_projections: bool = False,
    projection_steps: int = 1,
) -> xr.Dataset:
    """10×10 dataset with a 4-pixel cell at rows[4:6, 4:6].

    Reflectivity defaults: cell pixels = 45.0, background = 5.0.
    """
    refl = np.full((_H, _W), 5.0, dtype=np.float32)
    if refl_override is not None:
        refl = refl_override
    else:
        refl[4:6, 4:6] = 45.0

    labels = np.zeros((_H, _W), dtype=np.int32)
    labels[4:6, 4:6] = 1

    x_coords = np.arange(_W) * _SPACING_M
    y_coords = np.arange(_H) * _SPACING_M

    data_vars: dict = {
        "reflectivity": (("y", "x"), refl),
        "cell_labels": (("y", "x"), labels),
    }

    if with_projections:
        # frame_offset 0 = registration; 1..n = forward projections
        n_frames = projection_steps + 1
        proj = np.stack([labels] * n_frames, axis=0).astype(np.int32)
        data_vars["cell_projections"] = (("frame_offset", "y", "x"), proj)
        frame_offsets = list(range(n_frames))
    else:
        frame_offsets = [0]

    ds = xr.Dataset(
        data_vars,
        coords={
            "y": y_coords,
            "x": x_coords,
        },
    )
    if with_projections:
        ds = ds.assign_coords(frame_offset=frame_offsets)

    ds = ds.assign_coords(time=np.datetime64("2024-06-01T12:00:00"))

    if with_latlon:
        # Uniform lat/lon grids: lat from 35.00 to 35.09, lon from -97.00 to -97.09
        lats = np.linspace(35.00, 35.09, _H)
        lons = np.linspace(-97.00, -97.09, _W)
        lat_grid, lon_grid = np.meshgrid(lats, lons, indexing="ij")  # shape (H, W)
        ds = ds.assign_coords(lat=(("y", "x"), lat_grid), lon=(("y", "x"), lon_grid))

    return ds


# ---------------------------------------------------------------------------
# Area computation
# ---------------------------------------------------------------------------


def test_area_matches_pixel_count_times_pixel_area(make_analysis_config):
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds())

    assert len(df) == 1
    # 4 pixels × 1.0 km²/pixel = 4.0 km²
    assert df.iloc[0]["cell_area_sqkm"] == pytest.approx(4.0 * _PIXEL_AREA_KM2, abs=0.01)


def test_area_40dbz_km2_correct(make_analysis_config):
    """Only pixels strictly > 40.0 dBZ count toward area_40dbz_km2."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    refl = np.full((_H, _W), 5.0, dtype=np.float32)
    refl[4, 4] = 45.0  # above threshold
    refl[4, 5] = 45.0  # above threshold
    refl[5, 4] = 35.0  # below threshold
    refl[5, 5] = 35.0  # below threshold

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds(refl_override=refl))

    assert len(df) == 1
    assert df.iloc[0]["area_40dbz_km2"] == pytest.approx(2.0 * _PIXEL_AREA_KM2, abs=0.01)


def test_area_40dbz_km2_zero_when_no_pixel_exceeds_threshold(make_analysis_config):
    from adapt.modules.analysis.module import RadarCellAnalyzer

    refl = np.full((_H, _W), 5.0, dtype=np.float32)
    refl[4:6, 4:6] = 38.0  # all below 40 dBZ

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds(refl_override=refl))

    assert df.iloc[0]["area_40dbz_km2"] == pytest.approx(0.0, abs=0.001)


# ---------------------------------------------------------------------------
# Geographic centroid correctness
# ---------------------------------------------------------------------------


def test_geom_centroid_lat_within_cell_bounds(make_analysis_config):
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds())

    row = df.iloc[0]
    # Cell rows 4:6 → lat indices 4 and 5
    lats = np.linspace(35.00, 35.09, _H)
    lat_min = min(lats[4], lats[5])
    lat_max = max(lats[4], lats[5])
    assert lat_min <= row["cell_centroid_geom_lat"] <= lat_max


def test_geom_centroid_lon_within_cell_bounds(make_analysis_config):
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds())

    row = df.iloc[0]
    # Cell cols 4:6 → lon indices 4 and 5
    lons = np.linspace(-97.00, -97.09, _W)
    lon_min = min(lons[4], lons[5])
    lon_max = max(lons[4], lons[5])
    assert lon_min <= row["cell_centroid_geom_lon"] <= lon_max


def test_mass_centroid_lat_lon_present_and_finite(make_analysis_config):
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds())

    row = df.iloc[0]
    assert "cell_centroid_mass_lat" in row
    assert "cell_centroid_mass_lon" in row
    assert np.isfinite(row["cell_centroid_mass_lat"])
    assert np.isfinite(row["cell_centroid_mass_lon"])


def test_maxdbz_centroid_at_reflectivity_peak(make_analysis_config):
    """Max dBZ centroid pixel coords point to the single reflectivity peak pixel."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    refl = np.full((_H, _W), 5.0, dtype=np.float32)
    # Cell at rows 4:6, cols 4:6; peak at (row=4, col=4)
    refl[4:6, 4:6] = 35.0
    refl[4, 4] = 60.0  # single peak

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(_make_ds(refl_override=refl))

    row = df.iloc[0]
    # row=4 → centroid_y, col=4 → centroid_x (pixel coordinates)
    assert row["cell_centroid_maxdbz_y"] == pytest.approx(4, abs=0.5)
    assert row["cell_centroid_maxdbz_x"] == pytest.approx(4, abs=0.5)


# ---------------------------------------------------------------------------
# No lat/lon coords — fallback to pixel-only mode
# ---------------------------------------------------------------------------


def test_no_latlon_coords_pixel_centroids_still_finite(make_analysis_config):
    """Dataset without lat/lon → extract() returns finite pixel centroids, no crash."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    ds = _make_ds(with_latlon=False)
    df = analyzer.extract(ds)

    assert len(df) == 1
    row = df.iloc[0]
    assert np.isfinite(row["cell_centroid_geom_x"])
    assert np.isfinite(row["cell_centroid_geom_y"])


def test_no_latlon_area_still_correct(make_analysis_config):
    """Area computation does not depend on lat/lon coords."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    ds = _make_ds(with_latlon=False)
    df = analyzer.extract(ds)

    assert df.iloc[0]["cell_area_sqkm"] == pytest.approx(4.0 * _PIXEL_AREA_KM2, abs=0.01)


# ---------------------------------------------------------------------------
# Projection centroid columns
# ---------------------------------------------------------------------------


def test_projection_centroid_registration_column_present(make_analysis_config):
    """With projections in dataset, registration centroid column appears."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    ds = _make_ds(with_projections=True, projection_steps=1)
    df = analyzer.extract(ds)

    assert len(df) == 1
    assert "cell_centroid_registration_x" in df.columns
    assert "cell_centroid_registration_y" in df.columns


def test_projection_centroid_forward_column_present(make_analysis_config):
    """With 2 projection steps, projection1 centroid column appears."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    ds = _make_ds(with_projections=True, projection_steps=2)
    df = analyzer.extract(ds)

    assert len(df) == 1
    assert "cell_centroid_projection1_x" in df.columns
    assert "cell_centroid_projection1_y" in df.columns


# ---------------------------------------------------------------------------
# Empty result contract
# ---------------------------------------------------------------------------


def test_no_cells_returns_empty_dataframe_with_required_columns(make_analysis_config):
    """All-zero labels → empty DataFrame with required contract columns."""
    from adapt.modules.analysis.module import RadarCellAnalyzer

    refl = np.full((_H, _W), 5.0, dtype=np.float32)
    labels = np.zeros((_H, _W), dtype=np.int32)
    ds = xr.Dataset(
        {
            "reflectivity": (("y", "x"), refl),
            "cell_labels": (("y", "x"), labels),
        },
        coords={
            "y": np.arange(_H) * _SPACING_M,
            "x": np.arange(_W) * _SPACING_M,
            "time": np.datetime64("2024-06-01T12:00:00"),
        },
    )

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(ds)

    assert len(df) == 0
    assert "cell_label" in df.columns
    assert "cell_area_sqkm" in df.columns
