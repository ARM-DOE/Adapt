# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for the pyart convective/stratiform segmentation methods.

Each method builds the convective mask from a pyart classifier over the full 3D
grid (via ``pyart.xradar.Xgrid``); the shared watershed backend then labels
cells. Inputs are synthetic: a gridded NetCDF with an analytically-placed
convective core, written with pyart and read back exactly as the ingest node's
``grid_nc_path`` artifact would be.
"""

import numpy as np
import pytest

pyart = pytest.importorskip("pyart")
import xarray as xr  # noqa: E402

from adapt.configuration.schemas.param import SegmenterConfig  # noqa: E402
from adapt.modules.detection.config import DetectionConfig  # noqa: E402
from adapt.modules.detection.module import RadarCellSegmenter  # noqa: E402

pytestmark = pytest.mark.unit

PYART_METHODS = [
    "conv_strat_raut",
    "conv_strat_yuter",
    "feature_detection",
    "steiner_conv_strat",
]

CORE_YX = (20, 20)  # convective core centre (grid index)
Z_LEVEL = 2000.0


def _method_params(method: str) -> dict:
    """The curated ParamConfig defaults for a method (as build_config resolves them)."""
    return getattr(SegmenterConfig(), f"{method}_params").model_dump()


def _config(method: str) -> DetectionConfig:
    return DetectionConfig(
        method=method,
        method_params=_method_params(method),
        closing_kernel=(1, 1),
        filter_by_size=True,
        min_cellsize_gridpoint=3,
        max_cellsize_gridpoint=None,
        h_maxima=3.0,
        reflectivity_var="reflectivity",
        labels_var="cell_labels",
        z_level=Z_LEVEL,
    )


@pytest.fixture
def grid_nc(tmp_path):
    """Synthetic 3D grid NetCDF: a strong convective core plus a broad stratiform blob."""
    nz, ny, nx = 11, 60, 60
    grid = pyart.testing.make_empty_grid(
        (nz, ny, nx), ((0.0, 10000.0), (-30000.0, 30000.0), (-30000.0, 30000.0))
    )
    yy, xx = np.meshgrid(np.arange(ny), np.arange(nx), indexing="ij")
    cy, cx = CORE_YX
    # Broad, intense core so every method's convective class survives its own
    # minimum-object-size filter (~1 km grid; e.g. Yuter/feature min_km2_size=10).
    field = 52.0 * np.exp(-(((yy - cy) ** 2 + (xx - cx) ** 2) / 60.0))  # convective core
    field += 16.0 * np.exp(-(((yy - 42) ** 2 + (xx - 42) ** 2) / 100.0))  # stratiform
    refl = np.repeat(field[None, :, :], nz, axis=0)
    grid.fields["reflectivity"] = {"data": np.ma.masked_invalid(refl), "units": "dBZ"}
    path = tmp_path / "grid.nc"
    pyart.io.write_grid(str(path), grid)
    return str(path)


@pytest.fixture
def ds_2d(grid_nc):
    """2D reflectivity slice at the analysis z-level (as the ingest node produces)."""
    with xr.open_dataset(grid_nc, decode_times=False) as g:
        z_idx = int(np.argmin(np.abs(g["z"].values - Z_LEVEL)))
        refl = g["reflectivity"].isel(time=0, z=z_idx).values
        coords = {"y": g["y"].values, "x": g["x"].values}
    return xr.Dataset({"reflectivity": (("y", "x"), refl)}, coords=coords)


@pytest.mark.parametrize("method", PYART_METHODS)
def test_output_satisfies_segmentation_contract(method, ds_2d, grid_nc):
    """Labels are integer, non-negative, 2D, and shaped like the input slice."""
    out = RadarCellSegmenter(_config(method)).segment(ds_2d, grid_nc)
    labels = out["cell_labels"]

    assert labels.dtype.kind in {"i", "u"}
    assert labels.ndim == 2
    assert labels.shape == ds_2d["reflectivity"].shape
    assert int(labels.values.min()) >= 0
    assert labels.attrs["method"] == method


@pytest.mark.parametrize("method", PYART_METHODS)
def test_convective_core_is_labelled(method, ds_2d, grid_nc):
    """A 48 dBZ compact core is convective for every method and yields a cell."""
    labels = RadarCellSegmenter(_config(method)).segment(ds_2d, grid_nc)["cell_labels"].values
    cy, cx = CORE_YX

    assert labels.max() >= 1
    assert labels[cy - 2 : cy + 3, cx - 2 : cx + 3].max() > 0


@pytest.mark.parametrize("method", PYART_METHODS)
def test_deterministic(method, ds_2d, grid_nc):
    """Identical inputs produce identical labels."""
    first = RadarCellSegmenter(_config(method)).segment(ds_2d, grid_nc)["cell_labels"].values
    second = RadarCellSegmenter(_config(method)).segment(ds_2d, grid_nc)["cell_labels"].values
    np.testing.assert_array_equal(first, second)


@pytest.mark.parametrize("method", PYART_METHODS)
def test_missing_grid_nc_path_raises(method, ds_2d):
    """A pyart method with no 3D grid file fails loudly (no silent fallback)."""
    with pytest.raises(RuntimeError, match="grid_nc_path"):
        RadarCellSegmenter(_config(method)).segment(ds_2d, None)


@pytest.mark.parametrize("method", PYART_METHODS)
def test_records_method_params_in_attrs(method, ds_2d, grid_nc):
    """The output records exactly the parameters passed to the method."""
    params = _method_params(method)
    attrs = RadarCellSegmenter(_config(method)).segment(ds_2d, grid_nc)["cell_labels"].attrs

    for key, value in params.items():
        expected = int(value) if isinstance(value, bool) else value
        assert attrs[key] == expected
    # raut default is Adapt-owned, not silently inherited from pyart
    if method == "conv_strat_raut":
        assert attrs["conv_scale_km"] == 25


def test_segmenter_config_accepts_every_method():
    """All five methods pass the SegmenterConfig Literal validation."""
    for method in [*PYART_METHODS, "threshold"]:
        assert SegmenterConfig(method=method).method == method
