# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""compute_cell must be robust to dimension order and an extra time dim — the real
gridded NetCDF is (time, z, y, x), not the bare (z, y, x) the unit tests assumed."""

import numpy as np
import pytest
import xarray as xr

pytestmark = pytest.mark.unit

from adapt.modules.cell_volume_stats.config import CellVolumeStatsConfig  # noqa: E402
from adapt.modules.cell_volume_stats.module import CellVolumeStatsAlgorithm  # noqa: E402

Z = [0.0, 500.0, 1000.0, 1500.0]
Y = [0.0, 1000.0, 2000.0]
X = [0.0, 1000.0, 2000.0]
LABELS = np.zeros((3, 3), dtype=np.int32)
LABELS[1, 1] = 1  # one-pixel cell


def _dbz_zyx():
    dbz = np.full((4, 3, 3), np.nan)
    dbz[:2, 1, 1] = 40.0  # two defined levels at the cell pixel
    return dbz


def _grid(dims, data, coords):
    return xr.Dataset({"reflectivity": (dims, data)}, coords=coords)


def _run(grid):
    cfg = CellVolumeStatsConfig()
    return CellVolumeStatsAlgorithm(cfg).compute_cell(grid, LABELS, 1, "uid-1")


class TestDimRobustness:
    def _assert_expected(self, row):
        # 2 defined voxels * (1km^2 * 500m/1000) = 1.0 km^3; dbz_max = 40
        assert row["cell_volume_km3"] == pytest.approx(1.0)
        assert row["cell_area_km2"] == pytest.approx(1.0)
        assert row["dbz_max"] == pytest.approx(40.0)

    def test_canonical_zyx(self):
        g = _grid(["z", "y", "x"], _dbz_zyx(), {"z": Z, "y": Y, "x": X})
        self._assert_expected(_run(g))

    def test_time_z_y_x_like_real_netcdf(self):
        dbz = _dbz_zyx()[np.newaxis, ...]  # (1, 4, 3, 3)
        g = _grid(
            ["time", "z", "y", "x"],
            dbz,
            {"time": [np.datetime64("2024-01-01")], "z": Z, "y": Y, "x": X},
        )
        self._assert_expected(_run(g))

    def test_transposed_y_x_z(self):
        dbz = np.transpose(_dbz_zyx(), (1, 2, 0))  # (y, x, z)
        g = _grid(["y", "x", "z"], dbz, {"z": Z, "y": Y, "x": X})
        self._assert_expected(_run(g))
