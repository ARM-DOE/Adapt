# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Flash -> cell attribution by initiation point on the projected metres grid.

Synthetic grids with analytically known nearest-cell distances. No IO.
"""

import numpy as np
import pytest

from adapt.modules.lma.attribution import attribute_flashes

pytestmark = pytest.mark.unit

DX = 200.0  # metres


def _grid():
    # 11x11 grid, 200 m spacing, one cell (label 1) at the single centre pixel (5,5).
    n = 11
    coords = np.arange(n) * DX
    labels = np.zeros((n, n), dtype=np.int32)
    labels[5, 5] = 1
    lut = np.array(["NONE", "uid-A"], dtype=np.str_)
    return labels, coords, lut


def test_flash_inside_cell_is_attributed_with_zero_distance():
    labels, coords, lut = _grid()
    # centre pixel (5,5) is at x=1000, y=1000
    uids, dist = attribute_flashes(
        np.array([1000.0]), np.array([1000.0]), labels, coords, coords, lut
    )
    assert uids[0] == "uid-A"
    assert dist[0] == 0.0


def test_flash_one_pixel_outside_within_500m_attributes_to_nearest():
    labels, coords, lut = _grid()
    # x=1200 (pixel 6), y=1000 (pixel 5): 200 m from the cell at (5,5)
    uids, dist = attribute_flashes(
        np.array([1200.0]), np.array([1000.0]), labels, coords, coords, lut
    )
    assert uids[0] == "uid-A"
    assert dist[0] == pytest.approx(200.0)


def test_flash_beyond_500m_is_unattributed():
    labels, coords, lut = _grid()
    # x=1600 (pixel 8) -> 600 m away, beyond the 500 m search radius
    uids, dist = attribute_flashes(
        np.array([1600.0]), np.array([1000.0]), labels, coords, coords, lut
    )
    assert uids[0] == "UNATTRIBUTED"
    assert np.isnan(dist[0])


def test_flash_outside_grid_is_unattributed():
    labels, coords, lut = _grid()
    uids, dist = attribute_flashes(
        np.array([-5000.0]), np.array([1000.0]), labels, coords, coords, lut
    )
    assert uids[0] == "UNATTRIBUTED"
    assert np.isnan(dist[0])


def test_all_background_grid_attributes_nothing():
    coords = np.arange(11) * DX
    labels = np.zeros((11, 11), dtype=np.int32)
    lut = np.array(["NONE"], dtype=np.str_)
    uids, dist = attribute_flashes(
        np.array([1000.0, 200.0]), np.array([1000.0, 400.0]), labels, coords, coords, lut
    )
    assert list(uids) == ["UNATTRIBUTED", "UNATTRIBUTED"]


def test_vectorized_over_many_flashes():
    labels, coords, lut = _grid()
    fx = np.array([1000.0, 1200.0, 1600.0])
    fy = np.array([1000.0, 1000.0, 1000.0])
    uids, dist = attribute_flashes(fx, fy, labels, coords, coords, lut)
    assert list(uids) == ["uid-A", "uid-A", "UNATTRIBUTED"]
