# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Flash lon/lat -> projected x/y on Adapt's azimuthal-equidistant (radar) grid."""

import numpy as np
import pytest

from adapt.modules.xlma_stat.geo import project_lonlat_to_xy

pytestmark = pytest.mark.unit

LAT0, LON0 = 40.0, -88.0


def test_origin_maps_to_zero():
    x, y = project_lonlat_to_xy(np.array([LON0]), np.array([LAT0]), LAT0, LON0)
    assert x[0] == pytest.approx(0.0, abs=1e-3)
    assert y[0] == pytest.approx(0.0, abs=1e-3)


def test_one_degree_north_is_about_111km_and_positive_y():
    x, y = project_lonlat_to_xy(np.array([LON0]), np.array([LAT0 + 1.0]), LAT0, LON0)
    assert x[0] == pytest.approx(0.0, abs=1.0)
    assert y[0] == pytest.approx(111195.0, rel=0.01)


def test_east_gives_positive_x():
    x, y = project_lonlat_to_xy(np.array([LON0 + 0.5]), np.array([LAT0]), LAT0, LON0)
    assert x[0] > 0.0
