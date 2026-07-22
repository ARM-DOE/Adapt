import logging

import numpy as np
import pytest

from adapt.modules.projection.module import RadarCellProjector

pytestmark = pytest.mark.unit


def test_normalize_constant_field(make_projection_config):
    """Projector normalizes constant fields correctly."""
    config = make_projection_config()
    proj = RadarCellProjector(config)

    a = np.ones((4, 4), dtype=np.float32) * 10
    b = np.ones((4, 4), dtype=np.float32) * 10

    a_n, b_n = proj._normalize(a, b)

    assert a_n.dtype == np.uint8
    assert b_n.dtype == np.uint8


def test_fill_concave_hull_small_object_falls_back(make_projection_config):
    """Projector falls back for small objects in concave hull fill."""
    config = make_projection_config()
    proj = RadarCellProjector(config)

    mask = np.zeros((5, 5), dtype=bool)
    mask[2, 2] = True

    filled = proj._fill_concave_hull(mask)

    assert filled.any()


def test_fill_concave_hull_collinear_points_no_qhull_warning(make_projection_config, caplog):
    """Rank-deficient (collinear) points must not log a QhullError warning.

    A column of >=4 set pixels is < 2-D, so a Delaunay triangulation raises QH6013
    ("input is less than 3-dimensional"). The old code caught it and logged the full
    multi-line qhull dump as a WARNING for every such cell — pure clutter. The
    projector must detect degeneracy up front and fall back to dilation silently.
    """
    config = make_projection_config()
    proj = RadarCellProjector(config)

    mask = np.zeros((8, 8), dtype=bool)
    mask[1:6, 3] = True  # 5 collinear points in a single column -> rank 1

    with caplog.at_level(logging.WARNING, logger="adapt.modules.projection.module"):
        filled = proj._fill_concave_hull(mask)

    assert filled.any()  # dilation fallback still fills the line
    assert not [r for r in caplog.records if "oncave hull" in r.getMessage()]
