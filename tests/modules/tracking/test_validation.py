# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for the bidirectional-overlap GeometricValidator."""

import numpy as np
import pytest

from adapt.modules.tracking.matching.validation import GeometricValidator

pytestmark = pytest.mark.unit


def _mask(shape, r0, r1, c0, c1):
    m = np.zeros(shape, dtype=bool)
    m[r0:r1, c0:c1] = True
    return m


def test_well_matched_pair_passes():
    hull = _mask((8, 8), 2, 6, 2, 6)  # 16 px
    cell = _mask((8, 8), 2, 6, 2, 6)  # identical → Opc=Ocp=1
    v = GeometricValidator(0.2, 0.2).validate(hull, cell)
    assert v.passed and v.opc == pytest.approx(1.0) and v.ocp == pytest.approx(1.0)


def test_tiny_cell_inside_large_hull_rejected():
    hull = _mask((10, 10), 0, 10, 0, 10)  # 100 px
    cell = _mask((10, 10), 4, 6, 4, 6)  # 4 px fully inside
    v = GeometricValidator(0.2, 0.2).validate(hull, cell)
    # Opc = 4/4 = 1.0 (candidate fully covered) but Ocp = 4/100 = 0.04 → gate fails
    assert v.opc == pytest.approx(1.0)
    assert v.ocp == pytest.approx(0.04)
    assert not v.passed


def test_large_cell_engulfing_prediction_rejected():
    hull = _mask((10, 10), 4, 6, 4, 6)  # 4 px prediction
    cell = _mask((10, 10), 0, 10, 0, 10)  # 100 px engulfs it
    v = GeometricValidator(0.2, 0.2).validate(hull, cell)
    # Ocp = 4/4 = 1.0 but Opc = 4/100 = 0.04 → gate fails
    assert v.ocp == pytest.approx(1.0)
    assert v.opc == pytest.approx(0.04)
    assert not v.passed


def test_grazing_contact_rejected():
    hull = _mask((10, 10), 0, 5, 0, 5)  # 25 px
    cell = _mask((10, 10), 4, 9, 4, 9)  # 25 px, overlaps only rows/cols 4 → 1 px
    v = GeometricValidator(0.2, 0.2).validate(hull, cell)
    assert v.opc == pytest.approx(1.0 / 25.0)
    assert v.ocp == pytest.approx(1.0 / 25.0)
    assert not v.passed
