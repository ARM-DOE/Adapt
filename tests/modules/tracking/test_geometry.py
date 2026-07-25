# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for geometry-first matching primitives (analytic values, no fixtures)."""

import math

import numpy as np
import pytest

from adapt.modules.tracking.matching.geometry import (
    bidirectional_overlap,
    buffer_pixels_from_km,
    dilate_hull,
    geometric_mismatch,
    length_scale,
    mask_centroid,
    mass_weighted_centroid,
    pair_cost,
)

pytestmark = pytest.mark.unit


def test_bidirectional_overlap_asymmetric():
    # hull = 4 px column, cell = 8 px, intersection = 2 px
    hull = np.zeros((4, 4), dtype=bool)
    hull[0:4, 0] = True  # 4 px
    cell = np.zeros((4, 4), dtype=bool)
    cell[0:2, 0:4] = True  # 8 px
    # intersection = column 0 rows 0..1 = 2 px
    opc, ocp = bidirectional_overlap(hull, cell)
    assert opc == pytest.approx(2.0 / 8.0)  # intersection / candidate
    assert ocp == pytest.approx(2.0 / 4.0)  # intersection / hull


def test_bidirectional_overlap_disjoint_is_zero():
    hull = np.zeros((4, 4), dtype=bool)
    hull[0, 0] = True
    cell = np.zeros((4, 4), dtype=bool)
    cell[3, 3] = True
    assert bidirectional_overlap(hull, cell) == (0.0, 0.0)


def test_dilate_hull_single_pixel_by_one():
    m = np.zeros((5, 5), dtype=bool)
    m[2, 2] = True
    out = dilate_hull(m, 1)
    assert out[1:4, 1:4].all()  # 3x3 block around centre
    assert int(out.sum()) == 9


def test_dilate_hull_zero_buffer_is_identity():
    m = np.zeros((3, 3), dtype=bool)
    m[1, 1] = True
    assert np.array_equal(dilate_hull(m, 0), m)


def test_buffer_pixels_from_km_rounds_up():
    # 1 km buffer at 500 m/pixel = 2 px; at 800 m/pixel = ceil(1.25) = 2
    assert buffer_pixels_from_km(1.0, 500.0) == 2
    assert buffer_pixels_from_km(1.0, 800.0) == 2
    assert buffer_pixels_from_km(2.0, 1000.0) == 2


def test_length_scale_equiv_diameter():
    # hull 100 px, pixel_area 1e6 m² (1 km²) → area 1e8 m² → diameter 2*sqrt(1e8/pi)
    L = length_scale("hull_equiv_diameter", 100.0, 50.0, 1e6, fixed_km=5.0)
    assert pytest.approx(2.0 * math.sqrt(1e8 / math.pi)) == L


def test_length_scale_sum_radii():
    L = length_scale("sum_radii", 100.0, 25.0, 1e6, fixed_km=5.0)
    expected = math.sqrt(1e8 / math.pi) + math.sqrt(25e6 / math.pi)
    assert pytest.approx(expected) == L


def test_length_scale_fixed_km_ignores_area():
    L = length_scale("fixed_km", 100.0, 25.0, 1e6, fixed_km=5.0)
    assert pytest.approx(5000.0) == L


def test_length_scale_unknown_raises():
    with pytest.raises(ValueError, match="Unknown length_scale"):
        length_scale("nope", 1.0, 1.0, 1.0, fixed_km=1.0)


def test_geometric_mismatch_perfect_and_none():
    assert geometric_mismatch(1.0, 1.0) == pytest.approx(0.0)
    assert geometric_mismatch(0.0, 0.0) == pytest.approx(1.0)
    # sqrt spreads: Opc=Ocp=0.25 → g=0.25 → m=0.75
    assert geometric_mismatch(0.25, 0.25) == pytest.approx(0.75)


def test_pair_cost_combines_overlap_and_normalised_displacement():
    # m = 1 - sqrt(0.25)*sqrt(0.25) = 0.75; d/L = 1000/2000 = 0.5 → cost 1.25
    assert pair_cost(0.25, 0.25, 1000.0, 2000.0) == pytest.approx(1.25)


def test_pair_cost_degenerate_length_falls_back_to_mismatch():
    assert pair_cost(0.25, 0.25, 1000.0, 0.0) == pytest.approx(0.75)


def test_mask_centroid_geometric():
    m = np.zeros((3, 3), dtype=bool)
    m[0, 0] = True
    m[0, 2] = True
    row, col = mask_centroid(m)
    assert (row, col) == pytest.approx((0.0, 1.0))


def test_mass_weighted_centroid_shifts_toward_high_field():
    field = np.zeros((1, 3), dtype=float)
    field[0, 0] = 1.0
    field[0, 2] = 9.0
    mask = np.ones((1, 3), dtype=bool)
    # weights = field - min = [1,0,9]-0 → but min within mask is 0 (col1) → weights [1,0,9]
    row, col = mass_weighted_centroid(field, mask)
    # weighted col = (1*0 + 0*1 + 9*2)/10 = 1.8 → pulled toward col 2
    assert col == pytest.approx(1.8)
    assert row == pytest.approx(0.0)


def test_mass_weighted_centroid_uniform_field_falls_back_to_geometric():
    field = np.full((1, 3), 5.0, dtype=float)
    mask = np.ones((1, 3), dtype=bool)
    row, col = mass_weighted_centroid(field, mask)
    assert (row, col) == pytest.approx((0.0, 1.0))
