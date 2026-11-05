# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Geometry of tracker v2: growth, grown overlaps, core centre, shape residual.

Synthetic masks with analytically known answers; no fixtures.
"""

import math

import numpy as np
import pytest

from adapt.modules.tracking.matching.geometry import (
    cell_intensity,
    core_centre,
    footprint_pairs,
    grow,
    growth_radius_km,
    mahalanobis,
    second_moment,
)

pytestmark = pytest.mark.unit


def _disk(shape, centre, radius):
    rr, cc = np.mgrid[: shape[0], : shape[1]]
    return (rr - centre[0]) ** 2 + (cc - centre[1]) ** 2 <= radius**2


# ── growth radius r = r_max (1 − √A / ℓ0)₊ ──────────────────────────────────


def test_growth_radius_is_r_max_for_a_vanishing_cell():
    assert growth_radius_km(0.0, r_max_km=2.0, l0_km=12.0) == pytest.approx(2.0)


def test_growth_radius_falls_linearly_in_size():
    # √A = 6 km is half of ℓ0 → half of r_max
    assert growth_radius_km(36.0, r_max_km=2.0, l0_km=12.0) == pytest.approx(1.0)


def test_growth_radius_is_zero_at_and_above_l0():
    assert growth_radius_km(144.0, r_max_km=2.0, l0_km=12.0) == 0.0
    assert growth_radius_km(400.0, r_max_km=2.0, l0_km=12.0) == 0.0


# ── growth from the boundary (distance transform) ───────────────────────────


def test_grow_by_zero_is_identity():
    mask = _disk((21, 21), (10, 10), 3)
    assert np.array_equal(grow(mask, 0.0), mask)


def test_grow_is_measured_from_the_boundary_not_the_centre():
    # A 1-px-wide line grown by 2 px is 5 px wide along its whole length,
    # exactly like the rim of a round cell: growth depends on distance to the shape.
    line = np.zeros((11, 31), dtype=bool)
    line[5, 5:26] = True
    grown = grow(line, 2.0)
    assert grown[:, 15].sum() == 5
    assert grown[3:8, 5:26].all()


def test_grow_is_continuous_in_the_radius():
    mask = np.zeros((21, 21), dtype=bool)
    mask[10, 10] = True
    assert grow(mask, 1.0).sum() == 5  # von Neumann neighbours at distance 1
    assert grow(mask, 1.5).sum() == 9  # diagonals at √2 join


# ── pair search on grown shapes ─────────────────────────────────────────────


def test_identical_shapes_have_full_overlap_both_ways():
    labels = np.zeros((30, 30), dtype=np.int32)
    labels[_disk((30, 30), (15, 15), 4)] = 7
    pairs = footprint_pairs(labels == 7, labels, radius_px=1.0)
    (pair,) = pairs
    assert pair.label == 7
    assert pair.o_c == pytest.approx(1.0)
    assert pair.o_h == pytest.approx(1.0)
    assert pair.u == pytest.approx(0.0)


def test_overlap_fractions_are_normalised_by_each_grown_shape():
    # Footprint: a 4x4 block. Cell: a 4x8 block containing it. Radius 0.
    foot = np.zeros((20, 20), dtype=bool)
    foot[5:9, 5:9] = True
    labels = np.zeros((20, 20), dtype=np.int32)
    labels[5:9, 5:13] = 3
    (pair,) = footprint_pairs(foot, labels, radius_px=0.0)
    assert pair.intersection_px == 16
    assert pair.o_c == pytest.approx(16 / 32)  # share of the cell
    assert pair.o_h == pytest.approx(16 / 16)  # share of the footprint
    assert pair.u == pytest.approx(1 - math.sqrt(0.5))


def test_growth_turns_a_near_miss_into_a_candidate():
    # Two single pixels 3 px apart: disjoint when ungrown, overlapping when
    # both are grown by 2 px (the grown disks meet in the middle).
    foot = np.zeros((10, 20), dtype=bool)
    foot[5, 5] = True
    labels = np.zeros((10, 20), dtype=np.int32)
    labels[5, 8] = 1
    assert footprint_pairs(foot, labels, radius_px=0.0) == []
    (pair,) = footprint_pairs(foot, labels, radius_px=2.0)
    assert pair.label == 1 and pair.intersection_px > 0


def test_cell_is_grown_whole_even_when_it_extends_beyond_the_footprint():
    # A long cell touching a small footprint: |M⁺| must be the full grown cell.
    foot = np.zeros((20, 60), dtype=bool)
    foot[10, 5] = True
    labels = np.zeros((20, 60), dtype=np.int32)
    labels[10, 6:56] = 2
    (pair,) = footprint_pairs(foot, labels, radius_px=1.0)
    assert pair.cell_grown_px == grow(labels == 2, 1.0).sum()
    assert pair.footprint_grown_px == 5


def test_empty_footprint_has_no_pairs():
    labels = np.ones((5, 5), dtype=np.int32)
    assert footprint_pairs(np.zeros((5, 5), dtype=bool), labels, radius_px=2.0) == []


# ── intensity excess, mass, core centre ─────────────────────────────────────


def test_excess_is_positive_above_threshold_for_reflectivity():
    field = np.array([[30.0, 40.0, 50.0]])
    mask = np.ones_like(field, dtype=bool)
    mass, mean_excess = cell_intensity(field, mask, threshold=30.0, polarity=1)
    assert mass == pytest.approx(0 + 10 + 20)
    assert mean_excess == pytest.approx(10.0)


def test_excess_is_positive_below_threshold_for_brightness_temperature():
    field = np.array([[235.0, 225.0, 215.0]])
    mask = np.ones_like(field, dtype=bool)
    mass, _ = cell_intensity(field, mask, threshold=235.0, polarity=-1)
    assert mass == pytest.approx(0 + 10 + 20)


def test_core_centre_is_pulled_to_the_stronger_core_by_the_square():
    # Two pixels, excess 1 and 3: e-weighting gives 0.75, e²-weighting 0.9.
    field = np.array([[31.0, 33.0]])
    mask = np.ones_like(field, dtype=bool)
    row, col = core_centre(field, mask, threshold=30.0, polarity=1)
    assert row == pytest.approx(0.0)
    assert col == pytest.approx(9 / 10)


def test_core_centre_polarity_mirrors_for_brightness_temperature():
    field = np.array([[229.0, 227.0]])  # excess 1 and 3 below 230 K
    mask = np.ones_like(field, dtype=bool)
    _, col = core_centre(field, mask, threshold=230.0, polarity=-1)
    assert col == pytest.approx(9 / 10)


def test_core_centre_of_a_cell_without_excess_is_its_geometric_centre():
    field = np.full((1, 4), 20.0)
    mask = np.ones_like(field, dtype=bool)
    assert core_centre(field, mask, threshold=30.0, polarity=1) == pytest.approx((0.0, 1.5))


def test_missing_field_values_carry_no_excess():
    field = np.array([[np.nan, 40.0]])
    mask = np.ones_like(field, dtype=bool)
    mass, _ = cell_intensity(field, mask, threshold=30.0, polarity=1)
    assert mass == pytest.approx(10.0)


# ── second moment and shape-aware residual ──────────────────────────────────


def test_round_cell_residual_is_four_times_displacement_over_diameter():
    # For a disk of radius R, Σ = R²/4 I, so d = |δ| / (R/2) = 4 |δ| / D.
    mask = _disk((201, 201), (100, 100), 40)
    sigma = second_moment(mask)
    d = mahalanobis(np.array([0.0, 10.0]), sigma)
    assert d == pytest.approx(4 * 10 / 80, rel=0.02)


def test_residual_along_a_line_costs_less_than_across_it():
    line = np.zeros((41, 81), dtype=bool)
    line[18:23, 5:76] = True  # 5 px thick, 71 px long, along columns
    sigma = second_moment(line)
    along = mahalanobis(np.array([0.0, 5.0]), sigma)
    across = mahalanobis(np.array([5.0, 0.0]), sigma)
    assert along < across / 5


def test_single_pixel_moment_is_finite():
    mask = np.zeros((3, 3), dtype=bool)
    mask[1, 1] = True
    sigma = second_moment(mask)
    assert np.all(np.isfinite(np.linalg.inv(sigma)))
