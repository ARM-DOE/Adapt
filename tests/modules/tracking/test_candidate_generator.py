# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for CandidateGenerator (buffered high-recall candidates)."""

import numpy as np
import pytest

from adapt.modules.tracking.matching.candidate import CandidateGenerator

pytestmark = pytest.mark.unit


def _mask(shape, cells):
    m = np.zeros(shape, dtype=bool)
    for r0, r1, c0, c1 in cells:
        m[r0:r1, c0:c1] = True
    return m


def test_direct_intersection_is_a_candidate_without_buffer():
    hull = _mask((6, 6), [(1, 3, 1, 3)])
    cell = _mask((6, 6), [(1, 3, 1, 3)])  # overlaps hull
    gen = CandidateGenerator(buffer_pixels=0)
    buffered, pairs = gen.generate([hull], [cell])
    assert pairs == [(0, 0)]
    assert np.array_equal(buffered[0], hull)


def test_buffer_extends_recall_to_a_near_miss():
    hull = _mask((6, 6), [(2, 4, 2, 4)])
    near = _mask((6, 6), [(2, 4, 4, 5)])  # touches column 4, one pixel outside hull
    gen0 = CandidateGenerator(buffer_pixels=0)
    _, pairs0 = gen0.generate([hull], [near])
    # column 3 (hull) and column 4 (near) are adjacent but disjoint → no candidate
    assert pairs0 == []
    gen1 = CandidateGenerator(buffer_pixels=1)
    _, pairs1 = gen1.generate([hull], [near])
    assert pairs1 == [(0, 0)]  # dilation makes them intersect


def test_high_recall_multiple_candidates_per_hull():
    hull = _mask((6, 8), [(1, 5, 1, 6)])  # broad hull
    a = _mask((6, 8), [(1, 3, 1, 3)])
    b = _mask((6, 8), [(3, 5, 4, 6)])
    far = _mask((6, 8), [(0, 1, 7, 8)])  # outside even with 1 px buffer
    gen = CandidateGenerator(buffer_pixels=1)
    _, pairs = gen.generate([hull], [a, b, far])
    assert (0, 0) in pairs and (0, 1) in pairs
    assert (0, 2) not in pairs


def test_empty_hull_yields_no_candidates():
    empty = np.zeros((4, 4), dtype=bool)
    cell = _mask((4, 4), [(0, 2, 0, 2)])
    gen = CandidateGenerator(buffer_pixels=2)
    _, pairs = gen.generate([empty], [cell])
    assert pairs == []
