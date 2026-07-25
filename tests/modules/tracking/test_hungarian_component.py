# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for per-component HungarianMatcher."""

import pytest

from adapt.modules.tracking.matching.hungarian import HungarianMatcher

pytestmark = pytest.mark.unit


def test_two_by_two_picks_min_cost_assignment():
    # Component prev {0,1} ↔ curr {0,1}. Cheapest pairing is the anti-diagonal.
    costs = {(0, 0): 0.9, (0, 1): 0.1, (1, 0): 0.2, (1, 1): 0.8}
    matched = HungarianMatcher.match([0, 1], [0, 1], costs)
    assert sorted(matched) == [(0, 1), (1, 0)]


def test_unequal_component_leaves_surplus_unmatched():
    # Two previous, one current; only the cheaper prev keeps the cell.
    costs = {(0, 0): 0.7, (1, 0): 0.2}
    matched = HungarianMatcher.match([0, 1], [0], costs)
    assert matched == [(1, 0)]


def test_non_edge_assignment_is_dropped():
    # prev1 has no validated edge to curr1; padding must not create a fake match.
    costs = {(0, 0): 0.3}
    matched = HungarianMatcher.match([0, 1], [0, 1], costs)
    assert matched == [(0, 0)]


def test_empty_component_returns_nothing():
    assert HungarianMatcher.match([], [0], {}) == []
    assert HungarianMatcher.match([0], [], {}) == []
