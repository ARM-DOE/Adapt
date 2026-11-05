# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Per-component Hungarian assignment with a no-link option at the cost ceiling."""

import pytest

from adapt.modules.tracking.matching.hungarian import HungarianMatcher

pytestmark = pytest.mark.unit

C_MAX = 1.8


def test_two_by_two_picks_min_cost_assignment():
    costs = {(0, 0): 0.9, (0, 1): 0.1, (1, 0): 0.2, (1, 1): 0.8}
    assert sorted(HungarianMatcher.match([0, 1], [0, 1], costs, C_MAX)) == [(0, 1), (1, 0)]


def test_unequal_component_leaves_surplus_unmatched():
    costs = {(0, 0): 0.7, (1, 0): 0.2}
    assert HungarianMatcher.match([0, 1], [0], costs, C_MAX) == [(1, 0)]


def test_non_edge_is_never_assigned():
    costs = {(0, 0): 0.3}
    assert HungarianMatcher.match([0, 1], [0, 1], costs, C_MAX) == [(0, 0)]


def test_a_link_at_the_ceiling_is_accepted():
    assert HungarianMatcher.match([0], [0], {(0, 0): C_MAX}, C_MAX) == [(0, 0)]


def test_no_link_is_chosen_when_every_rearrangement_costs_more():
    # Without the no-link column prev 0 would be forced onto curr 1 to give
    # prev 1 its only candidate (1.75 + 0.2 = 1.95); leaving prev 1 unlinked
    # costs 0.1 + 1.8 = 1.9.
    costs = {(0, 0): 0.1, (1, 0): 0.2, (0, 1): 1.75}
    assert HungarianMatcher.match([0, 1], [0, 1], costs, C_MAX) == [(0, 0)]


def test_empty_component_returns_nothing():
    assert HungarianMatcher.match([], [0], {}, C_MAX) == []
    assert HungarianMatcher.match([0], [], {}, C_MAX) == []
