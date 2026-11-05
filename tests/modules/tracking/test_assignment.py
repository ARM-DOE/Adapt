# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Joint assignment per group of competing cells: no-link, crossing, margin."""

import pytest

from adapt.modules.tracking.matching.assignment import Link, components, solve_component

pytestmark = pytest.mark.unit

C_MAX = 1.8


def _link(i, j, cost, start=(0.0, 0.0), end=(0.0, 0.0)):
    return Link(i, j, cost, start, end)


def test_components_group_cells_connected_through_candidates():
    groups = components([(0, 0), (1, 0), (2, 1), (3, 2), (3, 3)])
    assert sorted(groups) == [([0, 1], [0]), ([2], [1]), ([3], [2, 3])]


def test_single_candidate_is_accepted_below_the_ceiling():
    solution = solve_component([_link(0, 0, 1.2)], C_MAX)
    assert solution.chosen == [(0, 0)]


def test_disjoint_pairs_resolve_independently():
    links = [_link(0, 0, 0.3), _link(1, 1, 0.4)]
    chosen = [
        p
        for prevs, currs in components([(0, 0), (1, 1)])
        for p in solve_component([lk for lk in links if lk.prev in prevs], C_MAX).chosen
    ]
    assert sorted(chosen) == [(0, 0), (1, 1)]


def test_cheaper_predecessor_wins_a_contested_cell():
    solution = solve_component([_link(0, 0, 0.6), _link(1, 0, 0.2)], C_MAX)
    assert solution.chosen == [(1, 0)]


def test_an_assignment_whose_links_cross_is_not_admissible():
    # Two cells side by side exchanging places is the cheapest pairing but it
    # crosses; the parallel pairing is chosen instead.
    a0, b0 = (0.0, 0.0), (0.0, 10.0)
    x, y = (10.0, 10.0), (10.0, 0.0)
    links = [
        _link(0, 0, 0.1, a0, x),
        _link(1, 1, 0.1, b0, y),
        _link(0, 1, 0.5, a0, y),
        _link(1, 0, 0.5, b0, x),
    ]
    solution = solve_component(links, C_MAX)
    assert sorted(solution.chosen) == [(0, 1), (1, 0)]
    assert set(solution.crossing_excluded) & {(0, 0), (1, 1)}


def test_when_every_alternative_crosses_the_costlier_link_is_dropped():
    a0, b0 = (0.0, 0.0), (0.0, 10.0)
    links = [_link(0, 0, 0.1, a0, (10.0, 10.0)), _link(1, 1, 0.3, b0, (10.0, 0.0))]
    solution = solve_component(links, C_MAX)
    assert solution.chosen == [(0, 0)]
    assert solution.crossing_excluded == {(1, 1): (0, 0)}


def test_margin_is_the_extra_cost_of_the_best_assignment_without_the_link():
    solution = solve_component([_link(0, 0, 0.4)], C_MAX)
    assert solution.margins[(0, 0)] == pytest.approx(C_MAX - 0.4)


def test_margin_to_a_runner_up():
    solution = solve_component([_link(0, 0, 0.4), _link(1, 0, 0.5)], C_MAX)
    # With (0,0): 0.4 + 1.8 (prev 1 unlinked). Without it: 1.8 + 0.5.
    assert solution.chosen == [(0, 0)]
    assert solution.margins[(0, 0)] == pytest.approx(0.1)
