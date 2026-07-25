# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for ConstraintPropagator and AssignmentGraph."""

import pytest

from adapt.modules.tracking.matching.assignment import AssignmentGraph, ConstraintPropagator

pytestmark = pytest.mark.unit


def test_unique_pair_is_forced():
    forced, remaining = ConstraintPropagator.resolve([(0, 0)])
    assert forced == [(0, 0)]
    assert remaining == []


def test_disjoint_unique_pairs_all_forced():
    forced, remaining = ConstraintPropagator.resolve([(0, 0), (1, 1), (2, 2)])
    assert forced == [(0, 0), (1, 1), (2, 2)]
    assert remaining == []


def test_full_two_by_two_is_ambiguous():
    edges = [(0, 0), (0, 1), (1, 0), (1, 1)]
    forced, remaining = ConstraintPropagator.resolve(edges)
    assert forced == []
    assert remaining == sorted(edges)


def test_partial_resolves_unique_leaves_ambiguous_remainder():
    # prev0↔curr0 unique; prev1 has two candidates → ambiguous
    forced, remaining = ConstraintPropagator.resolve([(0, 0), (1, 1), (1, 2)])
    assert forced == [(0, 0)]
    assert remaining == [(1, 1), (1, 2)]


def test_cascade_peeling_converges():
    # Removing the unique (0,0) does NOT free curr1 (still deg 2) → stays ambiguous.
    forced, remaining = ConstraintPropagator.resolve([(0, 0), (1, 1), (1, 2), (2, 2)])
    assert forced == [(0, 0)]
    assert remaining == [(1, 1), (1, 2), (2, 2)]


def test_contested_candidate_not_forced():
    # prev0 has a single candidate curr0, but curr0 is also claimed by prev1 → not unique
    forced, remaining = ConstraintPropagator.resolve([(0, 0), (1, 0), (1, 1)])
    assert forced == []
    assert remaining == sorted([(0, 0), (1, 0), (1, 1)])


def test_components_single_group():
    comp = AssignmentGraph([(0, 0), (0, 1), (1, 0), (1, 1)]).components()
    assert len(comp) == 1
    prevs, currs = comp[0]
    assert prevs == [0, 1] and currs == [0, 1]


def test_components_disconnected_ambiguous_groups_independent():
    # Two separate 2x2 ambiguous groups with no cross edges → two components.
    edges = [(0, 0), (0, 1), (1, 0), (1, 1), (2, 2), (2, 3), (3, 2), (3, 3)]
    forced, remaining = ConstraintPropagator.resolve(edges)
    assert forced == []
    comps = AssignmentGraph(remaining).components()
    comps_sorted = sorted(comps)
    assert comps_sorted == [([0, 1], [0, 1]), ([2, 3], [2, 3])]
