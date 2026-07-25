# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Selection rule — highest priority wins, deterministic tie-break."""

import pytest

from adapt.consumers.target_selection.rules import select_best

pytestmark = pytest.mark.unit


def test_highest_wins():
    assert select_best({"a": 62.0, "b": 45.0}) == "a"


def test_tie_breaks_to_smallest_uid():
    assert select_best({"b": 62.0, "a": 62.0}) == "a"


def test_empty_returns_none():
    assert select_best({}) is None
