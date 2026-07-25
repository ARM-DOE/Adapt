# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Continuation rule — keep current target unless beaten by switch_margin."""

import pytest

from adapt.consumers.target_selection.rules import continue_or_switch
from adapt.consumers.target_selection.selection import SelectionReason

pytestmark = pytest.mark.unit


def test_keeps_within_margin():
    # Strict inequality: 67.0 == 62.0 + 5.0 is NOT enough to switch.
    scored = {"a": 62.0, "b": 67.0}
    assert continue_or_switch("a", scored, 5.0) == ("a", SelectionReason.CONTINUATION)


def test_switches_beyond_margin():
    scored = {"a": 62.0, "b": 67.01}
    assert continue_or_switch("a", scored, 5.0) == ("b", SelectionReason.SWITCH)


def test_no_challenger_continues():
    assert continue_or_switch("a", {"a": 62.0}, 5.0) == ("a", SelectionReason.CONTINUATION)
