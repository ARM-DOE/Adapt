# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Stop rule — target lost, quality dropped, max duration."""

import pytest

from adapt.consumers.target_selection.rules import StopReason, should_stop
from tests.consumers.target_selection.factories import make_cell, make_config

pytestmark = pytest.mark.unit

CFG = make_config(max_observation_seconds=1800.0)


def test_target_lost():
    assert should_stop(None, CFG, 0.0) is StopReason.TARGET_LOST


def test_quality_dropped():
    cell = make_cell(values={"n_scans": 1.0})
    assert should_stop(cell, CFG, 0.0) is StopReason.QUALITY_DROPPED


def test_max_duration_boundary():
    # Inclusive boundary: observed == max fires the stop.
    assert should_stop(make_cell(), CFG, 1800.0) is StopReason.MAX_DURATION


def test_healthy_none():
    assert should_stop(make_cell(), CFG, 1799.0) is None
