# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Engine state machine — selection, continuation, switch, stop, determinism.

Fixed scores under the factory config (weights 1.0 / 0.05 / 2.0):
cell_a: refl 50, area 200, growth 1.0 -> 62.0
cell_b: refl 40, area 100, growth 0.0 -> 45.0
"""

from datetime import UTC, datetime

import pytest

from adapt.consumers.target_selection.engine import TargetSelectionEngine
from adapt.consumers.target_selection.selection import SelectionReason
from adapt.consumers.target_selection.snapshot import Snapshot
from tests.consumers.target_selection.factories import make_cell, make_config

pytestmark = pytest.mark.unit

T0 = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
T5 = datetime(2024, 6, 1, 12, 5, tzinfo=UTC)
T30 = datetime(2024, 6, 1, 12, 30, tzinfo=UTC)
T35 = datetime(2024, 6, 1, 12, 35, tzinfo=UTC)


def _cell_a(**kw):
    return make_cell("cell_a", refl=50.0, area=200.0, growth=1.0, **kw)


def _cell_b(refl=40.0, **kw):
    return make_cell("cell_b", refl=refl, area=100.0, growth=0.0, **kw)


def _snap(scan_time, *cells):
    return Snapshot(scan_time=scan_time, cells=cells)


def _engine():
    return TargetSelectionEngine(make_config())


def test_first_selection_new_target():
    engine = _engine()
    sel = engine.select(_snap(T0, _cell_a(), _cell_b()))
    assert sel.cell_uid == "cell_a"
    assert sel.reason is SelectionReason.NEW_TARGET
    assert sel.score == 62.0
    assert sel.selection_time == T0
    assert sel.observation_window == (T0, T30)
    assert sel.predicted_hulls is None
    assert engine.current_target.cell_uid == "cell_a"
    assert engine.current_target.started_at == T0


def test_no_candidates_none_idle():
    engine = _engine()
    sel = engine.select(_snap(T0, _cell_a(age=100.0), _cell_b(age=100.0)))
    assert sel is None
    assert engine.current_target is None


def test_continuation_at_exact_margin():
    engine = _engine()
    engine.select(_snap(T0, _cell_a(), _cell_b()))
    # cell_b at 67.0 == 62.0 + margin(5.0): strict > means no switch.
    sel = engine.select(_snap(T5, _cell_a(), _cell_b(refl=62.0)))
    assert sel.cell_uid == "cell_a"
    assert sel.reason is SelectionReason.CONTINUATION
    assert sel.observation_window == (T5, T30)  # end fixed, window shrinks
    assert engine.current_target.started_at == T0


def test_switch_resets_window():
    engine = _engine()
    engine.select(_snap(T0, _cell_a(), _cell_b()))
    sel = engine.select(_snap(T5, _cell_a(), _cell_b(refl=67.0)))  # 72.0 > 62 + 5
    assert sel.cell_uid == "cell_b"
    assert sel.reason is SelectionReason.SWITCH
    assert sel.observation_window == (T5, T35)
    assert engine.current_target.started_at == T5


def test_target_lost_reselects():
    engine = _engine()
    engine.select(_snap(T0, _cell_a(), _cell_b()))
    sel = engine.select(_snap(T5, _cell_b()))
    assert sel.cell_uid == "cell_b"
    assert sel.reason is SelectionReason.NEW_TARGET
    assert engine.current_target.started_at == T5


def test_quality_drop_stops():
    engine = _engine()
    engine.select(_snap(T0, _cell_a(), _cell_b()))
    sel = engine.select(_snap(T5, _cell_a(values={"n_scans": 1.0}), _cell_b()))
    assert sel.cell_uid == "cell_b"
    assert sel.reason is SelectionReason.NEW_TARGET


def test_max_duration_excludes_stopped_uid():
    engine = _engine()
    engine.select(_snap(T0, _cell_a(), _cell_b()))
    # At T30 observed == 1800 s; cell_a still scores highest but is excluded.
    sel = engine.select(_snap(T30, _cell_a(), _cell_b()))
    assert sel.cell_uid == "cell_b"
    assert sel.reason is SelectionReason.NEW_TARGET
    assert sel.observation_window == (T30, datetime(2024, 6, 1, 13, 0, tzinfo=UTC))


def test_max_duration_no_alternative_returns_none_idle():
    engine = _engine()
    engine.select(_snap(T0, _cell_a()))
    sel = engine.select(_snap(T30, _cell_a()))
    assert sel is None
    assert engine.current_target is None


@pytest.mark.determinism
def test_identical_sequences_identical_selections():
    snapshots = [
        _snap(T0, _cell_a(), _cell_b()),
        _snap(T5, _cell_a(), _cell_b(refl=67.0)),
        _snap(T30, _cell_b(refl=67.0)),
    ]

    def run():
        engine = _engine()
        return [engine.select(s) for s in snapshots]

    assert run() == run()
