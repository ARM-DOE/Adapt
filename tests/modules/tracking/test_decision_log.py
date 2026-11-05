# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The tracker records every pair it considered, every lineage hypothesis, and why.

Synthetic 8x8 scans (1000 m pixels) with analytically known outcomes.
"""

import numpy as np
import pytest

from adapt.modules.tracking.module import CellTracker
from tests.modules.tracking.test_tracker_quality import (
    _cell_stats,
    _make_config,
    _one_cell_scan,
    _synthetic_ds,
)

pytestmark = pytest.mark.unit

T0 = np.datetime64("2024-01-01T12:00:00")
T1 = np.datetime64("2024-01-01T12:05:00")
T2 = np.datetime64("2024-01-01T12:10:00")


def _cells_scan(time, spans: dict[int, tuple[int, int]], proj_labels=None):
    """Cells as {label: (col_start, col_stop)} on rows 2-3 of an 8x8 grid."""
    labels = np.zeros((8, 8), dtype=np.int32)
    rows = []
    for lab, (start, stop) in spans.items():
        labels[2:4, start:stop] = lab
        rows.append(
            {
                "id": lab,
                "area": float(2 * (stop - start)),
                "cx": (start + stop - 1) / 2 * 1000.0,
                "cy": 2500.0,
                "mean_refl": 40.0,
                "max_refl": 45.0,
            }
        )
    return _synthetic_ds(time, labels, proj_labels=proj_labels), _cell_stats(time, rows)


def test_first_scan_has_no_pairs_and_every_cell_initiates():
    tracker = CellTracker(_make_config())
    tracker.track(*_one_cell_scan(T0, 2), scan_id="scan0")
    d = tracker.decisions()
    assert d.scan.reset_code == "FIRST_SCAN"
    assert (d.scan.n_prev, d.scan.n_curr, d.scan.n_pairs) == (0, 1, 0)
    assert [c.fate for c in d.cells] == ["INITIATION"]


def test_perfect_continuation_is_one_assigned_pair_with_its_evidence():
    tracker = CellTracker(_make_config())
    tracker.track(*_one_cell_scan(T0, 2), scan_id="scan0")
    tracker.track(*_one_cell_scan(T1, 3), scan_id="scan1")
    (pair,) = tracker.decisions().pairs
    assert (pair.outcome, pair.gate, pair.source, pair.steps) == ("CONTINUE", "PASS", "LIVE", 1)
    assert pair.u == pytest.approx(0.0)
    assert pair.cost == pytest.approx(0.0)
    assert pair.speed_ms == pytest.approx(1000.0 / 300.0)
    assert pair.cost_rank == 1
    assert pair.margin == pytest.approx(_make_config().max_link_cost)


def test_overlap_rejection_is_recorded_with_its_numbers(make_tracking_config):
    tracker = CellTracker(make_tracking_config(tracker={"max_overlap_mismatch": 0.1}))
    tracker.track(*_cells_scan(T0, {1: (0, 4)}), scan_id="scan0")
    proj = np.zeros((8, 8), dtype=np.int32)
    proj[2:4, 0:4] = 1
    tracker.track(*_cells_scan(T1, {1: (2, 6)}, proj_labels=proj), scan_id="scan1")
    (pair,) = tracker.decisions().pairs
    assert (pair.gate, pair.outcome) == ("OVERLAP", "REJECTED")
    assert pair.u > 0.1
    assert 0.0 < pair.o_c < 1.0 and 0.0 < pair.o_h < 1.0


def test_speed_rejection_records_speed_and_code():
    tracker = CellTracker(_make_config(max_speed_ms=2.0))
    tracker.track(*_one_cell_scan(T0, 2), scan_id="scan0")
    tracker.track(*_one_cell_scan(T1, 3), scan_id="scan1")
    (pair,) = tracker.decisions().pairs
    assert pair.gate == "SPEED"
    assert pair.speed_ms == pytest.approx(1000.0 / 300.0)


def test_contested_cell_records_ranks_margin_and_the_loser():
    # Two previous cells whose footprints both reach one current cell.
    tracker = CellTracker(_make_config())
    tracker.track(*_cells_scan(T0, {1: (0, 3), 2: (5, 8)}), scan_id="scan0")
    proj = np.zeros((8, 8), dtype=np.int32)
    proj[2:4, 1:4] = 1
    proj[2:4, 4:7] = 2
    tracker.track(*_cells_scan(T1, {1: (2, 5)}, proj_labels=proj), scan_id="scan1")
    d = tracker.decisions()
    outcomes = sorted((p.prev_cell_label, p.outcome) for p in d.pairs)
    assert [o for _, o in outcomes].count("CONTINUE") == 1
    assert d.scan.n_components == 1
    winner = next(p for p in d.pairs if p.outcome == "CONTINUE")
    assert winner.margin > 0.0


def test_orphan_lineage_hypotheses_are_recorded_pass_or_fail():
    tracker = CellTracker(_make_config())
    tracker.track(*_cells_scan(T0, {1: (1, 7)}), scan_id="scan0")
    proj = np.zeros((8, 8), dtype=np.int32)
    proj[2:4, 1:7] = 1
    tracker.track(*_cells_scan(T1, {1: (1, 4), 2: (5, 7)}, proj_labels=proj), scan_id="scan1")
    d = tracker.decisions()
    orphan = next(c for c in d.cells if c.fate != "CONTINUE")
    rows = [r for r in d.lineage if r.cell_label == orphan.cell_label and r.side == "curr"]
    kinds = {r.hypothesis: r for r in rows}
    assert kinds["SPLIT"].chosen and kinds["SPLIT"].passed
    assert kinds["SPLIT"].fraction == pytest.approx(1.0)
    assert not kinds["INITIATION"].chosen


def test_unlinked_track_records_merge_and_latent_hypotheses():
    tracker = CellTracker(_make_config())
    tracker.track(*_cells_scan(T0, {1: (1, 4), 2: (5, 7)}), scan_id="scan0")
    proj = np.zeros((8, 8), dtype=np.int32)
    proj[2:4, 1:4] = 1
    proj[2:4, 5:7] = 2
    tracker.track(*_cells_scan(T1, {1: (1, 7)}, proj_labels=proj), scan_id="scan1")
    d = tracker.decisions()
    prev_rows = {r.hypothesis: r for r in d.lineage if r.side == "prev"}
    assert prev_rows["MERGE"].chosen and prev_rows["MERGE"].fraction == pytest.approx(1.0)
    assert not prev_rows["LATENT"].chosen
    (latent,) = d.latent
    assert (latent.status, latent.origin, latent.age) == ("CREATED", "MERGED", 1)
    assert d.scan.n_merge == 1


def test_latent_resumption_is_recorded():
    tracker = CellTracker(_make_config())
    tracker.track(*_one_cell_scan(T0, 2), scan_id="scan0")
    empty_ds, empty_stats = _cells_scan(
        T1, {}, proj_labels=_one_cell_scan(T0, 2)[0]["cell_labels"].values
    )
    tracker.track(empty_ds, empty_stats, scan_id="scan1")
    tracker.track(
        *_cells_scan(T2, {1: (2, 4)}, proj_labels=np.zeros((8, 8), np.int32)), scan_id="scan2"
    )
    d = tracker.decisions()
    (pair,) = d.pairs
    assert (pair.source, pair.steps, pair.outcome) == ("LATENT", 2, "RESUMED")
    assert [c.fate for c in d.cells] == ["RESUMED"]
    assert [lt.status for lt in d.latent] == ["RESUMED"]
    assert any(r.hypothesis == "RESUME" and r.chosen for r in d.lineage)


def test_gap_reset_is_recorded():
    tracker = CellTracker(_make_config(max_tracking_gap_minutes=3.0))
    tracker.track(*_one_cell_scan(T0, 2), scan_id="scan0")
    tracker.track(*_one_cell_scan(T1, 2), scan_id="scan1")
    d = tracker.decisions()
    assert d.scan.reset_code == "TRACK_GAP_EXCEEDED"
    assert d.scan.n_termination == 1
    assert [c.fate for c in d.cells] == ["INITIATION"]
