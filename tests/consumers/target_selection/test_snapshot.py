# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Snapshot data model — frozen, engine-facing, no pandas."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

from adapt.consumers.target_selection.snapshot import (
    CellSnapshot,
    Snapshot,
    TrajectoryPoint,
)

pytestmark = pytest.mark.unit


def _cell(uid: str = "a") -> CellSnapshot:
    return CellSnapshot(
        uid=uid,
        lat=35.0,
        lon=-97.0,
        area_sqkm=100.0,
        reflectivity_max=50.0,
        age_seconds=600.0,
        growth_rate_sqkm_per_min=0.5,
        trajectory=(TrajectoryPoint(lat=35.1, lon=-97.0, lead_seconds=300.0),),
        values={"n_scans": 5.0},
    )


def test_cell_snapshot_frozen():
    cell = _cell()
    with pytest.raises(FrozenInstanceError):
        cell.uid = "b"  # type: ignore[misc]


def test_snapshot_holds_cells():
    cells = (_cell("a"), _cell("b"))
    snap = Snapshot(scan_time=datetime(2024, 6, 1, 12, 0, tzinfo=UTC), cells=cells)
    assert snap.cells == cells
    assert snap.cells[0].trajectory[0].lead_seconds == 300.0
