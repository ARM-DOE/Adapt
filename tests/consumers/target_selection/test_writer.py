# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""JSONL selection log — append-only, canonical scan-time strings."""

import json
from datetime import UTC, datetime

import pytest

from adapt.consumers.target_selection.selection import SelectionReason, TargetSelection
from adapt.consumers.target_selection.snapshot import TrajectoryPoint
from adapt.consumers.target_selection.writer import append_selection

pytestmark = pytest.mark.unit


def _selection(reason=SelectionReason.SWITCH):
    t = datetime(2024, 6, 1, 14, 0, tzinfo=UTC)
    return TargetSelection(
        cell_uid="uid_beta",
        reason=reason,
        score=72.0,
        selection_time=t,
        trajectory=(TrajectoryPoint(lat=35.1, lon=-97.0, lead_seconds=7200.0),),
        observation_window=(t, datetime(2024, 6, 1, 14, 30, tzinfo=UTC)),
    )


def test_appends_jsonl(tmp_path):
    path = tmp_path / "selections.jsonl"
    append_selection(path, _selection())
    append_selection(path, _selection(reason=SelectionReason.CONTINUATION))
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {
        "cell_uid": "uid_beta",
        "reason": "SWITCH",
        "score": 72.0,
        "selection_time": "2024-06-01T14:00:00Z",
        "trajectory": [{"lat": 35.1, "lon": -97.0, "lead_seconds": 7200.0}],
        "observation_window": ["2024-06-01T14:00:00Z", "2024-06-01T14:30:00Z"],
        "predicted_hulls": None,
    }


def test_lines_are_separated_by_a_bare_newline(tmp_path):
    """The selection log is a data product — its bytes must not vary by platform.

    Text mode on Windows translates "\\n" to "\\r\\n", so the same run would emit a
    different file there.
    """
    path = tmp_path / "selections.jsonl"
    append_selection(path, _selection())
    append_selection(path, _selection())
    assert b"\r\n" not in path.read_bytes()


def test_missing_parent_dir_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        append_selection(tmp_path / "no_such_dir" / "selections.jsonl", _selection())
