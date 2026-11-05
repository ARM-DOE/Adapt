# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""A merge target records which cell it absorbed and how old that cell was.

A merge ends the absorbed cell's track while the survivor keeps its own uid and
age, so without these columns the longer history is unrecoverable from
``cells_by_scan``. Identity is deliberately not transferred — the survivor's
own ``cell_uid`` and ``age_seconds`` are untouched.
"""

import pandas as pd
import pytest

from adapt.persistence.track_store import TrackStore

pytestmark = pytest.mark.unit

SCAN_ISO = "2025-06-17T16:00:00Z"


def _rows(events: list[dict], first_seen: dict[str, str]) -> dict[int, dict]:
    tracked = pd.DataFrame([{"cell_label": 1, "cell_uid": "SURVIVOR"}])
    built = TrackStore._build_cells_rows(
        TrackStore.__new__(TrackStore),
        run_id="R",
        scan_id="S",
        scan_iso=SCAN_ISO,
        cell_stats_df=pd.DataFrame([{"cell_label": 1}]),
        tracked_cells_df=tracked,
        cell_events_df=pd.DataFrame(events),
        adjacency={},
        first_seen_map=first_seen,
    )
    return {r["cell_label"]: r for r in built}


def _merge(source_uid: str) -> dict:
    return {
        "event_type": "MERGE",
        "source_cell_uid": source_uid,
        "target_cell_uid": "SURVIVOR",
        "target_cell_label": 1,
    }


def test_merge_records_the_absorbed_cell_and_its_lifetime():
    row = _rows(
        [_merge("ABSORBED")],
        {"SURVIVOR": "2025-06-17T15:55:00Z", "ABSORBED": "2025-06-17T15:00:00Z"},
    )[1]
    assert row["merged_from_cell_uid"] == "ABSORBED"
    assert row["merged_from_age_seconds"] == pytest.approx(3600.0)
    # identity is NOT transferred: the survivor keeps its own, shorter age
    assert row["cell_uid"] == "SURVIVOR"
    assert row["age_seconds"] == pytest.approx(300.0)


def test_the_oldest_source_wins_when_several_merge_at_once():
    row = _rows(
        [_merge("YOUNG"), _merge("OLD")],
        {
            "SURVIVOR": "2025-06-17T15:55:00Z",
            "YOUNG": "2025-06-17T15:50:00Z",
            "OLD": "2025-06-17T14:00:00Z",
        },
    )[1]
    assert row["merged_from_cell_uid"] == "OLD"
    assert row["merged_from_age_seconds"] == pytest.approx(7200.0)


def test_a_cell_with_no_merge_carries_nulls():
    row = _rows([], {"SURVIVOR": "2025-06-17T15:55:00Z"})[1]
    assert row["merged_from_cell_uid"] is None
    assert row["merged_from_age_seconds"] is None
