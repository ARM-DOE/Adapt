"""Unit tests for TrackStore.

Each test uses an in-memory SQLite database with the three-table schema.
Inputs are synthetic DataFrames; no file I/O.
"""
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from adapt.persistence.track_store import TrackStore

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Schema DDL (minimal inline copy so tests have no dependency on the SQL file)
# ---------------------------------------------------------------------------

_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS cells_by_scan (
    run_id                  TEXT NOT NULL,
    scan_time               TEXT NOT NULL,
    cell_label              INTEGER NOT NULL,
    cell_uid                TEXT NOT NULL,
    cell_area_sqkm          REAL,
    cell_centroid_mass_lat  REAL,
    cell_centroid_mass_lon  REAL,
    cell_centroid_geom_x    REAL,
    cell_centroid_geom_y    REAL,
    radar_reflectivity_max  REAL,
    radar_reflectivity_mean REAL,
    radar_differential_reflectivity_max REAL,
    area_40dbz_km2          REAL,
    age_seconds             REAL NOT NULL DEFAULT 0,
    n_adjacent_cells        INTEGER NOT NULL DEFAULT 0,
    adjacent_cell_uids_json TEXT,
    is_initiated_here       INTEGER NOT NULL DEFAULT 0,
    is_split_target_here    INTEGER NOT NULL DEFAULT 0,
    is_merge_target_here    INTEGER NOT NULL DEFAULT 0,
    is_split_source_here    INTEGER NOT NULL DEFAULT 0,
    is_merge_source_here    INTEGER NOT NULL DEFAULT 0,
    is_terminated_after_here INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, scan_time, cell_uid),
    UNIQUE (run_id, scan_time, cell_label)
);

CREATE TABLE IF NOT EXISTS cell_events (
    event_id          INTEGER PRIMARY KEY,
    run_id            TEXT NOT NULL,
    source_scan_time  TEXT,
    target_scan_time  TEXT,
    event_type        TEXT NOT NULL,
    source_cell_uid    TEXT,
    target_cell_uid    TEXT,
    source_cell_label INTEGER,
    target_cell_label INTEGER,
    cost              REAL,
    is_dominant       INTEGER NOT NULL DEFAULT 0,
    event_group_id    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cell_tracks (
    run_id                        TEXT NOT NULL,
    cell_uid                      TEXT NOT NULL,
    first_seen_time               TEXT NOT NULL,
    last_seen_time                TEXT NOT NULL,
    n_scans                       INTEGER NOT NULL DEFAULT 0,
    origin_type                   TEXT NOT NULL,
    origin_event_group_id         TEXT,
    origin_n_parents              INTEGER NOT NULL DEFAULT 0,
    origin_primary_parent_cell_uid TEXT,
    termination_type              TEXT NOT NULL DEFAULT 'ACTIVE_AT_END',
    termination_event_group_id    TEXT,
    terminated_into_cell_uid      TEXT,
    max_area_sqkm                 REAL,
    max_reflectivity              REAL,
    PRIMARY KEY (run_id, cell_uid)
);
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "catalog.db"
    conn = sqlite3.connect(str(p))
    conn.executescript(_DDL)
    conn.close()
    return p


@pytest.fixture
def store(db_path):
    return TrackStore(db_path)


def _t(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def _cell_stats(cell_label: int, area: float = 4.0, refl: float = 40.0) -> pd.DataFrame:
    return pd.DataFrame([{
        "cell_label": cell_label,
        "cell_area_sqkm": area,
        "cell_centroid_mass_lat": 35.0,
        "cell_centroid_mass_lon": -97.0,
        "cell_centroid_geom_x": 10.0,
        "cell_centroid_geom_y": 10.0,
        "radar_reflectivity_max": refl,
        "radar_reflectivity_mean": refl - 5.0,
        "radar_differential_reflectivity_max": 1.0,
        "area_40dbz_km2": area * 0.5,
    }])

def _tracked_cells(cell_label: int, cell_uid: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "cell_label": cell_label,
        "cell_uid": cell_uid,
        "area": 4.0,
        "max_reflectivity": 40.0,
    }])

def _empty_cell_adjacency() -> pd.DataFrame:
    return pd.DataFrame(columns=["time", "cell_label_a", "cell_label_b", "touching_boundary_pixels"])

def _initiation_event(scan_time: datetime, cell_uid: str, cell_label: int) -> pd.DataFrame:
    ts = scan_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pd.DataFrame([{
        "time": scan_time,
        "event_type": "INITIATION",
        "source_cell_uid": None,
        "target_cell_uid": cell_uid,
        "source_cell_label": None,
        "target_cell_label": cell_label,
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{ts}:INITIATION:{cell_uid}",
    }])


def _continue_event(scan_time: datetime, cell_uid: str, src_label: int, tgt_label: int) -> pd.DataFrame:
    ts = scan_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pd.DataFrame([{
        "time": scan_time,
        "event_type": "CONTINUE",
        "source_cell_uid": cell_uid,
        "target_cell_uid": cell_uid,
        "source_cell_label": src_label,
        "target_cell_label": tgt_label,
        "cost": 0.1,
        "is_dominant": True,
        "event_group_id": f"{ts}:CONTINUE:{cell_uid}",
    }])


def _termination_event(scan_time: datetime, cell_uid: str, cell_label: int) -> pd.DataFrame:
    ts = scan_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pd.DataFrame([{
        "time": scan_time,
        "event_type": "TERMINATION",
        "source_cell_uid": cell_uid,
        "target_cell_uid": None,
        "source_cell_label": cell_label,
        "target_cell_label": None,
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{ts}:TERMINATION:{cell_uid}",
    }])


def _split_events(scan_time: datetime, parent_id: str, child_id: str, parent_label: int, child_label: int) -> pd.DataFrame:
    ts = scan_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pd.DataFrame([{
        "time": scan_time,
        "event_type": "SPLIT",
        "source_cell_uid": parent_id,
        "target_cell_uid": child_id,
        "source_cell_label": parent_label,
        "target_cell_label": child_label,
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{ts}:SPLIT:{parent_id}",
    }])


def _merge_events(scan_time: datetime, src_id: str, tgt_id: str, src_label: int, tgt_label: int) -> pd.DataFrame:
    ts = scan_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    return pd.DataFrame([{
        "time": scan_time,
        "event_type": "MERGE",
        "source_cell_uid": src_id,
        "target_cell_uid": tgt_id,
        "source_cell_label": src_label,
        "target_cell_label": tgt_label,
        "cost": None,
        "is_dominant": False,
        "event_group_id": f"{ts}:MERGE:{tgt_id}",
    }])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_initiation_inserts_cells_by_scan_and_tracks(store):
    t = _t("2024-01-01T12:00:00")
    store.write_scan(
        run_id="r1",
        scan_time=t,
        cell_stats_df=_cell_stats(1),
        tracked_cells_df=_tracked_cells(1, "AAAA"),
        cell_events_df=_initiation_event(t, "AAAA", 1),
        cell_adjacency_df=_empty_cell_adjacency(),
    )
    cells = store.get_cells_by_scan("r1", t)
    assert len(cells) == 1
    assert cells.iloc[0]["cell_uid"] == "AAAA"
    assert cells.iloc[0]["is_initiated_here"] == 1

    tracks = store.get_cell_tracks("r1")
    assert len(tracks) == 1
    assert tracks.iloc[0]["cell_uid"] == "AAAA"
    assert tracks.iloc[0]["origin_type"] == "INITIATION"
    assert tracks.iloc[0]["n_scans"] == 1


def test_continuation_updates_last_seen_and_n_scans(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    store.write_scan("r1", t1, _cell_stats(1), _tracked_cells(1, "BBBB"),
                     _initiation_event(t1, "BBBB", 1), _empty_cell_adjacency())
    store.write_scan("r1", t2, _cell_stats(1), _tracked_cells(1, "BBBB"),
                     _continue_event(t2, "BBBB", 1, 1), _empty_cell_adjacency())

    tracks = store.get_cell_tracks("r1")
    assert tracks.iloc[0]["n_scans"] == 2
    assert tracks.iloc[0]["last_seen_time"] == "2024-01-01T12:05:00Z"


def test_split_sets_split_source_retroactively_on_prev_scan(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    store.write_scan("r1", t1, _cell_stats(1), _tracked_cells(1, "CCCC"),
                     _initiation_event(t1, "CCCC", 1), _empty_cell_adjacency())

    split_evts = pd.concat([
        _continue_event(t2, "CCCC", 1, 1),
        _split_events(t2, "CCCC", "DDDD", 1, 2),
    ], ignore_index=True)
    cells2 = pd.concat([_tracked_cells(1, "CCCC"), _tracked_cells(2, "DDDD")], ignore_index=True)
    store.write_scan("r1", t2, pd.concat([_cell_stats(1), _cell_stats(2)], ignore_index=True),
                     cells2, split_evts, _empty_cell_adjacency())

    prev = store.get_cells_by_scan("r1", t1)
    assert prev.iloc[0]["is_split_source_here"] == 1


def test_merge_into_existing_track_marks_sources_terminated(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    init2 = pd.concat([
        _initiation_event(t1, "EE", 1),
        _initiation_event(t1, "FF", 2),
    ], ignore_index=True)
    cells1 = pd.concat([_tracked_cells(1, "EE"), _tracked_cells(2, "FF")], ignore_index=True)
    store.write_scan("r1", t1, pd.concat([_cell_stats(1), _cell_stats(2)], ignore_index=True),
                     cells1, init2, _empty_cell_adjacency())

    merge_evts = pd.concat([
        _continue_event(t2, "EE", 1, 1),
        _merge_events(t2, "FF", "EE", 2, 1),
        _termination_event(t2, "FF", 2),
    ], ignore_index=True)
    store.write_scan("r1", t2, _cell_stats(1), _tracked_cells(1, "EE"),
                     merge_evts, _empty_cell_adjacency())

    tracks = store.get_cell_tracks("r1")
    ff = tracks[tracks["cell_uid"] == "FF"].iloc[0]
    assert ff["termination_type"] == "MERGED"
    assert ff["terminated_into_cell_uid"] == "EE"


def test_merge_into_new_cell_uid_sets_origin_type_merge(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    cells1 = pd.concat([_tracked_cells(1, "GG"), _tracked_cells(2, "HH")], ignore_index=True)
    init2 = pd.concat([
        _initiation_event(t1, "GG", 1),
        _initiation_event(t1, "HH", 2),
    ], ignore_index=True)
    store.write_scan("r1", t1, pd.concat([_cell_stats(1), _cell_stats(2)], ignore_index=True),
                     cells1, init2, _empty_cell_adjacency())

    merge_evts = pd.concat([
        _merge_events(t2, "GG", "II", 1, 1),
        _merge_events(t2, "HH", "II", 2, 1),
        _termination_event(t2, "GG", 1),
        _termination_event(t2, "HH", 2),
    ], ignore_index=True)
    store.write_scan("r1", t2, _cell_stats(1), _tracked_cells(1, "II"),
                     merge_evts, _empty_cell_adjacency())

    tracks = store.get_cell_tracks("r1")
    ii = tracks[tracks["cell_uid"] == "II"].iloc[0]
    assert ii["origin_type"] in ("MERGE", "UNKNOWN")


def test_termination_sets_is_terminated_after_here_retroactively(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    store.write_scan("r1", t1, _cell_stats(1), _tracked_cells(1, "JJ"),
                     _initiation_event(t1, "JJ", 1), _empty_cell_adjacency())
    store.write_scan("r1", t2, _cell_stats(2), _tracked_cells(2, "KK"),
                     pd.concat([
                         _initiation_event(t2, "KK", 2),
                         _termination_event(t2, "JJ", 1),
                     ], ignore_index=True),
                     _empty_cell_adjacency())

    prev = store.get_cells_by_scan("r1", t1)
    assert prev.iloc[0]["is_terminated_after_here"] == 1


def test_get_track_history_returns_ordered_rows(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    t3 = _t("2024-01-01T12:10:00")
    store.write_scan("r1", t1, _cell_stats(1), _tracked_cells(1, "LL"),
                     _initiation_event(t1, "LL", 1), _empty_cell_adjacency())
    store.write_scan("r1", t2, _cell_stats(1), _tracked_cells(1, "LL"),
                     _continue_event(t2, "LL", 1, 1), _empty_cell_adjacency())
    store.write_scan("r1", t3, _cell_stats(1), _tracked_cells(1, "LL"),
                     _continue_event(t3, "LL", 1, 1), _empty_cell_adjacency())

    history = store.get_track_history("r1", "LL")
    assert len(history) == 3
    times = list(history["scan_time"])
    assert times == sorted(times)


def test_upsert_does_not_churn_rows_on_repeat_write(store):
    t = _t("2024-01-01T12:00:00")
    store.write_scan("r1", t, _cell_stats(1), _tracked_cells(1, "MM"),
                     _initiation_event(t, "MM", 1), _empty_cell_adjacency())
    store.write_scan("r1", t, _cell_stats(1), _tracked_cells(1, "MM"),
                     _initiation_event(t, "MM", 1), _empty_cell_adjacency())

    cells = store.get_cells_by_scan("r1", t)
    assert len(cells) == 1


def test_unique_constraint_rejects_duplicate_cell_label_per_scan(store):
    t = _t("2024-01-01T12:00:00")
    store.write_scan("r1", t, _cell_stats(1), _tracked_cells(1, "NN"),
                     _initiation_event(t, "NN", 1), _empty_cell_adjacency())

    # Writing same cell_label with different cell_uid should raise on unique(cell_label)
    with pytest.raises(Exception):
        store.write_scan("r1", t, _cell_stats(1), _tracked_cells(1, "OO"),
                         _initiation_event(t, "OO", 1), _empty_cell_adjacency())


def test_track_events_has_both_source_and_target_scan_times(store):
    t1 = _t("2024-01-01T12:00:00")
    t2 = _t("2024-01-01T12:05:00")
    store.write_scan("r1", t1, _cell_stats(1), _tracked_cells(1, "PP"),
                     _initiation_event(t1, "PP", 1), _empty_cell_adjacency())
    store.write_scan("r1", t2, _cell_stats(1), _tracked_cells(1, "PP"),
                     _continue_event(t2, "PP", 1, 1), _empty_cell_adjacency())

    events = store.get_cell_events("r1", "PP")
    init = events[events["event_type"] == "INITIATION"].iloc[0]
    cont = events[events["event_type"] == "CONTINUE"].iloc[0]

    assert pd.isna(init["source_scan_time"])
    assert init["target_scan_time"] is not None

    assert cont["source_scan_time"] is not None
    assert cont["target_scan_time"] is not None


def test_cell_uid_fields_are_persisted_and_returned(store):
    t = _t("2024-01-01T12:00:00")
    tracked = pd.DataFrame([{
        "cell_label": 1,
        "cell_uid": "UID1",
        "area": 4.0,
        "max_reflectivity": 40.0,
    }])
    events = pd.DataFrame([{
        "time": t,
        "event_type": "INITIATION",
        "source_cell_uid": None,
        "target_cell_uid": "UID1",
        "source_cell_label": None,
        "target_cell_label": 1,
        "cost": None,
        "is_dominant": False,
        "event_group_id": "2024-01-01T12:00:00Z:INITIATION:UID1",
    }])
    store.write_scan("r1", t, _cell_stats(1), tracked, events, _empty_cell_adjacency())

    cells = store.get_cells_by_scan("r1", t)
    assert "cell_uid" in cells.columns
    assert cells.iloc[0]["cell_uid"] == "UID1"

    ev = store.get_cell_events("r1", "UID1")
    assert "source_cell_uid" in ev.columns
    assert "target_cell_uid" in ev.columns
    assert ev.iloc[0]["target_cell_uid"] == "UID1"
