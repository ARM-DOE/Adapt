# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for _volume_stats — load + join of cell_volume_stats onto a track.

Synthetic SQLite databases only; no stored fixtures.
"""

import sqlite3

import pandas as pd
import pytest

pytestmark = pytest.mark.unit

from adapt.consumers.live._volume_stats import (  # noqa: E402
    load_track_volume_stats,
    merge_volume_stats,
)


def _make_db(path, rows):
    """Write a cell_volume_stats table with the given rows (list of tuples)."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE cell_volume_stats ("
        "run_id TEXT, scan_time TEXT, cell_uid TEXT, cell_top_m REAL, "
        "PRIMARY KEY (run_id, scan_time, cell_uid))"
    )
    conn.executemany("INSERT INTO cell_volume_stats VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()


def test_load_returns_only_requested_track(tmp_path):
    db = tmp_path / "catalog.db"
    _make_db(
        db,
        [
            ("run1", "2026-06-06T00:00:00Z", "aaaa", 9000.0),
            ("run1", "2026-06-06T00:05:00Z", "aaaa", 11000.0),
            ("run1", "2026-06-06T00:00:00Z", "bbbb", 4000.0),
        ],
    )
    out = load_track_volume_stats(db, "run1", "aaaa")
    assert list(out["cell_top_m"]) == [9000.0, 11000.0]
    assert set(out["cell_uid"]) == {"aaaa"}


def test_load_missing_db_returns_empty(tmp_path):
    out = load_track_volume_stats(tmp_path / "absent.db", "run1", "aaaa")
    assert out.empty


def test_load_missing_table_returns_empty(tmp_path):
    db = tmp_path / "catalog.db"
    sqlite3.connect(str(db)).close()  # db with no cell_volume_stats table
    out = load_track_volume_stats(db, "run1", "aaaa")
    assert out.empty


def test_merge_joins_cloud_top_on_scan_id():
    track = pd.DataFrame(
        {
            "scan_id": ["sid-1", "sid-2"],
            "cell_uid": ["aaaa", "aaaa"],
            "cell_area_sqkm": [10.0, 12.0],
        }
    )
    vol = pd.DataFrame(
        {
            "run_id": ["run1", "run1"],
            "scan_id": ["sid-1", "sid-2"],
            "cell_uid": ["aaaa", "aaaa"],
            "cell_top_m": [9000.0, 11000.0],
        }
    )
    out = merge_volume_stats(track, vol)
    assert list(out["cell_top_m"]) == [9000.0, 11000.0]
    # Overlapping columns are taken from the track frame, not duplicated.
    assert "cell_uid_x" not in out.columns
    assert list(out["cell_area_sqkm"]) == [10.0, 12.0]


def test_merge_left_join_keeps_unmatched_track_rows():
    track = pd.DataFrame({"scan_id": ["s1", "s2"], "v": [1, 2]})
    vol = pd.DataFrame({"scan_id": ["s1"], "cell_top_m": [9000.0]})
    out = merge_volume_stats(track, vol)
    assert len(out) == 2
    assert out["cell_top_m"].isna().sum() == 1


def test_merge_empty_volume_is_noop():
    track = pd.DataFrame({"scan_id": ["s1"], "v": [1]})
    out = merge_volume_stats(track, pd.DataFrame())
    pd.testing.assert_frame_equal(out, track)


def test_merge_nonempty_frame_without_scan_id_raises():
    # Only pre-identity data can produce volume rows lacking scan_id — that is
    # a recreate condition, never a silent no-op merge.
    track = pd.DataFrame({"scan_id": ["s1"], "v": [1]})
    vol = pd.DataFrame({"scan_time": ["2026-06-06T00:00:00Z"], "cell_top_m": [9000.0]})
    with pytest.raises(ValueError, match="Recreate"):
        merge_volume_stats(track, vol)
