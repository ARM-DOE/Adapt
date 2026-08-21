# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for _volume_stats — load + join of cell_volume_stats onto a track.

Loads go through the public API only (a fake StoreClient here); joins are pure.
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.unit

from adapt.consumers.live._volume_stats import (  # noqa: E402
    load_track_volume_stats,
    merge_volume_stats,
)


class _FakeClient:
    """StoreClient stand-in: tables() discovery + filtered table() reads."""

    def __init__(self, rows):
        self._df = pd.DataFrame(rows, columns=["run_id", "scan_time", "cell_uid", "cell_top_m"])

    def tables(self, collection):
        names = ["cell_volume_stats"] if not self._df.empty else []
        return pd.DataFrame({"table_name": names})

    def table(self, name, collection, run_id=None, filters=None):
        df = self._df
        if run_id is not None:
            df = df[df["run_id"] == run_id]
        for column, value in (filters or {}).items():
            df = df[df[column] == value]
        return df.reset_index(drop=True)


def test_load_returns_only_requested_track():
    client = _FakeClient(
        [
            ("run1", "2026-06-06T00:05:00Z", "aaaa", 11000.0),
            ("run1", "2026-06-06T00:00:00Z", "aaaa", 9000.0),
            ("run1", "2026-06-06T00:00:00Z", "bbbb", 4000.0),
        ]
    )
    out = load_track_volume_stats(client, "KTST", "run1", "aaaa")
    assert list(out["cell_top_m"]) == [9000.0, 11000.0]  # scan_time-ordered
    assert set(out["cell_uid"]) == {"aaaa"}


def test_load_missing_table_returns_empty():
    out = load_track_volume_stats(_FakeClient([]), "KTST", "run1", "aaaa")
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
