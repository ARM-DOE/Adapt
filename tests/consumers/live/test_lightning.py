# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unit tests for _lightning.merge_lightning — join lma_cell_stats onto a track.

Lightning is keyed by 1-minute time; the track timeline by scan_time. The
merge aligns them by flooring scan_time to the minute. No IO.
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.unit

from adapt.consumers.live._lightning import merge_lightning  # noqa: E402


def _track_df():
    return pd.DataFrame(
        {
            "scan_time": ["2024-05-18T12:00:30Z", "2024-05-18T12:01:10Z"],
            "cell_area_sqkm": [100.0, 110.0],
        }
    )


def _lma_df():
    return pd.DataFrame(
        {
            "time": ["2024-05-18T12:00:00Z", "2024-05-18T12:01:00Z"],
            "flash_count": [3, 7],
            "source_count": [30, 70],
        }
    )


def test_joins_lightning_by_minute():
    out = merge_lightning(_track_df(), _lma_df())
    assert list(out["flash_count"]) == [3, 7]
    assert list(out["source_count"]) == [30, 70]
    # original columns preserved
    assert list(out["cell_area_sqkm"]) == [100.0, 110.0]


def test_scans_in_same_minute_get_same_bin():
    track = pd.DataFrame({"scan_time": ["2024-05-18T12:00:05Z", "2024-05-18T12:00:50Z"]})
    out = merge_lightning(track, _lma_df())
    assert list(out["flash_count"]) == [3, 3]


def test_empty_lightning_returns_track_unchanged():
    track = _track_df()
    out = merge_lightning(track, pd.DataFrame())
    assert list(out.columns) == list(track.columns)
    assert "flash_count" not in out.columns


def test_missing_bin_yields_nan():
    track = pd.DataFrame({"scan_time": ["2024-05-18T12:05:00Z"]})
    out = merge_lightning(track, _lma_df())
    assert pd.isna(out["flash_count"].iloc[0])
