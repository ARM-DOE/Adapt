# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""xlma_stat science: lightning associated with minute-resolution cell masks.

The module performs lightning science only: each flash is matched to the mask
of *its own minute* (exact bin — no nearest-scan or projection logic), then
rolled up per (cell_uid, minute) and per (cell_uid, scan).
"""

import numpy as np
import pandas as pd
import pytest

from adapt.modules.xlma_stat.config import XlmaStatConfig
from adapt.modules.xlma_stat.module import MinuteMask, XlmaStatistics

pytestmark = pytest.mark.unit

pytest.importorskip("pyproj")

RADAR = (40.0, -88.0)
_GRID = (np.arange(21) - 10) * 1000.0  # metres; radar at index 10


def _mask(minute: str, cell_col: int | None, target_scan: str, fraction: float) -> MinuteMask:
    """One minute mask; cell_col is the left column of a 3x3 cell, None = no cell."""
    labels = np.zeros((21, 21), dtype=np.int32)
    if cell_col is not None:
        labels[9:12, cell_col : cell_col + 3] = 1
    return MinuteMask(
        minute_time=pd.Timestamp(minute),
        cell_labels=labels,
        x=_GRID,
        y=_GRID,
        cell_uid_lut=np.array(["NONE", "uid-A"], dtype=np.str_),
        source_scan_time=pd.Timestamp(target_scan) - pd.Timedelta(minutes=3),
        target_scan_time=pd.Timestamp(target_scan),
        interpolation_fraction=fraction,
    )


def _flash(flash_id: int, time: str, lat: float = 40.0, lon: float = -88.0) -> dict:
    return {
        "flash_id": flash_id,
        "flash_time": pd.Timestamp(time),
        "flash_init_lat": lat,
        "flash_init_lon": lon,
        "flash_init_alt_m": 6000.0,
        "flash_center_lat": lat,
        "flash_center_lon": lon,
        "flash_area_km2": 10.0,
        "flash_energy_gj": 2.0,
        "flash_duration_s": 0.5,
        "source_count": 2,
    }


def _sources(flash_ids: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "flash_id": np.repeat(flash_ids, 2),
            "source_alt_m": 7000.0,
            "source_power_dbw": -10.0,
            "source_chi2": 1.0,
            "station_count": 8.0,
        }
    )


def _compute(flashes_rows, sources, masks):
    algo = XlmaStatistics(XlmaStatConfig(input_dir="/unused", search_radius_m=500.0))
    return algo.compute(pd.DataFrame(flashes_rows), sources, masks, RADAR)


# Three minute masks targeting scan 19:03; the cell sits at the radar origin
# (centre, col 9) only in the 19:02 mask — elsewhere it is far west (col 2).
_MASKS = [
    _mask("2024-05-18T19:01", cell_col=2, target_scan="2024-05-18T19:03", fraction=1 / 3),
    _mask("2024-05-18T19:02", cell_col=9, target_scan="2024-05-18T19:03", fraction=2 / 3),
    _mask("2024-05-18T19:03", cell_col=2, target_scan="2024-05-18T19:03", fraction=1.0),
]


def test_flash_is_matched_to_its_own_minutes_mask():
    """A flash at 19:02 hits the cell (centred then); at 19:01 the same point is empty."""
    minutes_df, _ = _compute(
        [_flash(1, "2024-05-18T19:02:30"), _flash(2, "2024-05-18T19:01:10")],
        _sources([1, 2]),
        _MASKS,
    )

    by_uid = minutes_df.set_index("cell_uid")
    assert by_uid.loc["uid-A", "flash_count"] == 1
    assert pd.Timestamp(by_uid.loc["uid-A", "time"]) == pd.Timestamp("2024-05-18T19:02")
    assert by_uid.loc["UNATTRIBUTED", "flash_count"] == 1


def test_minute_rows_carry_mask_provenance():
    minutes_df, _ = _compute([_flash(1, "2024-05-18T19:02:30")], _sources([1]), _MASKS)

    row = minutes_df.iloc[0]
    assert pd.Timestamp(row["source_scan_time"]) == pd.Timestamp("2024-05-18T19:00")
    assert pd.Timestamp(row["target_scan_time"]) == pd.Timestamp("2024-05-18T19:03")
    assert row["interpolation_fraction"] == pytest.approx(2 / 3)
    assert row["lightning_source_count"] == 2


def test_scan_table_aggregates_member_minutes():
    """Flashes in two of the three minutes of scan 19:03 roll up to one scan row."""
    minutes_df, scan_df = _compute(
        [
            _flash(1, "2024-05-18T19:02:10"),
            _flash(2, "2024-05-18T19:02:40"),
            _flash(3, "2024-05-18T19:03:20", lat=40.0),  # 19:03 mask: cell far west
        ],
        _sources([1, 2, 3]),
        _MASKS,
    )

    row = scan_df.set_index("cell_uid").loc["uid-A"]
    assert pd.Timestamp(row["scan_time"]) == pd.Timestamp("2024-05-18T19:03")
    assert row["flash_count"] == 2
    assert row["n_minutes"] == 3  # minutes the scan's masks cover
    assert row["n_lightning_minutes"] == 1  # only 19:02 had uid-A lightning
    assert pd.Timestamp(row["first_lightning_minute"]) == pd.Timestamp("2024-05-18T19:02")
    # scan totals equal the sum of the member minute rows
    member = minutes_df[minutes_df["cell_uid"] == "uid-A"]
    assert row["flash_count"] == member["flash_count"].sum()
    assert row["lightning_source_count"] == member["lightning_source_count"].sum()


def test_flashes_outside_mask_coverage_are_excluded():
    """No mask exists for 18:00 — the flash is outside the run's coverage."""
    minutes_df, scan_df = _compute([_flash(1, "2024-05-18T18:00:00")], _sources([1]), _MASKS)

    assert minutes_df.empty
    assert scan_df.empty


def test_empty_flashes_returns_empty_frames_with_columns():
    minutes_df, scan_df = _compute([], _sources([]), _MASKS)

    assert minutes_df.empty and scan_df.empty
    for col in ("cell_uid", "time", "flash_count", "interpolation_fraction"):
        assert col in minutes_df.columns
    for col in ("cell_uid", "scan_time", "flash_count", "n_minutes"):
        assert col in scan_df.columns


def test_raises_without_minute_masks():
    with pytest.raises(ValueError, match="minute mask"):
        _compute([_flash(1, "2024-05-18T19:02:00")], _sources([1]), [])
