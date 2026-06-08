# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""LMA science orchestrator: attribute clustered flashes to cells, bin, aggregate.

Pure — operates on already-clustered frames + injected scan masks, so it needs no
PyXLMA / scikit-learn. Synthetic flashes with hand-checkable attribution.
"""

import numpy as np
import pandas as pd
import pytest

from adapt.modules.lma.config import LMAConfig
from adapt.modules.lma.module import LMACellStatistics, ScanMask

pytestmark = pytest.mark.unit

RADAR = (40.0, -88.0)


def _scan_mask():
    coords = (np.arange(11) - 5) * 200.0  # metres, radar at index 5 (x=y=0)
    labels = np.zeros((11, 11), dtype=np.int32)
    labels[5, 5] = 1  # one cell at the radar origin
    lut = np.array(["NONE", "uid-A"], dtype=np.str_)
    return ScanMask(
        scan_time=pd.Timestamp("2024-05-18T12:00:00"),
        cell_labels=labels,
        x=coords,
        y=coords,
        cell_uid_lut=lut,
    )


def _flashes():
    return pd.DataFrame(
        {
            "flash_id": [1, 2],
            "flash_time": pd.to_datetime(["2024-05-18T12:00:05", "2024-05-18T12:00:40"]),
            # flash 1 at radar origin (inside cell); flash 2 one degree north (off grid)
            "flash_init_lat": [40.0, 41.0],
            "flash_init_lon": [-88.0, -88.0],
            "flash_init_alt_m": [6000.0, 8000.0],
            "flash_center_lat": [40.0, 41.0],
            "flash_center_lon": [-88.0, -88.0],
            "flash_area_km2": [10.0, 20.0],
            "flash_energy_gj": [2.0, 4.0],
            "flash_duration_s": [4.0, 6.0],
            "source_count": [3, 1],
        }
    )


def _sources():
    return pd.DataFrame(
        {
            "flash_id": [1, 1, 1, 2],
            "source_alt_m": [5000.0, 6000.0, 7000.0, 8000.0],
            "source_power_dbw": [-10.0, -20.0, -30.0, -40.0],
            "source_chi2": [1.0, 1.0, 1.0, 2.0],
            "station_count": [7.0, 7.0, 7.0, 6.0],
        }
    )


def _algo():
    return LMACellStatistics(LMAConfig(input_dir="/unused", search_radius_m=500.0))


def test_attributes_inside_flash_and_marks_offgrid_unattributed():
    cell_stats, flash_attr = _algo().compute(_flashes(), _sources(), [_scan_mask()], RADAR)

    by_uid = flash_attr.set_index("flash_id")["cell_uid"]
    assert by_uid[1] == "uid-A"
    assert by_uid[2] == "UNATTRIBUTED"


def test_cell_stats_grouped_by_cell_and_bin():
    cell_stats, _ = _algo().compute(_flashes(), _sources(), [_scan_mask()], RADAR)

    stats = cell_stats.set_index("cell_uid")
    assert set(stats.index) == {"uid-A", "UNATTRIBUTED"}
    assert stats.loc["uid-A", "flash_count"] == 1
    assert stats.loc["uid-A", "source_count"] == 3
    assert stats.loc["uid-A", "mean_flash_area_km2"] == pytest.approx(10.0)
    assert stats.loc["UNATTRIBUTED", "flash_count"] == 1


def test_empty_flashes_returns_empty_frames_with_columns():
    empty_f = _flashes().iloc[0:0]
    empty_s = _sources().iloc[0:0]
    cell_stats, flash_attr = _algo().compute(empty_f, empty_s, [_scan_mask()], RADAR)

    assert cell_stats.empty and flash_attr.empty
    assert "cell_uid" in cell_stats.columns and "time_bin" in cell_stats.columns
    assert "attribution_dist_m" in flash_attr.columns


def test_nearest_scan_chosen_for_attribution():
    # Two scans; the cell only exists in the later one. A flash near that time
    # must attribute to uid-A, proving closest-scan selection.
    early = _scan_mask()
    early = ScanMask(
        scan_time=pd.Timestamp("2024-05-18T11:00:00"),
        cell_labels=np.zeros((11, 11), dtype=np.int32),
        x=early.x,
        y=early.y,
        cell_uid_lut=np.array(["NONE"], dtype=np.str_),
    )
    late = _scan_mask()
    _, flash_attr = _algo().compute(
        _flashes().iloc[[0]], _sources().iloc[[0]], [early, late], RADAR
    )
    assert flash_attr.iloc[0]["cell_uid"] == "uid-A"
