# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""1-minute binning + per-(cell_uid, time_bin) aggregation with known outputs."""

import pandas as pd
import pytest

from adapt.modules.lma.aggregate import (
    aggregate_cell_stats,
    build_flash_attribution,
    floor_to_minute,
)

pytestmark = pytest.mark.unit


def test_floor_to_minute_drops_seconds():
    t = pd.to_datetime(["2024-05-18T12:00:45", "2024-05-18T12:01:10"])
    binned = floor_to_minute(t)
    assert list(binned) == list(pd.to_datetime(["2024-05-18T12:00:00", "2024-05-18T12:01:00"]))


def _flashes():
    return pd.DataFrame(
        {
            "flash_id": [1, 2],
            "cell_uid": ["A", "A"],
            "time_bin": pd.to_datetime(["2024-05-18T12:00:00"] * 2),
            "flash_time": pd.to_datetime(["2024-05-18T12:00:00", "2024-05-18T12:00:30"]),
            "flash_area_km2": [10.0, 20.0],
            "flash_energy_gj": [2.0, 4.0],
            "flash_duration_s": [4.0, 6.0],
            "flash_init_alt_m": [5000.0, 7000.0],
            "flash_init_lat": [40.0, 40.1],
            "flash_init_lon": [-88.0, -88.1],
            "flash_center_lat": [40.0, 40.1],
            "flash_center_lon": [-88.0, -88.1],
            "source_count": [3, 1],
            "attribution_dist_m": [0.0, 200.0],
        }
    )


def _sources():
    return pd.DataFrame(
        {
            "cell_uid": ["A", "A", "A", "A"],
            "time_bin": pd.to_datetime(["2024-05-18T12:00:00"] * 4),
            "source_alt_m": [1000.0, 2000.0, 3000.0, 9000.0],
            "source_power_dbw": [-10.0, -20.0, -30.0, -40.0],
            "source_chi2": [1.0, 1.0, 1.0, 1.0],
            "station_count": [6.0, 6.0, 7.0, 7.0],
        }
    )


def test_aggregate_cell_stats_known_values():
    df = aggregate_cell_stats(_flashes(), _sources())
    assert len(df) == 1
    row = df.iloc[0]

    assert row.cell_uid == "A"
    assert row.time_bin == pd.Timestamp("2024-05-18T12:00:00")
    assert row.mean_flash_time == pd.Timestamp("2024-05-18T12:00:15")
    assert row.flash_count == 2
    assert row.source_count == 4
    assert row.flash_rate_per_min == pytest.approx(2.0)
    assert row.mean_flash_area_km2 == pytest.approx(15.0)
    assert row.max_flash_area_km2 == pytest.approx(20.0)
    assert row.total_flash_area_km2 == pytest.approx(30.0)
    assert row.mean_flash_init_alt_m == pytest.approx(6000.0)
    assert row.median_source_alt_m == pytest.approx(2500.0)
    assert row.p95_source_alt_m == pytest.approx(8100.0)
    assert row.max_source_alt_m == pytest.approx(9000.0)
    assert row.mean_flash_energy == pytest.approx(3.0)
    assert row.total_flash_energy == pytest.approx(6.0)
    assert row.mean_source_power_dbw == pytest.approx(-25.0)
    assert row.max_source_power_dbw == pytest.approx(-10.0)
    assert row.mean_flash_duration_s == pytest.approx(5.0)
    assert row.max_flash_duration_s == pytest.approx(6.0)
    assert row.mean_source_chi2 == pytest.approx(1.0)
    assert row.mean_station_count == pytest.approx(6.5)
    assert row.source_density_km2 == pytest.approx(4.0 / 30.0)


def test_aggregate_separates_cells_and_bins():
    flashes = _flashes()
    flashes.loc[1, "cell_uid"] = "B"
    sources = _sources()
    sources.loc[3, "cell_uid"] = "B"
    df = aggregate_cell_stats(flashes, sources)
    assert set(df.cell_uid) == {"A", "B"}


def test_build_flash_attribution_one_row_per_flash():
    fa = build_flash_attribution(_flashes())
    assert len(fa) == 2
    assert set(["flash_id", "cell_uid", "flash_time", "attribution_dist_m"]).issubset(fa.columns)
    assert list(fa.flash_id) == [1, 2]
