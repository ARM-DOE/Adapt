# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""PyXLMA clustering wrapper + frame converters, on a synthetic in-memory dataset.

No LMA files and no mocks: a CF event dataset is built in memory, clustered by
real pyXLMA, and the resulting frames are checked structurally.
"""

import numpy as np
import pandas as pd
import pytest
from pyxlma.lmalib.io import cf_netcdf

from adapt.modules.lma.reader import (
    cluster_flashes_with_stats,
    events_to_frame,
    flashes_to_frame,
)

pytestmark = pytest.mark.unit

# pyXLMA flash clustering uses scikit-learn's DBSCAN; skip until it is installed.
pytest.importorskip("sklearn")


def _synthetic_events():
    """Six sources forming two well-separated flashes (in time)."""
    ds = cf_netcdf.new_dataset(events=6)
    base = np.datetime64("2024-05-18T12:00:00")
    # cluster 1: t≈0s near (40.00, -88.00); cluster 2: t≈30s near (40.00, -88.00)
    secs = np.array([0.0, 0.02, 0.04, 30.0, 30.02, 30.04])
    ds["event_time"][:] = base + (secs * 1e9).astype("timedelta64[ns]")
    ds["event_latitude"][:] = np.array([40.0, 40.001, 40.002, 40.0, 40.001, 40.002])
    ds["event_longitude"][:] = np.array([-88.0, -88.001, -88.0, -88.0, -88.001, -88.0])
    ds["event_altitude"][:] = np.array([5000, 6000, 7000, 5000, 6000, 7000.0])
    ds["event_power"][:] = np.array([-10, -20, -30, -10, -20, -30.0])
    ds["event_chi2"][:] = 1.0
    ds["event_stations"][:] = 7
    ds["event_id"][:] = np.arange(6)
    ds["network_center_latitude"].data = np.float64(40.0)
    ds["network_center_longitude"].data = np.float64(-88.0)
    ds["network_center_altitude"].data = np.float64(0.0)
    return ds


def test_clustering_produces_two_flashes_and_consistent_frames():
    events = _synthetic_events()
    flashes_ds = cluster_flashes_with_stats(events, distance_m=3000.0, time_s=0.15)

    flashes = flashes_to_frame(flashes_ds)
    sources = events_to_frame(flashes_ds)

    assert len(flashes) == 2
    assert sources["station_count"].iloc[0] == pytest.approx(7.0)
    # every source rolls up under a real flash; counts are consistent
    assert flashes["source_count"].sum() == 6
    assert set(sources["flash_id"]).issuperset(set(flashes["flash_id"]))
    # flash-level fields are finite and sensible
    assert flashes["flash_area_km2"].notna().all()
    assert (flashes["flash_duration_s"] >= 0).all()
    assert pd.api.types.is_datetime64_any_dtype(flashes["flash_time"])
