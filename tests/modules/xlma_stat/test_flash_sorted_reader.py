# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Flash-sorted NetCDF reader: synthetic pyXLMA-style files, no pyxlma involved.

Files are written in-test with the same variable names, dtypes, and fill
conventions as pyXLMA flash-sorted output (e.g. LYLOUT_*_map*_chi*.nc), then
read back through read_flash_sorted_frames.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.modules.xlma_stat.reader import read_flash_sorted_frames

pytestmark = pytest.mark.unit

_PARENT_FILL = np.uint64(np.iinfo(np.uint64).max)


def _write_flash_sorted_nc(path, base_time: str, flash_ids: list[int]) -> None:
    """Write a minimal pyXLMA-style flash-sorted file: 2 events per flash + 1 noise event."""
    n_f = len(flash_ids)
    base = np.datetime64(base_time, "ns")
    fids = np.array(flash_ids, dtype=np.uint64)

    parent = np.repeat(fids, 2)
    parent = np.concatenate([parent, [_PARENT_FILL]])  # one unassigned (noise) event
    n_e = parent.size

    ds = xr.Dataset(
        {
            "flash_id": ("number_of_flashes", fids),
            "flash_event_count": ("number_of_flashes", np.full(n_f, 2, dtype=np.uint32)),
            "flash_duration": (
                "number_of_flashes",
                np.full(n_f, 100_000_000, dtype="timedelta64[ns]"),  # 0.1 s
            ),
            "flash_time_start": (
                "number_of_flashes",
                base + (np.arange(n_f) * np.timedelta64(60, "s")).astype("timedelta64[ns]"),
            ),
            "flash_init_latitude": ("number_of_flashes", np.full(n_f, 40.0, dtype=np.float32)),
            "flash_init_longitude": ("number_of_flashes", np.full(n_f, -88.0, dtype=np.float32)),
            "flash_init_altitude": ("number_of_flashes", np.full(n_f, 6000.0, dtype=np.float32)),
            "flash_center_latitude": ("number_of_flashes", np.full(n_f, 40.0, dtype=np.float32)),
            "flash_center_longitude": (
                "number_of_flashes",
                np.full(n_f, -88.0, dtype=np.float32),
            ),
            "flash_area": ("number_of_flashes", np.full(n_f, 12.5, dtype=np.float32)),
            "flash_energy": ("number_of_flashes", np.full(n_f, 0.3, dtype=np.float32)),
            "event_parent_flash_id": ("number_of_events", parent),
            "event_altitude": ("number_of_events", np.full(n_e, 7000.0, dtype=np.float32)),
            "event_power": ("number_of_events", np.full(n_e, -10.0, dtype=np.float32)),
            "event_chi2": ("number_of_events", np.full(n_e, 1.0, dtype=np.float32)),
            "event_stations": ("number_of_events", np.full(n_e, 8, dtype=np.uint8)),
        }
    )
    ds.to_netcdf(
        path,
        encoding={
            "event_parent_flash_id": {"_FillValue": _PARENT_FILL},
            "event_stations": {"_FillValue": np.uint8(0)},
        },
    )


def test_single_file_frames_match_pyxlma_layout(tmp_path):
    path = tmp_path / "LYLOUT_240518_120000_3600_map.nc"
    _write_flash_sorted_nc(path, "2024-05-18T12:00:00", flash_ids=[0, 1, 2])

    flashes, sources = read_flash_sorted_frames([str(path)])

    assert len(flashes) == 3
    assert len(sources) == 6  # the unassigned (noise) event is dropped
    assert pd.api.types.is_datetime64_any_dtype(flashes["flash_time"])
    assert flashes["flash_duration_s"].tolist() == pytest.approx([0.1, 0.1, 0.1])
    assert flashes["flash_area_km2"].tolist() == pytest.approx([12.5] * 3)
    # every remaining source joins a real flash
    assert set(sources["flash_id"]) == set(flashes["flash_id"])
    assert sources["station_count"].tolist() == pytest.approx([8.0] * 6)


def test_multiple_files_get_unique_flash_ids(tmp_path):
    a = tmp_path / "a.nc"
    b = tmp_path / "b.nc"
    _write_flash_sorted_nc(a, "2024-05-18T12:00:00", flash_ids=[0, 1])
    _write_flash_sorted_nc(b, "2024-05-18T13:00:00", flash_ids=[0, 1])  # ids restart

    flashes, sources = read_flash_sorted_frames([str(a), str(b)])

    assert len(flashes) == 4
    assert flashes["flash_id"].is_unique
    # each flash keeps exactly its own two sources across the re-keying
    counts = sources.groupby("flash_id").size()
    assert counts.tolist() == [2, 2, 2, 2]
    assert set(counts.index) == set(flashes["flash_id"])


def test_non_flash_sorted_netcdf_is_rejected(tmp_path):
    path = tmp_path / "events_only.nc"
    xr.Dataset({"event_latitude": ("number_of_events", np.zeros(3, dtype=np.float32))}).to_netcdf(
        path
    )

    with pytest.raises(ValueError, match="not a flash-sorted LMA NetCDF"):
        read_flash_sorted_frames([str(path)])


def test_empty_file_list_is_rejected():
    with pytest.raises(ValueError, match="No LMA flash-sorted NetCDF files"):
        read_flash_sorted_frames([])
