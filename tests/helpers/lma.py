# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Synthetic xLMA flash-sorted NetCDF builder shared by LMA tests."""

import numpy as np
import xarray as xr


def write_flash_sorted_nc(
    path, base_time: str, n_flashes: int, lat: float = 40.0, lon: float = -88.0
) -> None:
    """Flash-sorted file with all flashes initiating at (lat, lon); default = radar origin."""
    base = np.datetime64(base_time, "ns")
    fids = np.arange(n_flashes, dtype=np.uint64)
    parent = np.repeat(fids, 2)
    n_e = parent.size
    ds = xr.Dataset(
        {
            "flash_id": ("number_of_flashes", fids),
            "flash_event_count": ("number_of_flashes", np.full(n_flashes, 2, dtype=np.uint32)),
            "flash_duration": (
                "number_of_flashes",
                np.full(n_flashes, 100_000_000, dtype="timedelta64[ns]"),
            ),
            "flash_time_start": (
                "number_of_flashes",
                base + (np.arange(n_flashes) * np.timedelta64(10, "s")).astype("timedelta64[ns]"),
            ),
            "flash_init_latitude": (
                "number_of_flashes",
                np.full(n_flashes, lat, dtype=np.float32),
            ),
            "flash_init_longitude": (
                "number_of_flashes",
                np.full(n_flashes, lon, dtype=np.float32),
            ),
            "flash_init_altitude": (
                "number_of_flashes",
                np.full(n_flashes, 6000.0, dtype=np.float32),
            ),
            "flash_center_latitude": (
                "number_of_flashes",
                np.full(n_flashes, lat, dtype=np.float32),
            ),
            "flash_center_longitude": (
                "number_of_flashes",
                np.full(n_flashes, lon, dtype=np.float32),
            ),
            "flash_area": ("number_of_flashes", np.full(n_flashes, 12.5, dtype=np.float32)),
            "flash_energy": ("number_of_flashes", np.full(n_flashes, 0.3, dtype=np.float32)),
            "event_parent_flash_id": ("number_of_events", parent),
            "event_altitude": ("number_of_events", np.full(n_e, 7000.0, dtype=np.float32)),
            "event_power": ("number_of_events", np.full(n_e, -10.0, dtype=np.float32)),
            "event_chi2": ("number_of_events", np.full(n_e, 1.0, dtype=np.float32)),
            "event_stations": ("number_of_events", np.full(n_e, 8, dtype=np.uint8)),
        }
    )
    ds.to_netcdf(path)
