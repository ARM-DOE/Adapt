# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The single PyXLMA boundary: read LMA files, cluster flashes, compute stats.

All ``pyxlma`` calls live here. Flash clustering is global and flash-first
(``cluster_flashes`` — pyXLMA's DBSCAN), never reimplemented and never restricted
to cell boundaries. Outputs are plain pandas DataFrames consumed by the pure
science layer (attribution, binning, aggregation), keeping the third-party
dependency isolated and the rest of the module testable without it.
"""

import numpy as np
import pandas as pd
from pyxlma.lmalib.flash.cluster import cluster_flashes
from pyxlma.lmalib.flash.properties import flash_stats
from pyxlma.lmalib.io import read as lma_read

_FILL = np.iinfo(np.uint64).max


def read_event_dataset(filenames: list[str]):
    """Read one or more LMA ASCII files into a pyXLMA event (source) dataset."""
    if not filenames:
        raise ValueError("No LMA input files provided.")
    return lma_read.dataset(filenames)


def cluster_flashes_with_stats(events_ds, distance_m: float, time_s: float):
    """Cluster sources into flashes and attach flash-level statistics."""
    clustered = cluster_flashes(events_ds, distance=distance_m, time=time_s)
    return flash_stats(clustered)


def flashes_to_frame(flashes_ds) -> pd.DataFrame:
    """Flatten valid flashes (>=1 event, not the noise label) to a DataFrame."""
    event_count = flashes_ds["flash_event_count"].values
    flash_id = flashes_ds["flash_id"].values
    valid = (event_count > 0) & (flash_id != _FILL)
    duration_s = (flashes_ds["flash_duration"].values / np.timedelta64(1, "s")).astype(float)
    df = pd.DataFrame(
        {
            "flash_id": flash_id,
            "flash_time": pd.to_datetime(flashes_ds["flash_time_start"].values),
            "flash_init_lat": flashes_ds["flash_init_latitude"].values,
            "flash_init_lon": flashes_ds["flash_init_longitude"].values,
            "flash_init_alt_m": flashes_ds["flash_init_altitude"].values,
            "flash_center_lat": flashes_ds["flash_center_latitude"].values,
            "flash_center_lon": flashes_ds["flash_center_longitude"].values,
            "flash_area_km2": flashes_ds["flash_area"].values,
            "flash_energy_gj": flashes_ds["flash_energy"].values,
            "flash_duration_s": duration_s,
            "source_count": event_count,
        }
    )
    return df[valid].reset_index(drop=True)


def events_to_frame(events_ds) -> pd.DataFrame:
    """Flatten sources to a DataFrame keyed by their parent flash id."""
    return pd.DataFrame(
        {
            "flash_id": events_ds["event_parent_flash_id"].values,
            "source_alt_m": events_ds["event_altitude"].values,
            "source_power_dbw": events_ds["event_power"].values,
            "source_chi2": events_ds["event_chi2"].values,
            "station_count": events_ds["event_stations"].values.astype(float),
        }
    )
