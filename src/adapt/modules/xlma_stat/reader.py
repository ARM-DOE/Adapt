# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The LMA input boundary: xLMA flash-sorted NetCDF, read with plain xarray.

Adapt consumes only flash-sorted NetCDF (xLMA/pyXLMA's own output format, e.g.
``LYLOUT_*_map*.nc``): standard variable names, flashes already clustered, and
the event→flash relation included. Raw LYLOUT ASCII sources must be converted
to flash-sorted NetCDF in xLMA first — ``pyxlma`` is not on conda and is not a
dependency of Adapt; clustering happens in the user's xLMA workflow, never here.

Outputs are plain pandas DataFrames consumed by the pure science layer
(attribution, binning, aggregation).
"""

import numpy as np
import pandas as pd
import xarray as xr

_FILL = np.iinfo(np.uint64).max

# Everything flashes_to_frame / events_to_frame read. A flash-sorted NetCDF
# missing any of these is rejected loudly, not partially ingested.
_FLASH_SORTED_REQUIRED = (
    "flash_id",
    "flash_event_count",
    "flash_duration",
    "flash_time_start",
    "flash_init_latitude",
    "flash_init_longitude",
    "flash_init_altitude",
    "flash_center_latitude",
    "flash_center_longitude",
    "flash_area",
    "flash_energy",
    "event_parent_flash_id",
    "event_altitude",
    "event_power",
    "event_chi2",
    "event_stations",
)


def read_flash_sorted_frames(filenames: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read xLMA flash-sorted NetCDF files into ``(flashes, sources)`` frames.

    Each file already carries clustered flashes (``flash_*`` variables) and the
    event→flash relation (``event_parent_flash_id``), so this path uses xarray
    only. Flash ids are unique within one file but restart between files; each
    file's ids are offset so ``flash_id`` stays a unique key across the combined
    frames (it is part of the primary key of ``lma_flash_attribution``).
    """
    if not filenames:
        raise ValueError("No LMA flash-sorted NetCDF files provided.")

    flash_parts: list[pd.DataFrame] = []
    source_parts: list[pd.DataFrame] = []
    next_id = np.uint64(0)
    for path in filenames:
        with xr.open_dataset(path, decode_timedelta=True) as ds:
            missing = [v for v in _FLASH_SORTED_REQUIRED if v not in ds.variables]
            if missing:
                raise ValueError(
                    f"'{path}' is not a flash-sorted LMA NetCDF: missing variables "
                    f"{missing}. Provide xLMA flash-sorted output (*.nc) — convert "
                    "raw LYLOUT ASCII sources in xLMA/pyxlma first."
                )
            flashes = flashes_to_frame(ds)
            sources = events_to_frame(ds)

        # NetCDF fill decoding turns unassigned parent flash ids into NaN floats;
        # drop those noise events and restore the integer key so the
        # source ↔ flash join holds.
        sources = sources[sources["flash_id"].notna()].copy()
        sources["flash_id"] = sources["flash_id"].astype(np.uint64) + next_id
        flashes["flash_id"] = flashes["flash_id"].astype(np.uint64) + next_id
        if len(flashes):
            next_id = np.uint64(int(flashes["flash_id"].max()) + 1)
        flash_parts.append(flashes)
        source_parts.append(sources)

    return (
        pd.concat(flash_parts, ignore_index=True),
        pd.concat(source_parts, ignore_index=True),
    )


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
