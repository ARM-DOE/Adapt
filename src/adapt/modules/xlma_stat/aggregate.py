# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Temporal binning and per-(cell_uid, time_bin) aggregation of attributed flashes.

Single temporal unit: a 1-minute accumulation bin on lightning time. Each
attributed flash (and all of its sources, by the one-flash-one-denominator rule)
rolls up to exactly one ``(cell_uid, time_bin)`` row.
"""

import numpy as np
import pandas as pd

_BIN = pd.Timedelta(minutes=1)


def floor_to_minute(times: pd.DatetimeIndex | pd.Series) -> pd.DatetimeIndex:
    """Floor lightning times to the start of their 1-minute accumulation bin."""
    return pd.DatetimeIndex(pd.to_datetime(times)).floor("min")


def aggregate_cell_stats(flashes: pd.DataFrame, sources: pd.DataFrame) -> pd.DataFrame:
    """Aggregate flashes + their sources into one row per ``(cell_uid, time_bin)``.

    ``flashes`` carries flash-level scalars and the attributed ``cell_uid`` /
    ``time_bin``; ``sources`` carries per-source values already tagged with the
    parent flash's ``cell_uid`` / ``time_bin``.
    """
    keys = ["cell_uid", "time_bin"]
    fg = flashes.groupby(keys, sort=True)
    flash_part = pd.DataFrame(
        {
            "mean_flash_time": fg["flash_time"].mean().dt.floor("s"),
            "flash_count": fg.size(),
            "mean_flash_area_km2": fg["flash_area_km2"].mean(),
            "max_flash_area_km2": fg["flash_area_km2"].max(),
            "total_flash_area_km2": fg["flash_area_km2"].sum(),
            "mean_flash_init_alt_m": fg["flash_init_alt_m"].mean(),
            "mean_flash_energy": fg["flash_energy_gj"].mean(),
            "total_flash_energy": fg["flash_energy_gj"].sum(),
            "mean_flash_duration_s": fg["flash_duration_s"].mean(),
            "max_flash_duration_s": fg["flash_duration_s"].max(),
        }
    )

    sg = sources.groupby(keys, sort=True)
    source_part = pd.DataFrame(
        {
            "source_count": sg.size(),
            "median_source_alt_m": sg["source_alt_m"].median(),
            "p95_source_alt_m": sg["source_alt_m"].quantile(0.95),
            "max_source_alt_m": sg["source_alt_m"].max(),
            "mean_source_power_dbw": sg["source_power_dbw"].mean(),
            "max_source_power_dbw": sg["source_power_dbw"].max(),
            "mean_source_chi2": sg["source_chi2"].mean(),
            "mean_station_count": sg["station_count"].mean(),
        }
    )

    df = flash_part.join(source_part, how="left").reset_index()
    df["flash_rate_per_min"] = df["flash_count"] / (_BIN / pd.Timedelta(minutes=1))
    df["source_density_km2"] = np.where(
        df["total_flash_area_km2"] > 0,
        df["source_count"] / df["total_flash_area_km2"],
        np.nan,
    )
    return df


def build_flash_attribution(flashes: pd.DataFrame) -> pd.DataFrame:
    """One row per flash mapping it to its cell — the per-flash detail table."""
    cols = [
        "flash_id",
        "cell_uid",
        "time_bin",
        "flash_time",
        "flash_init_lat",
        "flash_init_lon",
        "flash_init_alt_m",
        "flash_center_lat",
        "flash_center_lon",
        "flash_area_km2",
        "flash_energy_gj",
        "flash_duration_s",
        "source_count",
        "attribution_dist_m",
    ]
    return flashes[cols].reset_index(drop=True)
