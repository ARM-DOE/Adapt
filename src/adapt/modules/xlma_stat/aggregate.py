# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Temporal binning and aggregation of minute-attributed flashes.

Single temporal unit: a 1-minute accumulation bin on lightning time. Each
attributed flash (and all of its sources, by the one-flash-one-denominator
rule) rolls up to exactly one ``(cell_uid, time)`` minute row; scan rows are
derived only from the minute rows (association happens once).
"""

from collections.abc import Mapping

import numpy as np
import pandas as pd


def floor_to_minute(times: pd.DatetimeIndex | pd.Series) -> pd.DatetimeIndex:
    """Floor lightning times to the start of their 1-minute accumulation bin."""
    return pd.DatetimeIndex(pd.to_datetime(times)).floor("min")


def aggregate_minutes(flashes: pd.DataFrame, sources: pd.DataFrame) -> pd.DataFrame:
    """Aggregate flashes + their sources into one row per ``(cell_uid, time)``.

    ``flashes`` carries flash-level scalars, the attributed ``cell_uid`` /
    ``time`` minute, and the minute mask's provenance (source/target scan,
    interpolation fraction — constant within a minute); ``sources`` carries
    per-source values tagged with the parent flash's ``cell_uid`` / ``time``.
    """
    keys = ["cell_uid", "time"]
    fg = flashes.groupby(keys, sort=True)
    flash_part = pd.DataFrame(
        {
            # mask provenance — identical for every flash in the minute
            "source_scan_time": fg["source_scan_time"].first(),
            "target_scan_time": fg["target_scan_time"].first(),
            "interpolation_fraction": fg["interpolation_fraction"].first(),
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
            "mean_attribution_dist_m": fg["attribution_dist_m"].mean(),
            "max_attribution_dist_m": fg["attribution_dist_m"].max(),
        }
    )

    sg = sources.groupby(keys, sort=True)
    source_part = pd.DataFrame(
        {
            "lightning_source_count": sg.size(),
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
    df["flash_rate_per_min"] = df["flash_count"].astype(float)  # 1-minute bins
    df["source_density_km2"] = np.where(
        df["total_flash_area_km2"] > 0,
        df["lightning_source_count"] / df["total_flash_area_km2"],
        np.nan,
    )
    return df


def aggregate_scans(
    minutes_df: pd.DataFrame, n_minutes_per_scan: Mapping[pd.Timestamp, int]
) -> pd.DataFrame:
    """Roll minute rows up to one row per ``(cell_uid, scan_time)``.

    A minute belongs to the scan its mask was advected toward
    (``target_scan_time``). ``n_minutes_per_scan`` is the number of minute
    masks each scan owns, so consumers can tell quiet minutes from missing
    coverage.
    """
    keys = ["cell_uid", "target_scan_time"]
    g = minutes_df.groupby(keys, sort=True)
    df = (
        pd.DataFrame(
            {
                "n_lightning_minutes": g.size(),
                "flash_count": g["flash_count"].sum(),
                "lightning_source_count": g["lightning_source_count"].sum(),
                "mean_flash_rate_per_min": g["flash_rate_per_min"].mean(),
                "max_flash_rate_per_min": g["flash_rate_per_min"].max(),
                "first_lightning_minute": g["time"].min(),
                "last_lightning_minute": g["time"].max(),
                "total_flash_energy": g["total_flash_energy"].sum(),
                "max_source_alt_m": g["max_source_alt_m"].max(),
                "max_flash_area_km2": g["max_flash_area_km2"].max(),
                "mean_interpolation_fraction": g["interpolation_fraction"].mean(),
            }
        )
        .reset_index()
        .rename(columns={"target_scan_time": "scan_time"})
    )
    df["n_minutes"] = df["scan_time"].map(lambda t: n_minutes_per_scan[pd.Timestamp(t)])
    return df
