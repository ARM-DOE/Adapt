# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""LMA cell-statistics science: attribute clustered flashes to cells and roll up.

Pure with respect to PyXLMA: it consumes already-clustered flash/source frames
(from ``reader``) plus the injected per-scan cell masks, and produces the two
extension-table frames. Attribution is by flash initiation point; the mask used
for a flash is the scan whose time is closest to the flash's initiation time
(the flash's representative time).
"""

from dataclasses import dataclass

import numpy as np
import pandas as pd

from adapt.modules.lma.aggregate import (
    aggregate_cell_stats,
    build_flash_attribution,
    floor_to_minute,
)
from adapt.modules.lma.attribution import UNATTRIBUTED, attribute_flashes
from adapt.modules.lma.config import LMAConfig
from adapt.modules.lma.geo import project_lonlat_to_xy

_CELL_STATS_COLUMNS = [
    "cell_uid",
    "time_bin",
    "mean_flash_time",
    "flash_count",
    "source_count",
    "flash_rate_per_min",
    "mean_flash_area_km2",
    "max_flash_area_km2",
    "total_flash_area_km2",
    "mean_flash_init_alt_m",
    "median_source_alt_m",
    "p95_source_alt_m",
    "max_source_alt_m",
    "mean_flash_energy",
    "total_flash_energy",
    "mean_source_power_dbw",
    "max_source_power_dbw",
    "mean_flash_duration_s",
    "max_flash_duration_s",
    "mean_source_chi2",
    "mean_station_count",
    "source_density_km2",
]

_FLASH_ATTR_COLUMNS = [
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


@dataclass(frozen=True)
class ScanMask:
    """One scan's cell mask + grid, as injected by the PostProcessor."""

    scan_time: pd.Timestamp
    cell_labels: np.ndarray  # 2D (ny, nx) int
    x: np.ndarray  # 1D metres
    y: np.ndarray  # 1D metres
    cell_uid_lut: np.ndarray  # 1D str, indexed by cell_label


class LMACellStatistics:
    """Attribute flashes to cells, bin to 1 minute, and aggregate."""

    def __init__(self, config: LMAConfig) -> None:
        self.config = config

    def compute(
        self,
        flashes: pd.DataFrame,
        sources: pd.DataFrame,
        scan_masks: list[ScanMask],
        radar_origin: tuple[float, float],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(lma_cell_stats, lma_flash_attribution)`` frames."""
        if flashes.empty:
            return (
                pd.DataFrame(columns=_CELL_STATS_COLUMNS),
                pd.DataFrame(columns=_FLASH_ATTR_COLUMNS),
            )
        if not scan_masks:
            raise ValueError("No cell masks available in the repository for LMA attribution.")

        flashes = flashes.copy()
        flashes["time_bin"] = floor_to_minute(flashes["flash_time"])

        radar_lat, radar_lon = radar_origin
        fx, fy = project_lonlat_to_xy(
            flashes["flash_init_lon"].to_numpy(),
            flashes["flash_init_lat"].to_numpy(),
            radar_lat,
            radar_lon,
        )

        cell_uid, dist = self._attribute_per_scan(flashes, fx, fy, scan_masks)
        flashes["cell_uid"] = cell_uid
        flashes["attribution_dist_m"] = dist

        sources_attr = sources.merge(
            flashes[["flash_id", "cell_uid", "time_bin"]], on="flash_id", how="inner"
        )

        cell_stats = aggregate_cell_stats(flashes, sources_attr)
        flash_attr = build_flash_attribution(flashes)
        return cell_stats[_CELL_STATS_COLUMNS], flash_attr

    def _attribute_per_scan(
        self,
        flashes: pd.DataFrame,
        fx: np.ndarray,
        fy: np.ndarray,
        scan_masks: list[ScanMask],
    ) -> tuple[np.ndarray, np.ndarray]:
        """Pick the closest-in-time scan per flash, then attribute vectorized per scan."""
        scan_times = np.array([pd.Timestamp(m.scan_time).to_datetime64() for m in scan_masks])
        flash_times = pd.to_datetime(flashes["flash_time"]).to_numpy()
        # nearest scan index for every flash (vectorized |Δt| argmin)
        deltas = np.abs(flash_times[:, None] - scan_times[None, :])
        nearest = deltas.argmin(axis=1)

        uids = np.full(len(flashes), UNATTRIBUTED, dtype=object)
        dist = np.full(len(flashes), np.nan, dtype=float)
        for scan_idx in np.unique(nearest):
            mask = scan_masks[scan_idx]
            sel = np.where(nearest == scan_idx)[0]
            u, d = attribute_flashes(
                fx[sel],
                fy[sel],
                mask.cell_labels,
                mask.x,
                mask.y,
                mask.cell_uid_lut,
                search_radius_m=self.config.search_radius_m,
            )
            uids[sel] = u
            dist[sel] = d
        return uids.astype(np.str_), dist
