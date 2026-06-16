# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""xlma_stat science: associate flashes with minute-resolution cell masks.

Lightning science only. Each flash is matched to the cell mask of *its own
minute* (exact 1-minute bin) — the masks themselves are produced upstream by
the projection module and read through persistence; this module never projects
cells, interpolates masks, or estimates motion. Outputs roll up per
``(cell_uid, minute)`` (the authoritative association) and per
``(cell_uid, scan)`` (the radar-aligned aggregate, derived only from the
minute rows).
"""

import logging
from collections import Counter
from dataclasses import dataclass

import numpy as np
import pandas as pd

from adapt.modules.xlma_stat.aggregate import (
    aggregate_minutes,
    aggregate_scans,
    floor_to_minute,
)
from adapt.modules.xlma_stat.attribution import UNATTRIBUTED, attribute_flashes
from adapt.modules.xlma_stat.config import XlmaStatConfig
from adapt.modules.xlma_stat.geo import project_lonlat_to_xy

logger = logging.getLogger(__name__)

_MINUTES_COLUMNS = [
    "cell_uid",
    "time",
    "source_scan_time",
    "target_scan_time",
    "interpolation_fraction",
    "mean_flash_time",
    "flash_count",
    "lightning_source_count",
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
    "mean_attribution_dist_m",
    "max_attribution_dist_m",
]

_SCAN_COLUMNS = [
    "cell_uid",
    "scan_time",
    "n_minutes",
    "n_lightning_minutes",
    "flash_count",
    "lightning_source_count",
    "mean_flash_rate_per_min",
    "max_flash_rate_per_min",
    "first_lightning_minute",
    "last_lightning_minute",
    "total_flash_energy",
    "max_source_alt_m",
    "max_flash_area_km2",
    "mean_interpolation_fraction",
]


@dataclass(frozen=True)
class MinuteMask:
    """One minute's cell mask + grid, as injected by the PostProcessor."""

    minute_time: pd.Timestamp
    cell_labels: np.ndarray  # 2D (ny, nx) int
    x: np.ndarray  # 1D metres
    y: np.ndarray  # 1D metres
    cell_uid_lut: np.ndarray  # 1D str, indexed by cell_label
    source_scan_time: pd.Timestamp  # scan whose labels the mask carries
    target_scan_time: pd.Timestamp  # scan the mask was advected toward
    interpolation_fraction: float  # 0.0 = real segmentation, (0, 1] = advected


class XlmaStatistics:
    """Match flashes to their minute's mask, then aggregate per minute and scan."""

    def __init__(self, config: XlmaStatConfig) -> None:
        self.config = config

    def compute(
        self,
        flashes: pd.DataFrame,
        sources: pd.DataFrame,
        minute_masks: list[MinuteMask],
        radar_origin: tuple[float, float],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(xlma_stat_minutes, xlma_stat_scan)`` frames."""
        if not minute_masks:
            raise ValueError(
                "No minute masks available for xlma_stat association — the run "
                "has no registration_minutes geometry; reprocess it with the "
                "current pipeline."
            )
        if flashes.empty:
            return self._empty_frames()

        masks_by_minute = {pd.Timestamp(m.minute_time): m for m in minute_masks}

        flashes = flashes.copy()
        flashes["time"] = floor_to_minute(flashes["flash_time"])
        covered = flashes["time"].isin(masks_by_minute)
        if not covered.all():
            logger.info(
                "xlma_stat: excluding %d of %d flashes outside the run's minute-mask "
                "coverage [%s .. %s]",
                int((~covered).sum()),
                len(flashes),
                min(masks_by_minute),
                max(masks_by_minute),
            )
            flashes = flashes[covered].reset_index(drop=True)
        if flashes.empty:
            return self._empty_frames()

        radar_lat, radar_lon = radar_origin
        fx, fy = project_lonlat_to_xy(
            flashes["flash_init_lon"].to_numpy(),
            flashes["flash_init_lat"].to_numpy(),
            radar_lat,
            radar_lon,
        )

        n = len(flashes)
        uids = np.full(n, UNATTRIBUTED, dtype=object)
        dist = np.full(n, np.nan, dtype=float)
        source_scan = np.empty(n, dtype="datetime64[ns]")
        target_scan = np.empty(n, dtype="datetime64[ns]")
        fraction = np.full(n, np.nan, dtype=float)

        for minute, sel in flashes.groupby("time").indices.items():
            mask = masks_by_minute[pd.Timestamp(minute)]
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
            source_scan[sel] = pd.Timestamp(mask.source_scan_time).to_datetime64()
            target_scan[sel] = pd.Timestamp(mask.target_scan_time).to_datetime64()
            fraction[sel] = mask.interpolation_fraction

        flashes["cell_uid"] = uids.astype(np.str_)
        flashes["attribution_dist_m"] = dist
        flashes["source_scan_time"] = source_scan
        flashes["target_scan_time"] = target_scan
        flashes["interpolation_fraction"] = fraction

        sources_attr = sources.merge(
            flashes[["flash_id", "cell_uid", "time"]], on="flash_id", how="inner"
        )

        minutes_df = aggregate_minutes(flashes, sources_attr)
        n_minutes_per_scan = Counter(pd.Timestamp(m.target_scan_time) for m in minute_masks)
        scan_df = aggregate_scans(minutes_df, n_minutes_per_scan)
        return minutes_df[_MINUTES_COLUMNS], scan_df[_SCAN_COLUMNS]

    @staticmethod
    def _empty_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
        return (
            pd.DataFrame(columns=_MINUTES_COLUMNS),
            pd.DataFrame(columns=_SCAN_COLUMNS),
        )
