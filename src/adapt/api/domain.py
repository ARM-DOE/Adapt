# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""First-class domain objects for the Adapt repository API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import xarray as xr

__all__ = ["Run", "Track", "Scan", "ScanBundle"]


@dataclass(frozen=True)
class Run:
    """A single pipeline execution."""

    run_id: str
    radar_id: str
    start_time: datetime
    end_time: datetime | None
    status: str  # 'running' | 'complete' | 'failed'
    mode: str  # 'realtime' | 'historical'


@dataclass(frozen=True)
class Track:
    """Lifecycle summary for one tracked cell (one row from cell_tracks)."""

    run_id: str
    cell_uid: str
    first_seen: datetime
    last_seen: datetime
    n_scans: int
    lifetime_s: float
    origin_type: str  # INITIATION | SPLIT | MERGE | UNKNOWN
    termination_type: str  # TERMINATION | MERGED | ACTIVE_AT_END | UNKNOWN
    max_area_km2: float
    max_reflectivity_dbz: float


@dataclass(frozen=True)
class Scan:
    """Metadata for one processed scan."""

    scan_time: datetime
    radar_id: str
    run_id: str
    n_cells: int
    max_reflectivity: float
    has_tracks: bool


@dataclass
class ScanBundle:
    """All data products for a single scan, loaded together."""

    scan: Scan
    segmentation: xr.Dataset | None
    cells: pd.DataFrame | None
    tracks: list[Track] = field(default_factory=list)
