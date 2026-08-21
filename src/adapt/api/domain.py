# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""First-class domain objects for the Adapt store API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
import xarray as xr

__all__ = ["Collection", "Run", "Scan", "ScanBundle", "ScanRaster", "ScanRef", "Track"]


@dataclass(frozen=True)
class Collection:
    """A radar/site data domain; many runs, one catalog + products + objects."""

    collection_id: str
    source_kind: str
    location_lat: float | None
    location_lon: float | None


@dataclass(frozen=True)
class Run:
    """A single pipeline execution (the ONE run lifecycle, from the registry)."""

    run_id: str
    collection_id: str
    status: str  # 'running' | 'completed' | 'cancelled' | 'failed'
    started_at: datetime
    ended_at: datetime | None
    config_hash: str
    pipeline_version: str


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
    # Max of the run's TRACKED field over the cell's life (see
    # StoreClient.run_tracking_field) — dBZ only when that field is
    # reflectivity. Column name kept for store compatibility.
    max_reflectivity_dbz: float


@dataclass(frozen=True)
class Scan:
    """Metadata for one processed scan.

    ``scan_id`` is the identity (join key); times are ordering/display metadata.
    Coverage times are per-source and may be absent.
    """

    scan_id: str
    run_id: str
    collection_id: str
    scan_time: datetime
    source_file_name: str
    status: str
    start_time: datetime | None = None
    end_time: datetime | None = None


@dataclass(frozen=True)
class ScanRef:
    """One position on a run's scan timeline — identity plus display time."""

    run_id: str
    scan_id: str
    scan_time: datetime


class ScanRaster:
    """A scan's raster product, fully loaded in memory — no file handle retained.

    Closeable context manager: ``close()`` releases the dataset reference; the
    underlying NetCDF file was already closed before construction, so a leaked
    ScanRaster costs memory, never file descriptors.
    """

    def __init__(self, dataset: xr.Dataset, ref: ScanRef, product: str) -> None:
        self.dataset = dataset
        self.ref = ref
        self.product = product

    def __enter__(self) -> ScanRaster:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.dataset.close()


@dataclass
class ScanBundle:
    """All data products for a single scan, loaded together."""

    scan: Scan
    segmentation: xr.Dataset | None
    cells: pd.DataFrame | None
    tracks: list[Track] = field(default_factory=list)
