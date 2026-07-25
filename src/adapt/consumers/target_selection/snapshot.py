# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Frozen input data model for the engine.

The engine sees only these types — never pandas or RepositoryClient.
Conversion from repository tables happens once, in repository_source.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TrajectoryPoint:
    """One projected future position of a cell."""

    lat: float
    lon: float
    lead_seconds: float


@dataclass(frozen=True)
class CellSnapshot:
    """State of one tracked cell at the snapshot's scan time.

    values holds every numeric column of the merged cells_by_scan +
    cell_tracks row, keyed by column name — the pluggable quality
    gate reads from it.
    """

    uid: str
    lat: float
    lon: float
    area_sqkm: float
    reflectivity_max: float
    age_seconds: float
    growth_rate_sqkm_per_min: float
    trajectory: tuple[TrajectoryPoint, ...]
    values: Mapping[str, float]


@dataclass(frozen=True)
class Snapshot:
    """All tracked cells present at one scan time."""

    scan_time: datetime
    cells: tuple[CellSnapshot, ...]
