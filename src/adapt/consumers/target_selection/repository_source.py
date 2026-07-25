# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Boundary between the repository and the engine.

The only module in this package that imports adapt.api. Repository
tables are converted to the frozen Snapshot exactly once, here; the
engine never sees pandas or RepositoryClient.
"""

import re
from collections.abc import Iterable
from datetime import datetime

import numpy as np
import pandas as pd

from adapt.api.client import RepositoryClient
from adapt.consumers.target_selection.snapshot import (
    CellSnapshot,
    Snapshot,
    TrajectoryPoint,
)
from adapt.utils.time import from_scan_iso, to_scan_iso

# Per-track lifecycle columns merged into each cell's `values` mapping
# (cells_by_scan carries none of these).
_TRACK_COLUMNS = ("n_scans", "duration_seconds", "max_area_sqkm", "max_reflectivity")


def build_snapshot(
    client: RepositoryClient,
    run_id: str,
    radar: str,
    *,
    growth_window_scans: int,
    at: datetime | None = None,
) -> Snapshot:
    """Return the latest scan of a run as a frozen Snapshot.

    With ``at``, the run is replayed as of that instant: only scans at or
    before ``at`` are visible (growth rates and scan cadence included).
    """
    history = client.table("cells_by_scan", radar=radar, run_id=run_id)
    if history.empty:
        raise ValueError(f"No cells_by_scan rows for run {run_id!r} (radar {radar!r})")
    if at is not None:
        history = history[history["scan_time"] <= to_scan_iso(at)]
        if history.empty:
            raise ValueError(
                f"No scans at or before {to_scan_iso(at)} in run {run_id!r} (radar {radar!r})"
            )
    tracks = client.tracks(run_id, radar=radar)

    scan_times = sorted(history["scan_time"].unique())
    latest_iso = scan_times[-1]
    scan_interval = _scan_interval_seconds(scan_times)

    latest = history[history["scan_time"] == latest_iso]
    merged = latest.merge(
        tracks[["cell_uid", *_TRACK_COLUMNS]], on="cell_uid", how="left"
    ).sort_values("cell_uid")

    cells = tuple(
        _to_cell(row, history, scan_interval, growth_window_scans) for _, row in merged.iterrows()
    )
    return Snapshot(scan_time=from_scan_iso(latest_iso), cells=cells)


def _to_cell(
    row: pd.Series,
    history: pd.DataFrame,
    scan_interval: float | None,
    growth_window_scans: int,
) -> CellSnapshot:
    uid = row["cell_uid"]
    cell_history = history[history["cell_uid"] == uid].sort_values("scan_time")
    values = {
        col: float(v)
        for col, v in row.items()
        if isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, bool)
    }
    return CellSnapshot(
        uid=uid,
        lat=float(row["cell_centroid_mass_lat"]),
        lon=float(row["cell_centroid_mass_lon"]),
        area_sqkm=float(row["cell_area_sqkm"]),
        reflectivity_max=float(row["radar_reflectivity_max"]),
        age_seconds=float(row["age_seconds"]),
        growth_rate_sqkm_per_min=_growth_rate(cell_history, growth_window_scans),
        trajectory=_trajectory(row, scan_interval),
        values=values,
    )


def _growth_rate(cell_history: pd.DataFrame, window: int) -> float:
    """OLS slope of cell area (km2) vs time (min) over the last `window` scans.

    A cell seen in fewer than two scans has no measurable growth: 0.0.
    """
    rows = cell_history.tail(window)
    if len(rows) < 2:
        return 0.0
    start = from_scan_iso(rows["scan_time"].iloc[0])
    minutes = [(from_scan_iso(s) - start).total_seconds() / 60.0 for s in rows["scan_time"]]
    areas = rows["cell_area_sqkm"].to_numpy(dtype=float)
    return float(np.polyfit(minutes, areas, 1)[0])


_PROJECTION_LAT = re.compile(r"^cell_centroid_projection(\d+)_lat$")


def _projection_steps(columns: Iterable[str]) -> tuple[int, ...]:
    """Forward-projection step indices present in the table, ascending.

    The analysis module writes projection{k} = k scan intervals ahead
    (index 0 is the registration centroid, stored under a different name).
    """
    cols = set(columns)
    return tuple(
        sorted(
            int(m.group(1))
            for m in map(_PROJECTION_LAT.match, cols)
            if m and f"cell_centroid_projection{m.group(1)}_lon" in cols
        )
    )


def _trajectory(row: pd.Series, scan_interval: float | None) -> tuple[TrajectoryPoint, ...]:
    """Projected centroids in ascending step order, stopping at the first null.

    Lead time of step k is k scan intervals. At the first scan of a run
    no cadence exists yet, so lead times are underivable and the
    trajectory is empty — a defined condition, not an error.
    """
    if scan_interval is None:
        return ()
    points = []
    for k in _projection_steps(row.index):
        lat = row[f"cell_centroid_projection{k}_lat"]
        lon = row[f"cell_centroid_projection{k}_lon"]
        if pd.isna(lat) or pd.isna(lon):
            break
        points.append(
            TrajectoryPoint(lat=float(lat), lon=float(lon), lead_seconds=k * scan_interval)
        )
    return tuple(points)


def _scan_interval_seconds(scan_times: list[str]) -> float | None:
    """Seconds between the two most recent distinct scan times, if any."""
    if len(scan_times) < 2:
        return None
    return (from_scan_iso(scan_times[-1]) - from_scan_iso(scan_times[-2])).total_seconds()
