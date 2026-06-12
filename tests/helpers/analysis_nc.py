# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Synthetic analysis-NetCDF builder mirroring the processor's persisted output."""

import numpy as np
import pandas as pd
import xarray as xr

GRID = np.linspace(-10_000.0, 10_000.0, 21)  # metres, radar at centre


def cell_block(col: int, label: int = 1, size: int = 3) -> np.ndarray:
    """21x21 labels with one size×size cell whose left edge is at column `col`."""
    labels = np.zeros((21, 21), dtype=np.int32)
    labels[9 : 9 + size, col : col + size] = label
    return labels


def make_analysis_ds(
    scan_time: str,
    prev_scan_time: str | None,
    cell_labels: np.ndarray,
    cell_uids: list[str],
    minute_labels: dict[str, np.ndarray] | None = None,
    registration_uids: list[str] | None = None,
) -> xr.Dataset:
    """Analysis dataset as the processor persists it.

    ``minute_labels`` maps ISO minute → 2D prev-label mask; ``registration_uids``
    is the prev-scan label→uid LUT (index 1..n). Omit it to simulate the first
    scan pair of a run (no previous tracking).
    """
    ds = xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.full((21, 21), 30.0, dtype=np.float32)),
            "cell_labels": (("y", "x"), cell_labels),
            "cell_uid": ("cell_label", np.array(["NONE", *cell_uids], dtype=np.str_)),
        },
        coords={"x": GRID, "y": GRID, "cell_label": np.arange(len(cell_uids) + 1)},
    )
    if minute_labels is not None:
        minutes = np.array(sorted(minute_labels), dtype="datetime64[ns]")
        t_prev = pd.Timestamp(prev_scan_time)
        t_curr = pd.Timestamp(scan_time)
        fractions = ((pd.DatetimeIndex(minutes) - t_prev) / (t_curr - t_prev)).to_numpy()
        ds["registration_minutes"] = xr.DataArray(
            np.stack([minute_labels[m] for m in sorted(minute_labels)]),
            dims=("minute", "y", "x"),
            coords={"minute": minutes, "y": GRID, "x": GRID},
        )
        ds = ds.assign_coords(interpolation_fraction=("minute", fractions.astype(np.float32)))
        ds.attrs["registration_source_scan_time"] = t_prev.isoformat()
        ds.attrs["registration_target_scan_time"] = t_curr.isoformat()
    if registration_uids is not None:
        ds["registration_cell_uid"] = xr.DataArray(
            np.array(["NONE", *registration_uids], dtype=np.str_),
            dims=("registration_cell_label",),
            coords={"registration_cell_label": np.arange(len(registration_uids) + 1)},
        )
    return ds
