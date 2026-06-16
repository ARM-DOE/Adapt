# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Label -> global cell_uid lookup tables for the analysis dataset.

Tracking-owned science: the 2D ``cell_labels`` segmentation carries only local
labels, so the analysis dataset gains 1D LUT variables resolving them to the
global uids assigned by tracking. Pure functions — a new dataset is returned;
the input is never mutated.
"""

import numpy as np
import pandas as pd
import xarray as xr

from adapt.utils.cell_uid_lut import build_cell_uid_lut


def attach_cell_uid_lut(ds: xr.Dataset, tracked_cells: pd.DataFrame) -> xr.Dataset:
    """Return ``ds`` plus a 1D ``cell_uid`` variable mapping cell_label -> uid.

    Indexed by ``cell_label`` (0 = background -> "NONE", 1..n = cells), so the
    2D segmentation's local labels can be resolved to global uids from inside
    the analysis NetCDF. Returns ``ds`` unchanged when there are no tracked
    cells.
    """
    if tracked_cells is None or tracked_cells.empty:
        return ds
    ds = ds.copy()
    max_label = int(ds["cell_labels"].max())
    label_to_uid = dict(
        zip(tracked_cells["cell_label"].astype(int), tracked_cells["cell_uid"], strict=True)
    )
    lut = build_cell_uid_lut(label_to_uid, max_label)
    ds["cell_uid"] = xr.DataArray(
        lut,
        dims=["cell_label"],
        coords={"cell_label": np.arange(max_label + 1)},
        attrs={"description": "cell_label index -> global cell_uid; index 0 = NONE (background)"},
    )
    return ds


def attach_registration_uid_lut(
    ds: xr.Dataset, prev_tracked_cells: pd.DataFrame | None
) -> xr.Dataset:
    """Return ``ds`` plus a 1D ``registration_cell_uid`` for previous-scan labels.

    ``registration_minutes`` (and ``cell_projections[0]``) carry the *previous*
    scan's labels, so resolving them to global uids needs the previous scan's
    label -> uid mapping. Returns ``ds`` unchanged when no previous tracking
    exists (first scan pair of a run).
    """
    if prev_tracked_cells is None or prev_tracked_cells.empty:
        return ds
    ds = ds.copy()
    label_to_uid = dict(
        zip(
            prev_tracked_cells["cell_label"].astype(int),
            prev_tracked_cells["cell_uid"],
            strict=True,
        )
    )
    max_label = max(label_to_uid)
    lut = build_cell_uid_lut(label_to_uid, max_label)
    ds["registration_cell_uid"] = xr.DataArray(
        lut,
        dims=["registration_cell_label"],
        coords={"registration_cell_label": np.arange(max_label + 1)},
        attrs={
            "description": (
                "previous-scan cell_label index -> global cell_uid for the "
                "registration_minutes masks; index 0 = NONE (background)"
            )
        },
    )
    return ds
