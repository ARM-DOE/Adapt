# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Segmentation stage contract.

Enforces that after cell detection, labels are present, integer-typed,
non-negative, and 2D.
"""

import numpy as np
import xarray as xr

from adapt.contracts.pipeline import require


def assert_segmented(ds: xr.Dataset, labels_name: str) -> None:
    """Enforce segmentation stage contract.

    Parameters
    ----------
    ds : xr.Dataset
        Dataset from segmenter.segment()
    labels_name : str
        Name of cell labels variable (from config)

    Raises
    ------
    ContractViolation
        If any invariant is violated
    """
    require(
        labels_name in ds.data_vars,
        f"Segmentation contract violated: '{labels_name}' not found",
    )
    labels = ds[labels_name]
    require(
        labels.dtype.kind in {"i", "u"},
        f"Segmentation contract violated: '{labels_name}' dtype is {labels.dtype}, "
        "expected integer",
    )
    label_vals = labels.values
    require(
        np.min(label_vals) >= 0,
        "Segmentation contract violated: labels contain negative values "
        f"(min={np.min(label_vals)})",
    )
    require(
        labels.ndim == 2,
        f"Segmentation contract violated: '{labels_name}' has {labels.ndim} dims, expected 2",
    )


def check_segmented_ds(ds: xr.Dataset) -> None:
    """Bound contract for the standard segmented dataset (cell_labels variable name fixed)."""
    assert_segmented(ds, "cell_labels")


def check_seed_carry(ages: tuple) -> None:
    """Carry age per cell label: a tuple of ints >= 0, index i for label i + 1.

    0 means the cell was seeded independently this scan; k means it has been
    carried k consecutive scans from the previous frame's cores.
    """
    require(isinstance(ages, tuple), f"seed_carry must be a tuple, got {type(ages).__name__}")
    for i, age in enumerate(ages):
        require(
            isinstance(age, int) and not isinstance(age, bool),
            f"seed_carry[{i}] must be an int, got {type(age).__name__}",
        )
        require(age >= 0, f"seed_carry[{i}] must be >= 0, got {age}")
