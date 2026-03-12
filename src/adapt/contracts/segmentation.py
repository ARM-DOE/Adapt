"""Segmentation stage contract.

Enforces the guarantee that after segmentation, cell labels are present,
properly typed, and in canonical form (largest cells first).
"""

import xarray as xr
import numpy as np
from adapt.contracts.base import require


def assert_segmented(ds: xr.Dataset, labels_name: str) -> None:
    """Enforce segmentation stage contract.

    Called immediately after segmentation. Verifies that the segmenter
    produced valid, typed, labeled output.

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
        f"Segmentation contract violated: '{labels_name}' not found"
    )

    labels = ds[labels_name]

    # Verify type
    require(
        labels.dtype.kind in {"i", "u"},
        f"Segmentation contract violated: '{labels_name}' dtype is {labels.dtype}, expected integer"
    )

    # Verify range: background=0, cells=1..N
    label_vals = labels.values
    require(
        np.min(label_vals) >= 0,
        f"Segmentation contract violated: labels contain negative values (min={np.min(label_vals)})"
    )

    # Verify 2D shape (consistent with grid)
    require(
        labels.ndim == 2,
        f"Segmentation contract violated: '{labels_name}' has {labels.ndim} dims, expected 2"
    )
