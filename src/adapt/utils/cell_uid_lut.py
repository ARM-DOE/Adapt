# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Build the cell_label -> cell_uid lookup array stored in analysis NetCDF.

Pure function. The 2D ``cell_labels`` segmentation uses 0 for background and
1..n for cells; this produces a 1D array indexed by that label so a pixel's
local ``cell_label`` maps to its global ``cell_uid``.
"""

from collections.abc import Mapping

import numpy as np


def build_cell_uid_lut(
    label_to_uid: Mapping[int, str], max_label: int, null_value: str = "NONE"
) -> np.ndarray:
    """Return a 1D string array of length ``max_label + 1``.

    Index 0 holds ``null_value`` (background); index ``L`` holds the
    ``cell_uid`` of ``cell_label`` ``L``. Every label in ``1..max_label`` must
    be present in ``label_to_uid`` — a gap is a contract violation and raises.
    """
    lut = np.empty(max_label + 1, dtype=object)
    lut[0] = null_value
    for label in range(1, max_label + 1):
        if label not in label_to_uid:
            raise ValueError(f"cell_label {label} has no cell_uid in mapping")
        lut[label] = str(label_to_uid[label])
    return lut.astype(np.str_)
