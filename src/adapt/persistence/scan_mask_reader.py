# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Read per-scan cell masks from the analysis NetCDFs of a repository.

Post-process modules cannot import persistence (architectural boundary), so the
PostProcessor reads the masks here and injects them. Each record carries the
segmentation labels, the projected grid coordinates (metres), the label→cell_uid
lookup written alongside the segmentation, and the scan time.
"""

import pandas as pd

from adapt.persistence.repository import DataRepository, ProductType


def read_scan_masks(repository: DataRepository) -> list[dict]:
    """Return one mask record per analysis scan, newest-to-oldest as queried.

    Each record: ``{scan_time, cell_labels, x, y, cell_uid_lut}``. Raises if an
    analysis file lacks the ``cell_uid`` lookup needed for attribution.
    """
    masks: list[dict] = []
    for artifact in repository.query(product_type=ProductType.ANALYSIS_NC):
        ds = repository.open_dataset(artifact["artifact_id"])
        try:
            if "cell_uid" not in ds:
                raise ValueError(
                    f"Analysis artifact {artifact['artifact_id']} has no 'cell_uid' "
                    "lookup; cannot attribute lightning without it."
                )
            masks.append(
                {
                    "scan_time": pd.Timestamp(artifact["scan_time"]),
                    "cell_labels": ds["cell_labels"].values,
                    "x": ds["x"].values,
                    "y": ds["y"].values,
                    "cell_uid_lut": ds["cell_uid"].values.astype(str),
                }
            )
        finally:
            ds.close()
    return masks
