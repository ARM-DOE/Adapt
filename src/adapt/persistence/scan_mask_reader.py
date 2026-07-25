# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Read cell-mask geometry from the analysis NetCDFs of a repository.

Post-process modules cannot import persistence (architectural boundary), so the
PostProcessor reads the masks here and injects them. ``read_minute_masks``
yields one record per whole minute covered by the run: the projection module's
``registration_minutes`` frames (previous-scan label space, resolved by the
``registration_cell_uid`` LUT) plus, where a scan time falls exactly on the
minute grid, the real segmentation (fraction 0.0, the scan's own LUT) replacing
the advected frame. All artifact-selection logic lives here so association
modules never decide which geometry represents a cell at a given time.
"""

import logging

import pandas as pd

from adapt.persistence.repository import DataRepository, ProductType

logger = logging.getLogger(__name__)


def read_minute_masks(repository: DataRepository) -> list[dict]:
    """Return one mask record per whole minute covered by the run, ascending.

    Each record: ``{minute_time, cell_labels, x, y, cell_uid_lut,
    source_scan_time, target_scan_time, interpolation_fraction}``.

    Advected frames whose NetCDF lacks ``registration_cell_uid`` (the first
    scan pair of a run — its previous scan was never tracked) are skipped with
    a log message; real segmentation records replace advected frames at scan
    minutes. Raises if an analysis file lacks the ``cell_uid`` lookup.
    """
    by_minute: dict[pd.Timestamp, dict] = {}
    for artifact in repository.query(product_type=ProductType.ANALYSIS_NC):
        ds = repository.open_dataset(artifact["artifact_id"])
        try:
            if "cell_uid" not in ds:
                raise ValueError(
                    f"Analysis artifact {artifact['artifact_id']} has no 'cell_uid' "
                    "lookup; cannot attribute observations without it."
                )
            scan_time = pd.Timestamp(artifact["scan_time"]).tz_localize(None)
            x = ds["x"].values
            y = ds["y"].values

            if "registration_minutes" in ds:
                source = pd.Timestamp(ds.attrs["registration_source_scan_time"])
                target = pd.Timestamp(ds.attrs["registration_target_scan_time"])
                if "registration_cell_uid" in ds:
                    reg_lut = ds["registration_cell_uid"].values.astype(str)
                    frames = ds["registration_minutes"].values
                    minutes = pd.to_datetime(ds["minute"].values)
                    fractions = ds["interpolation_fraction"].values
                    for i, minute in enumerate(minutes):
                        by_minute[minute] = {
                            "minute_time": minute,
                            "cell_labels": frames[i],
                            "x": x,
                            "y": y,
                            "cell_uid_lut": reg_lut,
                            "source_scan_time": source,
                            "target_scan_time": target,
                            "interpolation_fraction": float(fractions[i]),
                        }
                else:
                    logger.info(
                        "Skipping %d advected minutes of %s: no registration_cell_uid "
                        "(first scan pair of the run has no previous tracking)",
                        int(ds["registration_minutes"].sizes["minute"]),
                        artifact["artifact_id"],
                    )

            # Real segmentation wins at scan times that land on the minute grid.
            if scan_time == scan_time.floor("min"):
                by_minute[scan_time] = {
                    "minute_time": scan_time,
                    "cell_labels": ds["cell_labels"].values,
                    "x": x,
                    "y": y,
                    "cell_uid_lut": ds["cell_uid"].values.astype(str),
                    "source_scan_time": scan_time,
                    "target_scan_time": scan_time,
                    "interpolation_fraction": 0.0,
                }
        finally:
            ds.close()

    return [by_minute[m] for m in sorted(by_minute)]


def read_projection_minute_masks(repository: DataRepository) -> list[dict]:
    """Return one forward-projection mask record per future minute, ascending.

    Each record: ``{minute_time, cell_labels, x, y, cell_uid_lut,
    source_scan_time, projection_fraction}``. ``cell_labels`` are the source
    scan's own labels advected forward (current-scan label space), so they are
    resolved by that scan's ``cell_uid`` LUT — no registration LUT is needed.

    Consecutive scans forecast the same future minute; the most recent source
    scan wins (shortest lead time is the best nowcast). Raises if a file carries
    ``projection_minutes`` but lacks the ``cell_uid`` lookup.
    """
    by_minute: dict[pd.Timestamp, dict] = {}
    for artifact in repository.query(product_type=ProductType.ANALYSIS_NC):
        ds = repository.open_dataset(artifact["artifact_id"])
        try:
            if "projection_minutes" not in ds:
                continue
            if "cell_uid" not in ds:
                raise ValueError(
                    f"Analysis artifact {artifact['artifact_id']} has 'projection_minutes' "
                    "but no 'cell_uid' lookup; cannot attribute forward projections without it."
                )
            source = pd.Timestamp(ds.attrs["registration_target_scan_time"])
            lut = ds["cell_uid"].values.astype(str)
            frames = ds["projection_minutes"].values
            minutes = pd.to_datetime(ds["projection_minute"].values)
            fractions = ds["projection_fraction"].values
            x = ds["x"].values
            y = ds["y"].values
            for i, minute in enumerate(minutes):
                incumbent = by_minute.get(minute)
                if incumbent is None or source > incumbent["source_scan_time"]:
                    by_minute[minute] = {
                        "minute_time": minute,
                        "cell_labels": frames[i],
                        "x": x,
                        "y": y,
                        "cell_uid_lut": lut,
                        "source_scan_time": source,
                        "projection_fraction": float(fractions[i]),
                    }
        finally:
            ds.close()

    return [by_minute[m] for m in sorted(by_minute)]
