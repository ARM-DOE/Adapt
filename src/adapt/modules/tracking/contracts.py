# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Contracts for the tracking module outputs.

Tracking emits scan-local outputs for persistence and downstream consumers:
- tracked_cells: one row per cell observation assigned to a track in the current scan
- cell_events: one row per lineage/event edge (continue/split/merge/initiation/termination)

A "track" is a single connected chain of cell observations across scans identified by a
stable cell_uid.
"""

from __future__ import annotations

import pandas as pd
from adapt.modules.base import require


def assert_tracked_cells(df: pd.DataFrame) -> None:
    require(
        isinstance(df, pd.DataFrame),
        f"Tracked cells contract violated: output is {type(df)}, expected DataFrame",
    )

    required_cols = [
        "time",
        "cell_label",
        "cell_uid",
        "area",
        "centroid_x",
        "centroid_y",
        "mean_reflectivity",
        "max_reflectivity",
        "core_area",
    ]

    for col in required_cols:
        require(
            col in df.columns,
            f"Tracked cells contract violated: missing required column '{col}'",
        )

    if len(df) == 0:
        return

    require(
        (df["cell_label"] > 0).all(),
        "Tracked cells contract violated: cell_label must be > 0 for all rows",
    )
    require(
        "cell_uid" in df.columns and df["cell_uid"].notna().all(),
        "Tracked cells contract violated: cell_uid must be non-null for all rows",
    )


def assert_cell_events(df: pd.DataFrame) -> None:
    require(
        isinstance(df, pd.DataFrame),
        f"Cell events contract violated: output is {type(df)}, expected DataFrame",
    )

    required_cols = [
        "time",
        "event_type",
        "source_cell_uid",
        "target_cell_uid",
        "source_cell_label",
        "target_cell_label",
        "cost",
        "is_dominant",
        "event_group_id",
    ]

    for col in required_cols:
        require(
            col in df.columns,
            f"Cell events contract violated: missing required column '{col}'",
        )

    if len(df) == 0:
        return

    valid = {"CONTINUE", "SPLIT", "MERGE", "INITIATION", "TERMINATION"}
    require(
        df["event_type"].isin(valid).all(),
        f"Cell events contract violated: invalid event_type present (valid={sorted(valid)})",
    )
