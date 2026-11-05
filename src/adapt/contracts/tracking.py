# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tracking stage contracts.

Enforces structural requirements on tracked_cells and cell_events DataFrames.
"""

from __future__ import annotations

import pandas as pd

from adapt.contracts.pipeline import require

_REQUIRED_TRACKED_COLS = [
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

_REQUIRED_EVENTS_COLS = [
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

# LATENT: a track that vanished is kept for possible resumption; RESUMED: it was.
_VALID_EVENT_TYPES = {
    "CONTINUE",
    "SPLIT",
    "MERGE",
    "INITIATION",
    "TERMINATION",
    "LATENT",
    "RESUMED",
}


def assert_tracked_cells(df: pd.DataFrame) -> None:
    """Enforce tracked cells contract.

    Raises
    ------
    ContractViolation
        If structural requirements are violated
    """
    require(
        isinstance(df, pd.DataFrame),
        f"Tracked cells contract violated: output is {type(df)}, expected DataFrame",
    )
    for col in _REQUIRED_TRACKED_COLS:
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
    """Enforce cell events contract.

    Raises
    ------
    ContractViolation
        If structural requirements are violated
    """
    require(
        isinstance(df, pd.DataFrame),
        f"Cell events contract violated: output is {type(df)}, expected DataFrame",
    )
    for col in _REQUIRED_EVENTS_COLS:
        require(
            col in df.columns,
            f"Cell events contract violated: missing required column '{col}'",
        )
    if len(df) == 0:
        return
    require(
        df["event_type"].isin(_VALID_EVENT_TYPES).all(),
        f"Cell events contract violated: invalid event_type present "
        f"(valid={sorted(_VALID_EVENT_TYPES)})",
    )


def check_tracked_cells(df: pd.DataFrame) -> None:
    """Bound contract for tracked cells DataFrame (skips validation on empty frame)."""
    if not df.empty:
        assert_tracked_cells(df)


def check_cell_events(df: pd.DataFrame) -> None:
    """Bound contract for cell events DataFrame (skips validation on empty frame)."""
    if not df.empty:
        assert_cell_events(df)
