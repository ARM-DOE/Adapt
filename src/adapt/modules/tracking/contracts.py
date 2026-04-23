"""Contracts for the tracking module outputs.

Tracking emits scan-local outputs for persistence and downstream consumers:
- tracked_cells: one row per cell observation assigned to a track in the current scan
- track_events: one row per lineage/event edge (continue/split/merge/initiation/termination)
- track_adjacency: one row per adjacency pair in track identity space

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
        "track_index",
        "cell_uid",
        "area",
        "centroid_x",
        "centroid_y",
        "mean_reflectivity",
        "max_reflectivity",
        "core_area",
        "n_adjacent_tracks",
        "adjacent_cell_uids_json",
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
        (df["track_index"] >= 1).all(),
        "Tracked cells contract violated: track_index must be >= 1 for all rows",
    )
    require(
        "cell_uid" in df.columns and df["cell_uid"].notna().all(),
        "Tracked cells contract violated: cell_uid must be non-null for all rows",
    )


def assert_track_events(df: pd.DataFrame) -> None:
    require(
        isinstance(df, pd.DataFrame),
        f"Track events contract violated: output is {type(df)}, expected DataFrame",
    )

    required_cols = [
        "time",
        "event_type",
        "source_track_index",
        "target_track_index",
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
            f"Track events contract violated: missing required column '{col}'",
        )

    if len(df) == 0:
        return

    valid = {"CONTINUE", "SPLIT", "MERGE", "INITIATION", "TERMINATION"}
    require(
        df["event_type"].isin(valid).all(),
        f"Track events contract violated: invalid event_type present (valid={sorted(valid)})",
    )


def assert_track_adjacency(df: pd.DataFrame) -> None:
    require(
        isinstance(df, pd.DataFrame),
        f"Track adjacency contract violated: output is {type(df)}, expected DataFrame",
    )

    required_cols = [
        "time",
        "track_index_a",
        "track_index_b",
        "cell_uid_a",
        "cell_uid_b",
        "touching_boundary_pixels",
    ]

    for col in required_cols:
        require(
            col in df.columns,
            f"Track adjacency contract violated: missing required column '{col}'",
        )

    if len(df) == 0:
        return

    require(
        (df["track_index_a"] >= 1).all() and (df["track_index_b"] >= 1).all(),
        "Track adjacency contract violated: track indices must be >= 1",
    )
    require(
        (df["track_index_a"] < df["track_index_b"]).all(),
        "Track adjacency contract violated: expected canonical ordering track_index_a < track_index_b",
    )
    require(
        (df["touching_boundary_pixels"] >= 1).all(),
        "Track adjacency contract violated: touching_boundary_pixels must be >= 1",
    )
