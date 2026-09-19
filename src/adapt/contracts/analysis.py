# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Analysis stage contracts.

Enforces structural requirements on cell statistics and adjacency DataFrames.
Scientific correctness of the values is the module's responsibility, not checked here.
"""

import pandas as pd

from adapt.contracts.pipeline import require

_REQUIRED_STATS_COLS = [
    "cell_label",
    "cell_area_sqkm",
    "time",
    "time_volume_start",
    "cell_centroid_mass_lat",
    "cell_centroid_mass_lon",
]

_REQUIRED_ADJACENCY_COLS = [
    "time",
    "cell_label_a",
    "cell_label_b",
    "touching_boundary_pixels",
]


def assert_analysis_output(df: pd.DataFrame, min_expected_rows: int = 0) -> None:
    """Enforce analysis stage contract.

    Parameters
    ----------
    df : pd.DataFrame
        Output from analyzer.extract()
    min_expected_rows : int, optional
        Minimum number of rows expected (default 0, allows no-cell frames)

    Raises
    ------
    ContractViolation
        If structural requirements are violated
    """
    require(
        isinstance(df, pd.DataFrame),
        f"Analysis contract violated: output is {type(df)}, expected DataFrame",
    )
    for col in _REQUIRED_STATS_COLS:
        require(
            col in df.columns,
            f"Analysis contract violated: missing required column '{col}'",
        )
    if len(df) > 0:
        require(
            (df["cell_label"] > 0).all(),
            "Analysis contract violated: cell_label must be > 0 for all rows",
        )
    require(
        len(df) >= min_expected_rows,
        f"Analysis contract violated: got {len(df)} cells, expected >= {min_expected_rows}",
    )


def assert_cell_adjacency(df: pd.DataFrame) -> None:
    """Enforce cell adjacency contract.

    Raises
    ------
    ContractViolation
        If structural requirements are violated
    """
    require(
        isinstance(df, pd.DataFrame),
        f"Cell adjacency contract violated: output is {type(df)}, expected DataFrame",
    )
    for col in _REQUIRED_ADJACENCY_COLS:
        require(
            col in df.columns,
            f"Cell adjacency contract violated: missing required column '{col}'",
        )
    if len(df) == 0:
        return
    require(
        (df["cell_label_a"] > 0).all() and (df["cell_label_b"] > 0).all(),
        "Cell adjacency contract violated: cell labels must be > 0",
    )
    require(
        (df["cell_label_a"] < df["cell_label_b"]).all(),
        "Cell adjacency contract violated: expected canonical ordering cell_label_a < cell_label_b",
    )
    require(
        (df["touching_boundary_pixels"] >= 1).all(),
        "Cell adjacency contract violated: touching_boundary_pixels must be >= 1",
    )


def check_cell_stats(df: pd.DataFrame) -> None:
    """Bound contract for cell statistics DataFrame."""
    assert_analysis_output(df)


def check_cell_adjacency(df: pd.DataFrame) -> None:
    """Bound contract for cell adjacency DataFrame."""
    assert_cell_adjacency(df)
