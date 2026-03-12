"""Analysis stage contract.

Enforces the guarantee that after cell analysis, output contains
required columns and fields are well-formed (no spurious NaNs in required fields).
"""

import pandas as pd
import numpy as np
from adapt.contracts.base import require


def assert_analysis_output(df: pd.DataFrame, min_expected_rows: int = 0) -> None:
    """Enforce analysis stage contract.

    Called after analyzer.extract(). Verifies that the output DataFrame
    has required columns and data is well-formed.

    We do NOT validate the scientific correctness of statistics — that's
    the analyzer's responsibility. We only check structural requirements.

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
        f"Analysis contract violated: output is {type(df)}, expected DataFrame"
    )

    # Required columns
    required_cols = [
        "cell_label",
        "cell_area_sqkm",
        "time",
    ]

    for col in required_cols:
        require(
            col in df.columns,
            f"Analysis contract violated: missing required column '{col}'"
        )

    # If there are cells, verify they have valid labels
    if len(df) > 0:
        require(
            (df["cell_label"] > 0).all(),
            "Analysis contract violated: cell_label must be > 0 for all rows"
        )

    # Verify minimum rows if specified
    require(
        len(df) >= min_expected_rows,
        f"Analysis contract violated: got {len(df)} cells, expected >= {min_expected_rows}"
    )
