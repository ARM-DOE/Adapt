# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Output contracts for the LMA post-process module.

Validates the two extension-table frames the module returns. Empty frames are
tolerated: a repository / time window with no lightning legitimately produces no
rows.
"""

import pandas as pd

from adapt.contracts.pipeline import require

_CELL_STATS_REQUIRED = (
    "cell_uid",
    "time_bin",
    "flash_count",
    "source_count",
    "flash_rate_per_min",
)

_FLASH_ATTR_REQUIRED = (
    "flash_id",
    "cell_uid",
    "time_bin",
    "attribution_dist_m",
)


def assert_lma_cell_stats(df) -> None:
    require(isinstance(df, pd.DataFrame), "lma_cell_stats output must be a DataFrame")
    if df.empty:
        return
    for col in _CELL_STATS_REQUIRED:
        require(col in df.columns, f"lma_cell_stats missing column '{col}'")


def assert_lma_flash_attribution(df) -> None:
    require(isinstance(df, pd.DataFrame), "lma_flash_attribution output must be a DataFrame")
    if df.empty:
        return
    for col in _FLASH_ATTR_REQUIRED:
        require(col in df.columns, f"lma_flash_attribution missing column '{col}'")


def check_lma_cell_stats(df) -> None:
    assert_lma_cell_stats(df)


def check_lma_flash_attribution(df) -> None:
    assert_lma_flash_attribution(df)
