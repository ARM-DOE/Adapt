# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Output contracts for the xlma_stat post-process module.

Validates the two extension-table frames the module returns. Empty frames are
tolerated: a repository / time window with no lightning legitimately produces no
rows.
"""

import pandas as pd

from adapt.contracts.pipeline import require

_MINUTES_REQUIRED = (
    "cell_uid",
    "time",
    "source_scan_time",
    "target_scan_time",
    "interpolation_fraction",
    "flash_count",
    "lightning_source_count",
    "flash_rate_per_min",
)

_SCAN_REQUIRED = (
    "cell_uid",
    "scan_time",
    "n_minutes",
    "n_lightning_minutes",
    "flash_count",
    "lightning_source_count",
)


def assert_xlma_stat_minutes(df) -> None:
    require(isinstance(df, pd.DataFrame), "xlma_stat_minutes output must be a DataFrame")
    if df.empty:
        return
    for col in _MINUTES_REQUIRED:
        require(col in df.columns, f"xlma_stat_minutes missing column '{col}'")


def assert_xlma_stat_scan(df) -> None:
    require(isinstance(df, pd.DataFrame), "xlma_stat_scan output must be a DataFrame")
    if df.empty:
        return
    for col in _SCAN_REQUIRED:
        require(col in df.columns, f"xlma_stat_scan missing column '{col}'")


def check_xlma_stat_minutes(df) -> None:
    assert_xlma_stat_minutes(df)


def check_xlma_stat_scan(df) -> None:
    assert_xlma_stat_scan(df)
