# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Accept/reject tests for the xlma_stat extension-table contracts."""

import pandas as pd
import pytest

from adapt.contracts import (
    ContractViolation,
    check_xlma_stat_minutes,
    check_xlma_stat_scan,
)

pytestmark = pytest.mark.unit

# Pinned here, not imported from the contract: these lists ARE the contract the
# tests enforce. Provenance columns record which scan's mask each cell_uid came
# from and how far between scans it was interpolated.
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


def _minutes_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cell_uid": ["abc123"],
            "time": ["2024-01-01T00:01:00Z"],
            "source_scan_time": ["2024-01-01T00:00:00Z"],
            "target_scan_time": ["2024-01-01T00:03:00Z"],
            "interpolation_fraction": [1 / 3],
            "flash_count": [3],
            "lightning_source_count": [120],
            "flash_rate_per_min": [3.0],
        }
    )


def _scan_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cell_uid": ["abc123"],
            "scan_time": ["2024-01-01T00:03:00Z"],
            "n_minutes": [3],
            "n_lightning_minutes": [1],
            "flash_count": [3],
            "lightning_source_count": [120],
        }
    )


class TestXlmaStatMinutesContract:
    def test_passes_on_valid_df(self):
        check_xlma_stat_minutes(_minutes_df())

    def test_passes_on_empty_df(self):
        check_xlma_stat_minutes(pd.DataFrame())

    @pytest.mark.parametrize("col", _MINUTES_REQUIRED)
    def test_fails_on_missing_column(self, col):
        with pytest.raises(ContractViolation, match=col):
            check_xlma_stat_minutes(_minutes_df().drop(columns=[col]))

    def test_fails_on_non_dataframe(self):
        with pytest.raises(ContractViolation, match="DataFrame"):
            check_xlma_stat_minutes({"cell_uid": ["abc123"]})


class TestXlmaStatScanContract:
    def test_passes_on_valid_df(self):
        check_xlma_stat_scan(_scan_df())

    def test_passes_on_empty_df(self):
        check_xlma_stat_scan(pd.DataFrame())

    @pytest.mark.parametrize("col", _SCAN_REQUIRED)
    def test_fails_on_missing_column(self, col):
        with pytest.raises(ContractViolation, match=col):
            check_xlma_stat_scan(_scan_df().drop(columns=[col]))

    def test_fails_on_non_dataframe(self):
        with pytest.raises(ContractViolation, match="DataFrame"):
            check_xlma_stat_scan([1, 2, 3])
