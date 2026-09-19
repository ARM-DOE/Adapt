"""Tests for pipeline contracts.

These tests verify that contracts are enforced at stage boundaries.
They test contract violations directly, without defensive logic downstream.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

pytestmark = pytest.mark.unit

from adapt.contracts import (  # noqa: E402
    ContractViolation,
    assert_analysis_output,
    assert_gridded,
    assert_projected,
    assert_segmented,
)


class TestGridContract:
    """Test grid stage contract."""

    def test_grid_contract_passes_with_valid_dataset(self):
        """Grid contract passes when x, y, and reflectivity exist."""
        ds = xr.Dataset(
            {"reflectivity": (("y", "x"), np.ones((4, 4)))},
            coords={"x": range(4), "y": range(4)},
        )
        # Should not raise
        assert_gridded(ds, "reflectivity")

    def test_grid_contract_fails_without_x(self):
        """Grid contract fails when x coordinate is missing."""
        ds = xr.Dataset(
            {"reflectivity": (("y", "x"), np.ones((4, 4)))},
            coords={"y": range(4)},
        )
        with pytest.raises(ContractViolation, match="missing 'x'"):
            assert_gridded(ds, "reflectivity")

    def test_grid_contract_fails_without_y(self):
        """Grid contract fails when y coordinate is missing."""
        ds = xr.Dataset(
            {"reflectivity": (("y", "x"), np.ones((4, 4)))},
            coords={"x": range(4)},
        )
        with pytest.raises(ContractViolation, match="missing 'y'"):
            assert_gridded(ds, "reflectivity")

    def test_grid_contract_fails_without_reflectivity(self):
        """Grid contract fails when reflectivity variable is missing."""
        ds = xr.Dataset(
            coords={"x": range(4), "y": range(4)},
        )
        with pytest.raises(ContractViolation, match="missing 'reflectivity'"):
            assert_gridded(ds, "reflectivity")

    def test_grid_contract_fails_with_wrong_dims(self):
        """Grid contract fails when reflectivity is 3D instead of 2D."""
        ds = xr.Dataset(
            {"reflectivity": (("z", "y", "x"), np.ones((2, 4, 4)))},
            coords={"x": range(4), "y": range(4), "z": range(2)},
        )
        with pytest.raises(ContractViolation, match="3 dims"):
            assert_gridded(ds, "reflectivity")


class TestSegmentationContract:
    """Test segmentation stage contract."""

    def test_segmentation_contract_passes_with_valid_labels(self):
        """Segmentation contract passes with integer labels."""
        labels = np.array([[0, 0, 1, 1], [0, 1, 1, 0]], dtype=np.int32)
        ds = xr.Dataset(
            {"cell_labels": (("y", "x"), labels)},
            coords={"x": range(4), "y": range(2)},
        )
        # Should not raise
        assert_segmented(ds, "cell_labels")

    def test_segmentation_contract_fails_without_labels(self):
        """Segmentation contract fails when labels variable is missing."""
        ds = xr.Dataset(coords={"x": range(4), "y": range(2)})
        with pytest.raises(ContractViolation, match="not found"):
            assert_segmented(ds, "cell_labels")

    def test_segmentation_contract_fails_with_float_labels(self):
        """Segmentation contract fails when labels are float instead of integer."""
        labels = np.array([[0, 0, 1, 1], [0, 1, 1, 0]], dtype=np.float32)
        ds = xr.Dataset(
            {"cell_labels": (("y", "x"), labels)},
            coords={"x": range(4), "y": range(2)},
        )
        with pytest.raises(ContractViolation, match="dtype"):
            assert_segmented(ds, "cell_labels")

    def test_segmentation_contract_fails_with_negative_labels(self):
        """Segmentation contract fails when labels contain negative values."""
        labels = np.array([[0, -1, 1, 1], [0, 1, 1, 0]], dtype=np.int32)
        ds = xr.Dataset(
            {"cell_labels": (("y", "x"), labels)},
            coords={"x": range(4), "y": range(2)},
        )
        with pytest.raises(ContractViolation, match="negative"):
            assert_segmented(ds, "cell_labels")

    def test_segmentation_contract_fails_with_wrong_dims(self):
        """Segmentation contract fails when labels are 3D instead of 2D."""
        labels = np.array([[[0, 1], [1, 0]]], dtype=np.int32)
        ds = xr.Dataset(
            {"cell_labels": (("z", "y", "x"), labels)},
            coords={"x": range(2), "y": range(2), "z": range(1)},
        )
        with pytest.raises(ContractViolation, match="3 dims"):
            assert_segmented(ds, "cell_labels")


class TestProjectionContract:
    """Test projection stage contract."""

    def test_projection_contract_passes_with_valid_flow(self):
        """Projection contract passes when flow fields exist."""
        ds = xr.Dataset(
            {
                "heading_x": (("y", "x"), np.ones((4, 4))),
                "heading_y": (("y", "x"), np.zeros((4, 4))),
            },
            coords={"x": range(4), "y": range(4)},
        )
        # Should not raise
        assert_projected(ds, max_steps=5)

    def test_projection_contract_fails_without_flow_u(self):
        """Projection contract fails when heading_x is missing."""
        ds = xr.Dataset(
            {"heading_y": (("y", "x"), np.zeros((4, 4)))},
            coords={"x": range(4), "y": range(4)},
        )
        with pytest.raises(ContractViolation, match="heading_x"):
            assert_projected(ds)

    def test_projection_contract_fails_without_flow_v(self):
        """Projection contract fails when heading_y is missing."""
        ds = xr.Dataset(
            {"heading_x": (("y", "x"), np.ones((4, 4)))},
            coords={"x": range(4), "y": range(4)},
        )
        with pytest.raises(ContractViolation, match="heading_y"):
            assert_projected(ds)

    def test_projection_contract_fails_with_too_many_steps(self):
        """Projection contract fails when projections exceed max_steps."""
        projections = np.zeros((7, 4, 4), dtype=np.int32)  # 7 steps > 5 max
        ds = xr.Dataset(
            {
                "heading_x": (("y", "x"), np.ones((4, 4))),
                "heading_y": (("y", "x"), np.zeros((4, 4))),
                "cell_projections": (("frame_offset", "y", "x"), projections),
            },
            coords={"x": range(4), "y": range(4)},
        )
        with pytest.raises(ContractViolation, match="expected 6"):
            assert_projected(ds, max_steps=5)


class TestAnalysisContract:
    """Test analysis stage contract."""

    def test_analysis_contract_passes_with_valid_dataframe(self):
        """Analysis contract passes with valid output DataFrame."""
        df = pd.DataFrame(
            {
                "cell_label": [1, 2],
                "cell_area_sqkm": [1.5, 2.5],
                "time": pd.to_datetime(["2025-01-01", "2025-01-01"]),
                "time_volume_start": [
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-01T00:00:00+00:00",
                ],
                "cell_centroid_mass_lat": [35.0, 35.1],
                "cell_centroid_mass_lon": [-97.0, -97.1],
                "radar_reflectivity_max": [45.0, 50.0],
                "radar_differential_reflectivity_max": [1.0, 1.5],
                "area_40dbz_km2": [1.0, 2.0],
            }
        )
        # Should not raise
        assert_analysis_output(df)

    def test_analysis_contract_passes_with_empty_dataframe(self):
        """Analysis contract passes with empty DataFrame (no cells detected)."""
        df = pd.DataFrame(
            {
                "cell_label": [],
                "cell_area_sqkm": [],
                "time": [],
                "time_volume_start": [],
                "cell_centroid_mass_lat": [],
                "cell_centroid_mass_lon": [],
                "radar_reflectivity_max": [],
                "radar_differential_reflectivity_max": [],
                "area_40dbz_km2": [],
            }
        )
        # Should not raise
        assert_analysis_output(df)

    def test_analysis_contract_fails_with_missing_cell_label(self):
        """Analysis contract fails when cell_label column is missing."""
        df = pd.DataFrame(
            {
                "cell_area_sqkm": [1.5],
                "time": pd.to_datetime(["2025-01-01"]),
            }
        )
        with pytest.raises(ContractViolation, match="cell_label"):
            assert_analysis_output(df)

    def test_analysis_contract_fails_with_zero_cell_label(self):
        """Analysis contract fails when cell_label is 0 or negative."""
        df = pd.DataFrame(
            {
                "cell_label": [0, 1],
                "cell_area_sqkm": [1.5, 2.5],
                "time": pd.to_datetime(["2025-01-01", "2025-01-01"]),
                "time_volume_start": [
                    "2025-01-01T00:00:00+00:00",
                    "2025-01-01T00:00:00+00:00",
                ],
                "cell_centroid_mass_lat": [35.0, 35.1],
                "cell_centroid_mass_lon": [-97.0, -97.1],
                "radar_reflectivity_max": [45.0, 50.0],
                "radar_differential_reflectivity_max": [1.0, 1.5],
                "area_40dbz_km2": [1.0, 2.0],
            }
        )
        with pytest.raises(ContractViolation, match="cell_label must be > 0"):
            assert_analysis_output(df)

    def test_analysis_contract_fails_with_insufficient_rows(self):
        """Analysis contract fails when row count below minimum."""
        df = pd.DataFrame(
            {
                "cell_label": [1],
                "cell_area_sqkm": [1.5],
                "time": pd.to_datetime(["2025-01-01"]),
                "time_volume_start": ["2025-01-01T00:00:00+00:00"],
                "cell_centroid_mass_lat": [35.0],
                "cell_centroid_mass_lon": [-97.0],
                "radar_reflectivity_max": [45.0],
                "radar_differential_reflectivity_max": [1.0],
                "area_40dbz_km2": [1.0],
            }
        )
        with pytest.raises(ContractViolation, match="expected >= 5"):
            assert_analysis_output(df, min_expected_rows=5)


class TestContractViolationException:
    """Test ContractViolation exception type and semantics."""

    def test_contract_violation_is_runtime_error(self):
        """ContractViolation is a RuntimeError subclass."""
        assert issubclass(ContractViolation, RuntimeError)

    def test_contract_violation_has_clear_message(self):
        """ContractViolation carries clear error message."""
        with pytest.raises(ContractViolation) as exc_info:
            ds = xr.Dataset()
            assert_segmented(ds, "cell_labels")

        msg = str(exc_info.value)
        assert "contract violated" in msg.lower()
        assert "cell_labels" in msg


def test_stats_without_field_specific_columns_pass_structural_contract():
    # ZDR/reflectivity stat columns are field- and source-dependent; the
    # analysis contract validates structure/identity only. Non-dual-pol
    # sources and non-reflectivity tracking fields must pass.
    df = pd.DataFrame(
        {
            "cell_label": [1],
            "cell_area_sqkm": [12.5],
            "time": [pd.Timestamp("2026-08-13T17:31:58Z")],
            "time_volume_start": [pd.Timestamp("2026-08-13T17:31:58Z")],
            "cell_centroid_mass_lat": [41.0],
            "cell_centroid_mass_lon": [-88.0],
        }
    )
    assert_analysis_output(df)  # must NOT raise
