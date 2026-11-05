# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for the bound check_* contract wrappers.

The assert_* primitives are tested in test_contracts.py.
These tests verify the check_* wrappers:  pass on valid data, raise on invalid,
and preserve the same ContractViolation semantics.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.contracts import (
    ContractViolation,
    check_cell_adjacency,
    check_cell_events,
    check_cell_stats,
    check_grid_ds_2d,
    check_projected_ds,
    check_segmented_ds,
    check_time_normalized,
    check_tracked_cells,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _valid_grid_ds():
    return xr.Dataset(
        {"reflectivity": (("y", "x"), np.ones((4, 4), dtype=np.float32))},
        coords={"x": range(4), "y": range(4)},
    )


def _valid_segmented_ds():
    labels = np.array([[0, 0, 1, 1], [0, 1, 1, 0]], dtype=np.int32)
    return xr.Dataset(
        {"cell_labels": (("y", "x"), labels)},
        coords={"x": range(4), "y": range(2)},
    )


def _valid_projected_ds():
    return xr.Dataset(
        {
            "heading_x": (("y", "x"), np.ones((4, 4))),
            "heading_y": (("y", "x"), np.zeros((4, 4))),
        },
        coords={"x": range(4), "y": range(4)},
    )


def _valid_cell_stats():
    return pd.DataFrame(
        {
            "cell_label": [1, 2],
            "cell_area_sqkm": [1.5, 2.5],
            "time": pd.to_datetime(["2025-01-01", "2025-01-01"]),
            "time_volume_start": ["2025-01-01T00:00:00+00:00"] * 2,
            "cell_centroid_mass_lat": [35.0, 35.1],
            "cell_centroid_mass_lon": [-97.0, -97.1],
            "radar_reflectivity_max": [45.0, 50.0],
            "radar_differential_reflectivity_max": [1.0, 1.5],
            "area_40dbz_km2": [1.0, 2.0],
        }
    )


def _valid_adjacency():
    return pd.DataFrame(
        {
            "time": pd.to_datetime(["2025-01-01"]),
            "cell_label_a": [1],
            "cell_label_b": [2],
            "touching_boundary_pixels": [5],
        }
    )


def _valid_tracked_cells():
    return pd.DataFrame(
        {
            "time": pd.to_datetime(["2025-01-01"]),
            "cell_label": [1],
            "cell_uid": ["abc-001"],
            "area": [4.0],
            "centroid_x": [2.5],
            "centroid_y": [2.5],
            "mean_reflectivity": [40.0],
            "max_reflectivity": [45.0],
            "core_area": [2.0],
        }
    )


def _valid_cell_events():
    return pd.DataFrame(
        {
            "time": pd.to_datetime(["2025-01-01"]),
            "event_type": ["INITIATION"],
            "source_cell_uid": [None],
            "target_cell_uid": ["abc-001"],
            "source_cell_label": [None],
            "target_cell_label": [1],
            "cost": [0.0],
            "is_dominant": [True],
            "event_group_id": [1],
        }
    )


# ---------------------------------------------------------------------------
# check_grid_ds_2d
# ---------------------------------------------------------------------------


class TestCheckGridDs2d:
    def test_passes_on_valid_ds(self):
        check_grid_ds_2d(_valid_grid_ds())  # must not raise

    def test_fails_on_missing_x(self):
        ds = xr.Dataset(
            {"reflectivity": (("y", "x"), np.ones((4, 4)))},
            coords={"y": range(4)},
        )
        with pytest.raises(ContractViolation):
            check_grid_ds_2d(ds)

    def test_fails_on_missing_reflectivity(self):
        ds = xr.Dataset(coords={"x": range(4), "y": range(4)})
        with pytest.raises(ContractViolation):
            check_grid_ds_2d(ds)


# ---------------------------------------------------------------------------
# check_segmented_ds
# ---------------------------------------------------------------------------


class TestCheckSegmentedDs:
    def test_passes_on_valid_ds(self):
        check_segmented_ds(_valid_segmented_ds())

    def test_fails_on_float_labels(self):
        labels = np.ones((4, 4), dtype=np.float32)
        ds = xr.Dataset(
            {"cell_labels": (("y", "x"), labels)},
            coords={"x": range(4), "y": range(4)},
        )
        with pytest.raises(ContractViolation):
            check_segmented_ds(ds)

    def test_fails_on_missing_labels_var(self):
        ds = xr.Dataset(coords={"x": range(4), "y": range(4)})
        with pytest.raises(ContractViolation):
            check_segmented_ds(ds)


# ---------------------------------------------------------------------------
# check_projected_ds
# ---------------------------------------------------------------------------


class TestCheckProjectedDs:
    def test_passes_on_valid_ds(self):
        check_projected_ds(_valid_projected_ds())

    def test_fails_on_missing_heading_x(self):
        ds = xr.Dataset(
            {"heading_y": (("y", "x"), np.zeros((4, 4)))},
            coords={"x": range(4), "y": range(4)},
        )
        with pytest.raises(ContractViolation):
            check_projected_ds(ds)


# ---------------------------------------------------------------------------
# check_cell_stats
# ---------------------------------------------------------------------------


class TestCheckCellStats:
    def test_passes_on_valid_df(self):
        check_cell_stats(_valid_cell_stats())

    def test_passes_on_empty_df(self):
        empty = pd.DataFrame(
            columns=[
                "cell_label",
                "cell_area_sqkm",
                "time",
                "time_volume_start",
                "cell_centroid_mass_lat",
                "cell_centroid_mass_lon",
                "radar_reflectivity_max",
                "radar_differential_reflectivity_max",
                "area_40dbz_km2",
            ]
        )
        check_cell_stats(empty)

    def test_fails_on_missing_column(self):
        df = _valid_cell_stats().drop(columns=["cell_label"])
        with pytest.raises(ContractViolation):
            check_cell_stats(df)

    def test_fails_on_zero_cell_label(self):
        df = _valid_cell_stats().copy()
        df.loc[0, "cell_label"] = 0
        with pytest.raises(ContractViolation):
            check_cell_stats(df)


# ---------------------------------------------------------------------------
# check_cell_adjacency
# ---------------------------------------------------------------------------


class TestCheckCellAdjacency:
    def test_passes_on_valid_df(self):
        check_cell_adjacency(_valid_adjacency())

    def test_passes_on_empty_df(self):
        empty = pd.DataFrame(
            columns=["time", "cell_label_a", "cell_label_b", "touching_boundary_pixels"]
        )
        check_cell_adjacency(empty)

    def test_fails_on_missing_column(self):
        df = _valid_adjacency().drop(columns=["touching_boundary_pixels"])
        with pytest.raises(ContractViolation):
            check_cell_adjacency(df)

    def test_fails_on_wrong_label_order(self):
        df = _valid_adjacency().copy()
        df["cell_label_a"], df["cell_label_b"] = df["cell_label_b"], df["cell_label_a"]
        with pytest.raises(ContractViolation):
            check_cell_adjacency(df)


# ---------------------------------------------------------------------------
# check_tracked_cells
# ---------------------------------------------------------------------------


class TestCheckTrackedCells:
    def test_passes_on_valid_df(self):
        check_tracked_cells(_valid_tracked_cells())

    def test_passes_on_empty_df(self):
        # check_tracked_cells skips validation on empty frames
        check_tracked_cells(pd.DataFrame())

    def test_fails_on_missing_column(self):
        df = _valid_tracked_cells().drop(columns=["cell_uid"])
        with pytest.raises(ContractViolation):
            check_tracked_cells(df)

    def test_fails_on_zero_cell_label(self):
        df = _valid_tracked_cells().copy()
        df["cell_label"] = 0
        with pytest.raises(ContractViolation):
            check_tracked_cells(df)

    def test_fails_on_null_uid(self):
        df = _valid_tracked_cells().copy()
        df["cell_uid"] = None
        with pytest.raises(ContractViolation):
            check_tracked_cells(df)


# ---------------------------------------------------------------------------
# check_cell_events
# ---------------------------------------------------------------------------


class TestCheckCellEvents:
    def test_passes_on_valid_df(self):
        check_cell_events(_valid_cell_events())

    def test_passes_on_empty_df(self):
        check_cell_events(pd.DataFrame())

    def test_fails_on_missing_column(self):
        df = _valid_cell_events().drop(columns=["event_type"])
        with pytest.raises(ContractViolation):
            check_cell_events(df)

    @pytest.mark.parametrize("event_type", ["LATENT", "RESUMED"])
    def test_latent_track_events_are_valid(self, event_type):
        df = _valid_cell_events().copy()
        df["event_type"] = event_type
        check_cell_events(df)

    def test_fails_on_invalid_event_type(self):
        df = _valid_cell_events().copy()
        df["event_type"] = "UNKNOWN"
        with pytest.raises(ContractViolation):
            check_cell_events(df)


# ---------------------------------------------------------------------------
# check_time_normalized
# ---------------------------------------------------------------------------


class TestCheckTimeNormalized:
    def test_passes_with_numpy_datetime64_coord(self):
        ds = xr.Dataset(coords={"time": np.datetime64("2025-01-01T12:00:00")})
        check_time_normalized(ds)  # must not raise

    def test_passes_with_no_time_coord_when_attr_present(self):
        ds = xr.Dataset(attrs={"time": "2025-01-01"})
        check_time_normalized(ds)

    def test_fails_without_time(self):
        ds = xr.Dataset()
        with pytest.raises(ContractViolation):
            check_time_normalized(ds)

    def test_check_is_same_as_assert(self):
        """check_time_normalized delegates to assert_time_normalized."""
        from adapt.contracts import assert_time_normalized

        ds = xr.Dataset(coords={"time": np.datetime64("2025-01-01T12:00:00")})
        # Both should pass without raising
        assert_time_normalized(ds)
        check_time_normalized(ds)


class TestGridContractIsStructural:
    def test_grid_2d_contract_accepts_any_field_name(self):
        # The bound contract is structural (x/y + non-empty): WHICH field
        # must exist depends on config (global_.tracking_field) and is
        # validated at the detection module boundary, not here.
        ds = xr.Dataset(
            {"pressure": (("y", "x"), np.zeros((4, 4)))},
            coords={"y": np.arange(4.0), "x": np.arange(4.0)},
        )
        check_grid_ds_2d(ds)  # must NOT raise

    def test_grid_2d_contract_still_requires_coords(self):
        ds = xr.Dataset({"pressure": (("a", "b"), np.zeros((4, 4)))})
        with pytest.raises(ContractViolation):
            check_grid_ds_2d(ds)

    def test_grid_2d_contract_rejects_empty_dataset(self):
        ds = xr.Dataset(coords={"y": np.arange(4.0), "x": np.arange(4.0)})
        with pytest.raises(ContractViolation):
            check_grid_ds_2d(ds)
