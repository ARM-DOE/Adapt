# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Edge cases, chaos, and adversarial tests.

Tests in this file probe boundary conditions, extreme inputs, and unusual
but valid combinations.  All use synthetic data — no IO, no real files.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.contracts import ContractViolation

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _refl_ds(data: np.ndarray) -> xr.Dataset:
    H, W = data.shape
    return xr.Dataset(
        {"reflectivity": (("y", "x"), data.astype(np.float32))},
        coords={"y": np.arange(H) * 1000.0, "x": np.arange(W) * 1000.0},
        attrs={"z_level_m": 2000},
    )


# ---------------------------------------------------------------------------
# Detection edge cases
# ---------------------------------------------------------------------------


class TestDetectionEdgeCases:
    def test_single_pixel_cell_filtered_by_min_size(self, make_detection_config):
        """A single-pixel cell below min_cellsize_gridpoint is discarded."""
        from adapt.modules.detection.module import RadarCellSegmenter

        data = np.zeros((8, 8), dtype=np.float32)
        data[4, 4] = 50.0  # one pixel at 50 dBZ
        ds = _refl_ds(data)
        config = make_detection_config(threshold=35, min_cellsize_gridpoint=2)
        result = RadarCellSegmenter(config).segment(ds)
        assert result["cell_labels"].values.max() == 0  # filtered out

    def test_entire_domain_above_threshold_is_one_cell(self, make_detection_config):
        """A single contiguous blob above threshold produces exactly one cell label."""
        from adapt.configuration.schemas.user import UserSegmenterConfig
        from adapt.modules.detection.module import RadarCellSegmenter

        data = np.zeros((8, 8), dtype=np.float32)
        data[2:6, 2:6] = 50.0  # 4×4 = 16 pixels, clearly above any min_size
        ds = _refl_ds(data)
        config = make_detection_config(
            threshold=35,
            min_cellsize_gridpoint=1,
            segmenter=UserSegmenterConfig(filter_by_size=False),
        )
        result = RadarCellSegmenter(config).segment(ds)
        labels = result["cell_labels"].values
        assert labels.max() == 1  # exactly one connected component

    def test_extreme_reflectivity_values_do_not_crash(self, make_detection_config):
        """75 dBZ (extreme hail) is handled without error."""
        from adapt.modules.detection.module import RadarCellSegmenter

        data = np.zeros((10, 10), dtype=np.float32)
        data[2:6, 2:6] = 75.0  # 4×4 = 16 pixels — well above any min_size threshold
        ds = _refl_ds(data)
        config = make_detection_config(threshold=40)
        result = RadarCellSegmenter(config).segment(ds)
        assert result["cell_labels"].values.max() >= 1

    def test_all_nan_reflectivity_returns_no_cells(self, make_detection_config):
        """NaN reflectivity should not produce any labelled cells."""
        from adapt.modules.detection.module import RadarCellSegmenter

        data = np.full((6, 6), np.nan, dtype=np.float32)
        ds = _refl_ds(data)
        config = make_detection_config(threshold=40)
        result = RadarCellSegmenter(config).segment(ds)
        # NaN is below threshold — no cells
        assert result["cell_labels"].values.max() == 0


# ---------------------------------------------------------------------------
# Tracker edge cases
# ---------------------------------------------------------------------------


class TestTrackerEdgeCases:
    @pytest.fixture
    def tracker(self, tracking_module_config):
        from adapt.modules.tracking.module import CellTracker

        return CellTracker(tracking_module_config)

    def _make_ds(self, labels, time):
        H, W = labels.shape
        refl = np.where(labels > 0, 45.0, 0.0).astype(np.float32)
        proj = np.stack([labels.astype(np.int32)], axis=0)
        ds = xr.Dataset(
            {
                "cell_labels": (["y", "x"], labels.astype(np.int32)),
                "reflectivity": (["y", "x"], refl),
                "cell_projections": (["frame_offset", "y", "x"], proj),
                "heading_x": (["y", "x"], np.zeros_like(labels, dtype=np.float32)),
                "heading_y": (["y", "x"], np.zeros_like(labels, dtype=np.float32)),
            },
            coords={
                "y": np.arange(H) * 1000.0,
                "x": np.arange(W) * 1000.0,
                "frame_offset": [0],
                "time": time,
            },
        )
        return ds

    def _stats(self, label, time, cx=3.0, cy=3.0, area=4.0):
        return pd.DataFrame(
            [
                {
                    "time": time,
                    "time_volume_start": time,
                    "cell_label": label,
                    "cell_area_sqkm": area,
                    "area_40dbz_km2": area,
                    "cell_centroid_geom_x": cx,
                    "cell_centroid_geom_y": cy,
                    "cell_centroid_mass_lat": 35.0,
                    "cell_centroid_mass_lon": -97.0,
                    "radar_reflectivity_mean": 42.0,
                    "radar_reflectivity_max": 50.0,
                    "radar_differential_reflectivity_max": 1.5,
                }
            ]
        )

    def test_tracker_uid_is_deterministic(self, tracking_module_config):
        """Same input frames produce the same cell_uid on every run."""
        from adapt.modules.tracking.module import CellTracker

        t1 = np.datetime64("2025-01-01T12:00:00")
        labels = np.zeros((6, 6), dtype=np.int32)
        labels[2:4, 2:4] = 1
        ds1 = self._make_ds(labels, t1)
        stats1 = self._stats(1, t1)

        tracker_a = CellTracker(tracking_module_config)
        tracker_b = CellTracker(tracking_module_config)

        tracked_a, _ = tracker_a.track(ds1, stats1)
        tracked_b, _ = tracker_b.track(ds1, stats1)
        assert tracked_a.iloc[0]["cell_uid"] == tracked_b.iloc[0]["cell_uid"]

    def test_tracker_handles_large_time_gap(self, tracker):
        """Two frames 60 min apart are processed without error."""
        t1 = np.datetime64("2025-01-01T11:00:00")
        t2 = np.datetime64("2025-01-01T12:00:00")  # 60 min gap
        labels = np.zeros((6, 6), dtype=np.int32)
        labels[2:4, 2:4] = 1
        ds1 = self._make_ds(labels, t1)
        ds2 = self._make_ds(labels, t2)
        stats1 = self._stats(1, t1)
        stats2 = self._stats(1, t2)
        tracked1, events1 = tracker.track(ds1, stats1)
        tracked2, events2 = tracker.track(ds2, stats2)
        # Both frames produce valid tracked output — gap handling does not crash
        assert not tracked1.empty
        assert not tracked2.empty
        # Either the track continues with the same uid, or a new track is initiated
        assert "cell_uid" in tracked1.columns
        assert "cell_uid" in tracked2.columns


# ---------------------------------------------------------------------------
# Contract adversarial tests
# ---------------------------------------------------------------------------


class TestContractAdversarial:
    def test_contract_rejects_non_integer_labels(self):
        """Float labels must raise ContractViolation, not silently pass."""
        from adapt.contracts import check_segmented_ds

        labels = np.array([[0.0, 0.5, 1.0, 1.0]], dtype=np.float32)
        ds = xr.Dataset(
            {"cell_labels": (("y", "x"), labels)},
            coords={"x": range(4), "y": range(1)},
        )
        with pytest.raises(ContractViolation):
            check_segmented_ds(ds)

    def test_contract_rejects_negative_cell_labels(self):
        """Negative label values must raise ContractViolation."""
        from adapt.contracts import assert_segmented

        labels = np.array([[-1, 0, 1]], dtype=np.int32)
        ds = xr.Dataset(
            {"cell_labels": (("y", "x"), labels)},
            coords={"x": range(3), "y": range(1)},
        )
        with pytest.raises(ContractViolation):
            assert_segmented(ds, "cell_labels")

    def test_contract_rejects_3d_grid(self):
        """3D reflectivity field must raise — contract expects 2D."""
        from adapt.contracts import assert_gridded

        data = np.ones((3, 4, 4), dtype=np.float32)
        ds = xr.Dataset(
            {"reflectivity": (("z", "y", "x"), data)},
            coords={"x": range(4), "y": range(4), "z": range(3)},
        )
        with pytest.raises(ContractViolation):
            assert_gridded(ds, "reflectivity")

    def test_contract_rejects_invalid_event_type(self):
        """Unknown event_type string in cell events must raise."""
        from adapt.contracts import check_cell_events

        df = pd.DataFrame(
            {
                "time": pd.to_datetime(["2025-01-01"]),
                "event_type": ["ALIEN_STORM"],  # not a valid type
                "source_cell_uid": [None],
                "target_cell_uid": ["x"],
                "source_cell_label": [None],
                "target_cell_label": [1],
                "cost": [0.0],
                "is_dominant": [True],
                "event_group_id": [1],
            }
        )
        with pytest.raises(ContractViolation):
            check_cell_events(df)

    def test_tracked_cells_rejects_null_uid(self):
        """Null cell_uid in tracked_cells must raise ContractViolation."""
        from adapt.contracts import check_tracked_cells

        df = pd.DataFrame(
            {
                "time": pd.to_datetime(["2025-01-01"]),
                "cell_label": [1],
                "cell_uid": [None],
                "area": [4.0],
                "centroid_x": [2.5],
                "centroid_y": [2.5],
                "mean_reflectivity": [40.0],
                "max_reflectivity": [45.0],
                "core_area": [2.0],
            }
        )
        with pytest.raises(ContractViolation):
            check_tracked_cells(df)


# ---------------------------------------------------------------------------
# Execution graph adversarial tests
# ---------------------------------------------------------------------------


class TestExecutorAdversarial:
    def test_executor_extra_keys_in_initial_context_are_ignored(self):
        """Additional keys not declared as inputs are silently passed through."""
        from adapt.execution.graph.builder import GraphBuilder
        from adapt.execution.graph.executor import GraphExecutor
        from adapt.modules.base import BaseModule

        class Sink(BaseModule):
            name = "sink"
            inputs = ["x"]
            outputs = []

            def run(self, ctx):
                return {}

        nodes = GraphBuilder([Sink()]).build()
        result = GraphExecutor(nodes).run({"x": 1, "unexpected": 99})
        assert result["unexpected"] == 99

    def test_builder_with_zero_modules_returns_empty_list(self):
        """Building a graph with no modules produces an empty node list."""
        from adapt.execution.graph.builder import GraphBuilder

        nodes = GraphBuilder([]).build()
        assert nodes == []

    def test_executor_with_empty_graph_returns_context_unchanged(self):
        """Running an executor with no nodes returns the initial context."""
        from adapt.execution.graph.executor import GraphExecutor

        result = GraphExecutor([]).run({"key": "value"})
        assert result == {"key": "value"}


# ---------------------------------------------------------------------------
# Utils time adversarial tests
# ---------------------------------------------------------------------------


class TestUtilsTimeAdversarial:
    def test_numpy_datetime64_scalar_unwraps_to_date(self):
        """np.datetime64 scalar is unwrapped to a Python date via .item()."""
        from datetime import date

        from adapt.utils.time import normalize_time_scalar

        result = normalize_time_scalar(np.datetime64("2025-01-01"))
        assert isinstance(result, date)

    def test_object_without_item_passthrough(self):
        """Non-numpy objects without .item() are returned unchanged."""
        from adapt.utils.time import normalize_time_scalar

        obj = {"a": 1}  # dict — no .item()
        result = normalize_time_scalar(obj)
        assert result is obj
