# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""User story tests — scientist perspective.

These tests describe end-user scientific outcomes rather than implementation
details.  All use synthetic numpy/xarray data; no real NEXRAD files, no IO.

Pattern: Given / When / Then, one scenario per test.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _reflectivity_ds(values_2d: np.ndarray) -> xr.Dataset:
    H, W = values_2d.shape
    return xr.Dataset(
        {"reflectivity": (("y", "x"), values_2d.astype(np.float32))},
        coords={"y": np.arange(H) * 1000.0, "x": np.arange(W) * 1000.0},
        attrs={"z_level_m": 2000},
    )


def _labeled_ds(labels: np.ndarray, time=None) -> xr.Dataset:
    """Dataset with cell_labels, reflectivity, projections, headings."""
    H, W = labels.shape
    refl = np.zeros((H, W), dtype=np.float32)
    refl[labels > 0] = 45.0
    projections = np.stack([labels.astype(np.int32)], axis=0)
    ds = xr.Dataset(
        {
            "cell_labels": (["y", "x"], labels.astype(np.int32)),
            "reflectivity": (["y", "x"], refl),
            "cell_projections": (["frame_offset", "y", "x"], projections),
            "heading_x": (["y", "x"], np.zeros((H, W), dtype=np.float32)),
            "heading_y": (["y", "x"], np.zeros((H, W), dtype=np.float32)),
        },
        coords={
            "y": np.arange(H) * 1000.0,
            "x": np.arange(W) * 1000.0,
            "frame_offset": [0],
        },
    )
    if time is None:
        time = np.datetime64("2025-06-15T12:00:00")
    return ds.assign_coords(time=time)


def _cell_stats_row(label: int, time, *, cx=5.0, cy=5.0, area=4.0) -> dict:
    return {
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


# ---------------------------------------------------------------------------
# Detection stories
# ---------------------------------------------------------------------------


class TestScientistCanDetectCells:
    def test_user_can_detect_cells_from_threshold(self, make_detection_config):
        """Given: 2D data with a clear 40-dBZ cluster.
        When: segmenter runs with threshold=35.
        Then: at least one labeled cell appears at the cluster location.
        """
        from adapt.modules.detection.module import RadarCellSegmenter

        refl = np.zeros((10, 10), dtype=np.float32)
        refl[4:7, 4:7] = 45.0  # 9-pixel cluster at centre
        ds = _reflectivity_ds(refl)
        config = make_detection_config(threshold=35, min_cellsize_gridpoint=4)
        result = RadarCellSegmenter(config).segment(ds)
        labels = result["cell_labels"].values
        assert labels[5, 5] > 0, "Centre of cluster should be labelled"

    def test_user_sees_no_cells_when_storm_below_threshold(self, detection_module_config):
        """Given: all reflectivity below detection threshold.
        When: segmenter runs.
        Then: output has no cells (all labels == 0).
        """
        from adapt.modules.detection.module import RadarCellSegmenter

        refl = np.full((8, 8), 20.0, dtype=np.float32)  # threshold is 40 dBZ by default
        ds = _reflectivity_ds(refl)
        result = RadarCellSegmenter(detection_module_config).segment(ds)
        assert result["cell_labels"].values.max() == 0

    def test_user_can_separate_two_distinct_storms(self, make_detection_config):
        """Given: two separated storm cores.
        When: segmenter runs.
        Then: exactly two cell labels appear.
        """
        from adapt.configuration.schemas.user import UserSegmenterConfig
        from adapt.modules.detection.module import RadarCellSegmenter

        refl = np.zeros((12, 12), dtype=np.float32)
        refl[2:4, 1:4] = 48.0  # storm A
        refl[8:10, 8:11] = 46.0  # storm B
        ds = _reflectivity_ds(refl)
        config = make_detection_config(
            threshold=40,
            segmenter=UserSegmenterConfig(filter_by_size=False),
        )
        result = RadarCellSegmenter(config).segment(ds)
        assert result["cell_labels"].values.max() == 2


# ---------------------------------------------------------------------------
# Tracking stories
# ---------------------------------------------------------------------------


class TestScientistCanTrackStorms:
    @pytest.fixture
    def tracker(self, tracking_module_config):
        from adapt.modules.tracking.module import CellTracker

        return CellTracker(tracking_module_config)

    def test_user_can_track_a_persistent_storm(self, tracker):
        """Given: one cell that persists at the same location across two frames.
        When: tracker runs on both frames.
        Then: the same cell_uid appears in both tracked outputs.
        """
        t1 = np.datetime64("2025-01-01T12:00:00")
        t2 = np.datetime64("2025-01-01T12:05:00")
        labels = np.zeros((8, 8), dtype=np.int32)
        labels[3:5, 3:5] = 1  # stationary 2×2 cell
        ds1 = _labeled_ds(labels, t1)
        ds2 = _labeled_ds(labels, t2)
        stats1 = pd.DataFrame([_cell_stats_row(1, t1, cx=3.5, cy=3.5)])
        stats2 = pd.DataFrame([_cell_stats_row(1, t2, cx=3.5, cy=3.5)])
        tracked1, events1 = tracker.track(ds1, stats1)
        tracked2, events2 = tracker.track(ds2, stats2)
        assert tracked1.iloc[0]["cell_uid"] == tracked2.iloc[0]["cell_uid"]
        assert events1["event_type"].iloc[0] == "INITIATION"
        assert events2["event_type"].iloc[0] == "CONTINUE"

    def test_user_sees_empty_output_when_no_storm(self, tracker):
        """Given: no cells in any frame.
        When: tracker runs.
        Then: tracked_cells and events are empty DataFrames, no exception raised.
        """
        t1 = np.datetime64("2025-01-01T12:00:00")
        t2 = np.datetime64("2025-01-01T12:05:00")
        empty_labels = np.zeros((6, 6), dtype=np.int32)
        ds1 = _labeled_ds(empty_labels, t1)
        ds2 = _labeled_ds(empty_labels, t2)
        stats_empty = pd.DataFrame(
            columns=[
                "time",
                "time_volume_start",
                "cell_label",
                "cell_area_sqkm",
                "area_40dbz_km2",
                "cell_centroid_geom_x",
                "cell_centroid_geom_y",
                "cell_centroid_mass_lat",
                "cell_centroid_mass_lon",
                "radar_reflectivity_mean",
                "radar_reflectivity_max",
                "radar_differential_reflectivity_max",
            ]
        )
        tracked1, events1 = tracker.track(ds1, stats_empty)
        tracked2, events2 = tracker.track(ds2, stats_empty)
        assert tracked1.empty
        assert tracked2.empty

    def test_user_can_identify_storm_initiation(self, tracker):
        """Given: a cell appears for the first time.
        When: tracker runs.
        Then: events contain an INITIATION event with a non-null cell_uid.
        """
        t1 = np.datetime64("2025-01-01T12:00:00")
        labels = np.zeros((6, 6), dtype=np.int32)
        labels[2:4, 2:4] = 1
        ds1 = _labeled_ds(labels, t1)
        stats1 = pd.DataFrame([_cell_stats_row(1, t1)])
        tracked, events = tracker.track(ds1, stats1)
        initiations = events[events["event_type"] == "INITIATION"]
        assert len(initiations) == 1
        assert initiations.iloc[0]["target_cell_uid"] is not None
