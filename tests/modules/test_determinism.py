# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Determinism tests: identical inputs + config ⇒ identical outputs.

Determinism is Adapt's first non-functional requirement (scientific
reproducibility). Each science module is run twice — fresh instance,
identical synthetic input — and the outputs compared exactly. Unseeded
randomness, wall-clock leakage, or iteration-order dependence fails here.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.modules.analysis.module import RadarCellAnalyzer
from adapt.modules.detection.module import RadarCellSegmenter
from adapt.modules.projection.module import RadarCellProjector
from adapt.modules.tracking.module import RadarCellTracker
from tests.helpers.fake_grid import make_textured_2d_ds

pytestmark = [pytest.mark.unit, pytest.mark.determinism]


def test_detection_is_deterministic(detection_module_config):
    """Watershed segmentation of the same textured field gives identical labels."""
    ds = make_textured_2d_ds(shape=(24, 24), seed=11, base=20.0, span=25.0)

    first = RadarCellSegmenter(detection_module_config).segment(ds)
    second = RadarCellSegmenter(detection_module_config).segment(ds)

    xr.testing.assert_identical(first, second)


def test_analysis_is_deterministic(analysis_module_config, labeled_ds_with_extras):
    """Per-cell statistics of the same labeled field are exactly equal."""
    first = RadarCellAnalyzer(analysis_module_config).extract(labeled_ds_with_extras)
    second = RadarCellAnalyzer(analysis_module_config).extract(labeled_ds_with_extras)

    pd.testing.assert_frame_equal(first, second, check_exact=True)


def test_projection_is_deterministic(projection_module_config, simple_labeled_ds_pair):
    """Optical-flow projection of the same frame pair is exactly equal."""
    first = RadarCellProjector(projection_module_config).project(simple_labeled_ds_pair)
    second = RadarCellProjector(projection_module_config).project(simple_labeled_ds_pair)

    xr.testing.assert_identical(first, second)


def _tracking_scan(time, labels):
    h, w = labels.shape
    refl = np.zeros((h, w), dtype=np.float32)
    refl[labels > 0] = 40.0
    ds = xr.Dataset(
        {
            "cell_labels": (("y", "x"), labels.astype(np.int32)),
            "reflectivity": (("y", "x"), refl),
            "cell_projections": (
                ("frame_offset", "y", "x"),
                labels.astype(np.int32)[np.newaxis, :, :],
            ),
            "heading_x": (("y", "x"), np.zeros((h, w), dtype=np.float32)),
            "heading_y": (("y", "x"), np.zeros((h, w), dtype=np.float32)),
        },
        coords={"y": np.arange(h) * 1000.0, "x": np.arange(w) * 1000.0, "frame_offset": [0]},
    )
    return ds.assign_coords(time=time)


def _tracking_stats(time, cell_label):
    return pd.DataFrame(
        [
            {
                "time": time,
                "time_volume_start": time,
                "cell_label": cell_label,
                "cell_area_sqkm": 4.0,
                "area_40dbz_km2": 4.0,
                "cell_centroid_geom_x": 2.5,
                "cell_centroid_geom_y": 2.5,
                "cell_centroid_mass_lat": 35.0,
                "cell_centroid_mass_lon": -97.0,
                "radar_reflectivity_mean": 40.0,
                "radar_reflectivity_max": 45.0,
                "radar_differential_reflectivity_max": 1.0,
            }
        ]
    )


def test_tracking_is_deterministic(tracking_module_config):
    """Two fresh trackers fed the same scan sequence emit identical uids and events."""
    t1 = np.datetime64("2024-01-01T12:00:00")
    t2 = np.datetime64("2024-01-01T12:05:00")
    labels = np.zeros((6, 6), dtype=np.int32)
    labels[2:4, 2:4] = 1

    def run_sequence():
        tracker = RadarCellTracker(tracking_module_config)
        frames = []
        for t in (t1, t2):
            tracked, events = tracker.track(_tracking_scan(t, labels), _tracking_stats(t, 1))
            frames.append((tracked, events))
        return frames

    for (tracked_a, events_a), (tracked_b, events_b) in zip(
        run_sequence(), run_sequence(), strict=True
    ):
        pd.testing.assert_frame_equal(tracked_a, tracked_b, check_exact=True)
        pd.testing.assert_frame_equal(events_a, events_b, check_exact=True)


def test_xlma_stat_is_deterministic():
    """Two runs over identical flashes and minute masks emit identical tables."""
    pytest.importorskip("pyproj")
    from adapt.modules.xlma_stat.config import XlmaStatConfig
    from adapt.modules.xlma_stat.module import MinuteMask, XlmaStatistics

    grid = (np.arange(21) - 10) * 1000.0
    mask_labels = np.zeros((21, 21), dtype=np.int32)
    mask_labels[9:12, 9:12] = 1
    mask = MinuteMask(
        minute_time=pd.Timestamp("2024-05-18T19:05"),
        cell_labels=mask_labels,
        x=grid,
        y=grid,
        cell_uid_lut=np.array(["NONE", "uid-A"], dtype=np.str_),
        source_scan_time=pd.Timestamp("2024-05-18T19:03"),
        target_scan_time=pd.Timestamp("2024-05-18T19:06"),
        interpolation_fraction=2 / 3,
    )
    flashes = pd.DataFrame(
        [
            {
                "flash_id": 1,
                "flash_time": pd.Timestamp("2024-05-18T19:05:10"),
                "flash_init_lat": 40.0,
                "flash_init_lon": -88.0,
                "flash_init_alt_m": 6000.0,
                "flash_center_lat": 40.0,
                "flash_center_lon": -88.0,
                "flash_area_km2": 10.0,
                "flash_energy_gj": 2.0,
                "flash_duration_s": 0.5,
                "source_count": 2,
            }
        ]
    )
    sources = pd.DataFrame(
        {
            "flash_id": [1, 1],
            "source_alt_m": 7000.0,
            "source_power_dbw": -10.0,
            "source_chi2": 1.0,
            "station_count": 8.0,
        }
    )

    def run_once():
        algo = XlmaStatistics(XlmaStatConfig(input_dir="/unused"))
        return algo.compute(flashes.copy(), sources.copy(), [mask], (40.0, -88.0))

    minutes_a, scan_a = run_once()
    minutes_b, scan_b = run_once()
    pd.testing.assert_frame_equal(minutes_a, minutes_b, check_exact=True)
    pd.testing.assert_frame_equal(scan_a, scan_b, check_exact=True)
