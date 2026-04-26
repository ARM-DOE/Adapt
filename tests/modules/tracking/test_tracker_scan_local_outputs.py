# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

import numpy as np
import pandas as pd
import xarray as xr
import tempfile
from pathlib import Path

import pytest

from adapt.modules.tracking.module import RadarCellTracker
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.user import UserConfig
from adapt.configuration.schemas.resolve import resolve_config


@pytest.fixture
def config():
    d = tempfile.mkdtemp()
    try:
        param = ParamConfig()
        param.tracker.split_overlap_threshold = 0.4
        user = UserConfig(base_dir=str(Path(d)), radar="TEST_RADAR")
        return resolve_config(param, user, None)
    finally:
        import shutil
        shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def tracker(config):
    return RadarCellTracker(config)


def _synthetic_ds(time, labels, refl=None, proj_labels=None):
    H, W = labels.shape
    if refl is None:
        refl = np.zeros((H, W), dtype=np.float32)
        refl[labels > 0] = 40.0
    if proj_labels is None:
        proj_labels = labels

    projections = np.stack([proj_labels.astype(np.int32)], axis=0)
    ds = xr.Dataset(
        {
            "cell_labels": (["y", "x"], labels.astype(np.int32)),
            "reflectivity": (["y", "x"], refl.astype(np.float32)),
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
    return ds.assign_coords(time=time)


def _cell_stats(time, rows):
    data = []
    for r in rows:
        data.append(
            {
                "time": time,
                "time_volume_start": time,
                "cell_label": r["id"],
                "cell_area_sqkm": r["area"],
                "area_40dbz_km2": r.get("area40", r["area"]),
                "cell_centroid_geom_x": r["cx"],
                "cell_centroid_geom_y": r["cy"],
                "cell_centroid_mass_lat": r.get("lat", 35.0),
                "cell_centroid_mass_lon": r.get("lon", -97.0),
                "radar_reflectivity_mean": r["mean_refl"],
                "radar_reflectivity_max": r["max_refl"],
                "radar_differential_reflectivity_max": r.get("max_zdr", 1.0),
            }
        )
    return pd.DataFrame(data)


def test_birth_and_continue_events(tracker):
    t1 = np.datetime64("2024-01-01T12:00:00")
    t2 = np.datetime64("2024-01-01T12:05:00")

    labels1 = np.zeros((6, 6), dtype=np.int32)
    labels1[2:4, 2:4] = 1
    ds1 = _synthetic_ds(t1, labels1)
    stats1 = _cell_stats(t1, [{"id": 1, "area": 4.0, "cx": 2.5, "cy": 2.5, "mean_refl": 40.0, "max_refl": 45.0}])

    tracked1, events1 = tracker.track(ds1, stats1)
    assert len(tracked1) == 1
    assert "cell_uid" in tracked1.columns
    assert pd.notna(tracked1.iloc[0]["cell_uid"])
    uid1 = str(tracked1.iloc[0]["cell_uid"])
    assert len(events1[events1["event_type"] == "INITIATION"]) == 1

    labels2 = labels1.copy()
    ds2 = _synthetic_ds(t2, labels2, proj_labels=labels1)
    stats2 = _cell_stats(t2, [{"id": 1, "area": 4.0, "cx": 2.5, "cy": 2.5, "mean_refl": 40.0, "max_refl": 45.0}])

    tracked2, events2 = tracker.track(ds2, stats2)
    assert len(tracked2) == 1
    assert str(tracked2.iloc[0]["cell_uid"]) == uid1
    assert "source_cell_uid" in events2.columns
    assert "target_cell_uid" in events2.columns
    assert len(events2[events2["event_type"] == "CONTINUE"]) == 1
    assert len(events2[events2["event_type"] == "INITIATION"]) == 0


def test_split_event(tracker):
    t1 = np.datetime64("2024-01-01T12:00:00")
    t2 = np.datetime64("2024-01-01T12:05:00")

    labels1 = np.zeros((8, 8), dtype=np.int32)
    labels1[3:5, 2:6] = 1
    ds1 = _synthetic_ds(t1, labels1)
    stats1 = _cell_stats(t1, [{"id": 1, "area": 8.0, "cx": 3.5, "cy": 3.5, "mean_refl": 40.0, "max_refl": 45.0}])
    tracker.track(ds1, stats1)

    labels2 = np.zeros((8, 8), dtype=np.int32)
    labels2[3:5, 2:4] = 1
    labels2[3:5, 4:6] = 2
    proj = labels1.copy()
    ds2 = _synthetic_ds(t2, labels2, proj_labels=proj)
    stats2 = _cell_stats(
        t2,
        [
            {"id": 1, "area": 4.0, "cx": 2.5, "cy": 3.5, "mean_refl": 40.0, "max_refl": 45.0},
            {"id": 2, "area": 4.0, "cx": 4.5, "cy": 3.5, "mean_refl": 40.0, "max_refl": 45.0},
        ],
    )

    tracked2, events2 = tracker.track(ds2, stats2)
    assert len(tracked2) == 2
    assert len(events2[events2["event_type"] == "SPLIT"]) == 1
    assert tracked2["cell_uid"].nunique() == 2


def test_merge_event_emits_death(tracker):
    t1 = np.datetime64("2024-01-01T12:00:00")
    t2 = np.datetime64("2024-01-01T12:05:00")

    labels1 = np.zeros((10, 10), dtype=np.int32)
    labels1[4:6, 2:4] = 1
    labels1[4:6, 6:8] = 2
    ds1 = _synthetic_ds(t1, labels1)
    stats1 = _cell_stats(
        t1,
        [
            {"id": 1, "area": 4.0, "cx": 2.5, "cy": 4.5, "mean_refl": 40.0, "max_refl": 45.0},
            {"id": 2, "area": 4.0, "cx": 6.5, "cy": 4.5, "mean_refl": 40.0, "max_refl": 45.0},
        ],
    )
    tracker.track(ds1, stats1)

    labels2 = np.zeros((10, 10), dtype=np.int32)
    labels2[4:6, 3:7] = 1
    proj = np.zeros((10, 10), dtype=np.int32)
    proj[4:6, 2:4] = 1
    proj[4:6, 6:8] = 2
    ds2 = _synthetic_ds(t2, labels2, proj_labels=proj)
    stats2 = _cell_stats(t2, [{"id": 1, "area": 8.0, "cx": 4.5, "cy": 4.5, "mean_refl": 45.0, "max_refl": 50.0}])

    tracked2, events2 = tracker.track(ds2, stats2)
    assert len(tracked2) == 1
    assert len(events2[events2["event_type"] == "MERGE"]) == 1
    deaths = events2[events2["event_type"] == "TERMINATION"]
    assert len(deaths) >= 1
    assert deaths["source_cell_uid"].notna().any()
