# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Phase-B tracking-quality behaviours: hard gap limits, physical motion
constraints, deterministic overlap-first matching, and persisted diagnostics.

Synthetic inputs with analytically known outcomes; no stored fixtures.
"""

import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig
from adapt.execution.nodes.tracking import TrackingModule
from adapt.modules.tracking.module import RadarCellTracker
from adapt.modules.tracking.projection import select_registration_labels

pytestmark = pytest.mark.unit


def _make_config(**overrides):
    d = tempfile.mkdtemp()
    try:
        param = ParamConfig()
        param.tracker.split_overlap_threshold = 0.4
        for key, val in overrides.items():
            setattr(param.tracker, key, val)
        user = UserConfig(base_dir=str(Path(d)), radar="TEST_RADAR")
        internal = resolve_config(param, user, None)
        return TrackingModule.build_config(internal)
    finally:
        shutil.rmtree(d, ignore_errors=True)


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
        coords={"y": np.arange(H) * 1000.0, "x": np.arange(W) * 1000.0, "frame_offset": [0]},
    )
    return ds.assign_coords(time=time)


def _cell_stats(time, rows):
    return pd.DataFrame(
        [
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
            for r in rows
        ]
    )


def _one_cell_scan(time, x_pix, proj_labels=None):
    """A single 2x2 cell placed at column x_pix on an 8x8 grid (1000 m pixels)."""
    labels = np.zeros((8, 8), dtype=np.int32)
    labels[2:4, x_pix : x_pix + 2] = 1
    cx = (x_pix + 0.5) * 1000.0
    stats = _cell_stats(
        time,
        [{"id": 1, "area": 4.0, "cx": cx, "cy": 2500.0, "mean_refl": 40.0, "max_refl": 45.0}],
    )
    ds = _synthetic_ds(time, labels, proj_labels=proj_labels if proj_labels is not None else labels)
    return ds, stats


# ---------------------------------------------------------------------------
# B1 — hard scan-gap limits
# ---------------------------------------------------------------------------


def test_gap_exceeded_terminates_and_restarts():
    """dt above max_tracking_gap_minutes terminates active tracks and starts fresh."""
    cfg = _make_config(max_tracking_gap_minutes=10.0)
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:20:00")  # 20 min > 10-min hard limit

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0)
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    ds1, stats1 = _one_cell_scan(t1, 2)
    tracked1, events1 = tracker.track(ds1, stats1)

    assert (events1["event_type"] == "CONTINUE").sum() == 0, "No match may cross the hard gap"
    assert uid0 in set(events1[events1["event_type"] == "TERMINATION"]["source_cell_uid"]), (
        "Active track must be terminated when the gap is exceeded"
    )
    assert (events1["event_type"] == "INITIATION").sum() == 1, "A fresh track must start"
    new_uid = str(events1[events1["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])
    assert new_uid != uid0


def test_non_monotonic_time_resets_without_crash():
    """A backwards scan time must not raise; it resets tracks instead."""
    cfg = _make_config()
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:05:00")
    t_back = np.datetime64("2024-01-01T12:00:00")  # earlier than t0

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0)
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    ds1, stats1 = _one_cell_scan(t_back, 2)
    _, events1 = tracker.track(ds1, stats1)  # must not raise

    assert (events1["event_type"] == "CONTINUE").sum() == 0
    assert uid0 in set(events1[events1["event_type"] == "TERMINATION"]["source_cell_uid"])
    assert (events1["event_type"] == "INITIATION").sum() == 1


def test_normal_gap_still_continues():
    """A within-limit gap with an exact projection still produces CONTINUE."""
    cfg = _make_config(max_tracking_gap_minutes=20.0, match_cost_threshold=0.0)
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")  # 5 min < 20-min limit

    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0)
    ds1, stats1 = _one_cell_scan(t1, 2, proj_labels=ds0["cell_labels"].values)
    _, events1 = tracker.track(ds1, stats1)

    assert (events1["event_type"] == "CONTINUE").sum() == 1


# ---------------------------------------------------------------------------
# B3 — physical motion constraints (hard reject before matching)
# ---------------------------------------------------------------------------


def test_velocity_exceeded_rejects_match():
    """An over-speed candidate is rejected even with a perfect projection overlap."""
    # x=2 → x=6 is 4000 m in 300 s = 13.3 m/s; cap at 5 m/s rejects it.
    cfg = _make_config(max_speed_ms=5.0, match_cost_threshold=0.0, max_tracking_gap_minutes=60.0)
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0)
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    # Projection predicts the jumped position exactly → overlap exists, but speed cap bites.
    ds1, stats1 = _one_cell_scan(t1, 6)
    _, events1 = tracker.track(ds1, stats1)

    assert (events1["event_type"] == "CONTINUE").sum() == 0
    assert uid0 in set(events1[events1["event_type"] == "TERMINATION"]["source_cell_uid"])
    assert (events1["event_type"] == "INITIATION").sum() == 1


def test_acceleration_exceeded_rejects_match():
    """A candidate far faster than the track's own prior speed is rejected."""
    # scan0→1: x2→x3 (3.33 m/s sets prior). scan1→2: x3→x6 (10 m/s) > 2×3.33.
    cfg = _make_config(
        max_speed_ms=40.0,
        max_speed_multiplier=2.0,
        match_cost_threshold=0.0,
        max_tracking_gap_minutes=60.0,
    )
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")
    t2 = np.datetime64("2024-01-01T12:10:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0)
    ds1, stats1 = _one_cell_scan(t1, 3)
    _, events1 = tracker.track(ds1, stats1)
    assert (events1["event_type"] == "CONTINUE").sum() == 1, "slow step must continue"

    ds2, stats2 = _one_cell_scan(t2, 6)
    _, events2 = tracker.track(ds2, stats2)
    assert (events2["event_type"] == "CONTINUE").sum() == 0, "accelerating step must be rejected"


# ---------------------------------------------------------------------------
# B4 — deterministic overlap-first matching
# ---------------------------------------------------------------------------


def test_unique_overlap_matches_despite_high_cost():
    """A unique projected-hull overlap continues a track even when the cost-based
    Hungarian post-filter (keep_cost) would have rejected it."""
    # keep_cost is tiny so a non-zero-cost pair fails the Hungarian filter, but the
    # projection covers the child fully (overlap 1.0 ≥ 0.3) → overlap-first matches.
    cfg = _make_config(keep_cost_threshold=0.01, match_cost_threshold=0.0)
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0)
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    # Cell shifts one pixel and brightens → small but non-zero 4-term cost.
    labels1 = np.zeros((8, 8), dtype=np.int32)
    labels1[2:4, 3:5] = 1
    stats1 = _cell_stats(
        t1,
        [{"id": 1, "area": 4.0, "cx": 3500.0, "cy": 2500.0, "mean_refl": 50.0, "max_refl": 55.0}],
    )
    ds1 = _synthetic_ds(t1, labels1, proj_labels=labels1)  # projection predicts new position
    tracked1, events1 = tracker.track(ds1, stats1)

    assert (events1["event_type"] == "CONTINUE").sum() == 1, "overlap-first must continue the track"
    assert str(tracked1.iloc[0]["cell_uid"]) == uid0, "continued cell keeps its uid"


# ---------------------------------------------------------------------------
# B6 — per-match diagnostics on accepted matches
# ---------------------------------------------------------------------------


def test_continue_event_carries_diagnostics():
    """An accepted CONTINUE records overlap, iou, distance, speed, cost, method."""
    cfg = _make_config(match_cost_threshold=0.0, max_tracking_gap_minutes=60.0)
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0)
    # cell moves one pixel; projection predicts it → unique overlap match
    ds1, stats1 = _one_cell_scan(t1, 3)
    _, events1 = tracker.track(ds1, stats1)

    cont = events1[events1["event_type"] == "CONTINUE"]
    assert len(cont) == 1
    row = cont.iloc[0]
    assert row["match_method"] in {"OVERLAP", "HUNGARIAN"}
    assert 0.0 <= float(row["candidate_overlap"]) <= 1.0
    assert 0.0 <= float(row["candidate_iou"]) <= 1.0
    # cell moved 1000 m in 300 s ≈ 3.33 m/s
    assert float(row["candidate_centroid_distance_m"]) == pytest.approx(1000.0, abs=1e-6)
    assert float(row["candidate_speed_ms"]) == pytest.approx(1000.0 / 300.0, abs=1e-6)
    assert pd.notna(row["candidate_final_cost"])


def test_initiation_event_has_null_diagnostics():
    """INITIATION rows carry no candidate diagnostics."""
    cfg = _make_config()
    tracker = RadarCellTracker(cfg)
    ds0, stats0 = _one_cell_scan(np.datetime64("2024-01-01T12:00:00"), 2)
    _, events0 = tracker.track(ds0, stats0)
    init = events0[events0["event_type"] == "INITIATION"].iloc[0]
    assert pd.isna(init["candidate_overlap"])
    assert pd.isna(init["match_method"])


# ---------------------------------------------------------------------------
# B5 — motion-state crossing prevention (heading-consistency penalty)
# ---------------------------------------------------------------------------


def test_heading_penalty_breaks_ambiguous_match_toward_consistent_track():
    """With an established +x velocity, a heading penalty steers an ambiguous
    match to the heading-consistent candidate instead of the reversed one."""
    cfg = _make_config(
        heading_change_penalty_weight=0.5,
        match_cost_threshold=0.0,
        max_tracking_gap_minutes=60.0,
    )
    tracker = RadarCellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")
    t2 = np.datetime64("2024-01-01T12:10:00")

    # scan0→1 establish a +x velocity (heading 0) for track label 1.
    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0)
    ds1, stats1 = _one_cell_scan(t1, 4)  # perfect projection → CONTINUE, vx>0
    tracker.track(ds1, stats1)

    # scan2: the registration hull (label 1) fills the whole row band, so it
    # overlaps P and Q identically (equal IoU). P and Q are equidistant from the
    # prev centroid (x=4) — 2000 m each. Q is made *cheaper* on base cost (its
    # reflectivity matches the track, P's differs) so that WITHOUT the heading
    # penalty Q wins. Only the +x/−x heading asymmetry can flip it back to P.
    labels2 = np.zeros((8, 8), dtype=np.int32)
    labels2[2:4, 6:8] = 1  # P ahead  (centroid x = 6500, +2000, +x consistent)
    labels2[2:4, 2:4] = 2  # Q behind (centroid x = 2500, −2000, −x reversed)
    proj2 = np.zeros((8, 8), dtype=np.int32)
    proj2[2:4, 0:8] = 1  # symmetric hull spanning the full row band
    stats2 = _cell_stats(
        t2,
        [
            {"id": 1, "area": 4.0, "cx": 6500.0, "cy": 2500.0, "mean_refl": 42.0, "max_refl": 45.0},
            {"id": 2, "area": 4.0, "cx": 2500.0, "cy": 2500.0, "mean_refl": 40.0, "max_refl": 45.0},
        ],
    )
    ds2 = _synthetic_ds(t2, labels2, proj_labels=proj2)
    _, events2 = tracker.track(ds2, stats2)

    cont = events2[events2["event_type"] == "CONTINUE"]
    assert len(cont) == 1, "the track should continue to exactly one cell"
    assert int(cont.iloc[0]["target_cell_label"]) == 1, "heading penalty must steer to the +x cell"


# ---------------------------------------------------------------------------
# B2 — registration-based multi-step projection
# ---------------------------------------------------------------------------


def test_registration_selects_nearest_minute():
    """The minute frame closest to the real gap is chosen for the hull."""
    f1 = np.full((4, 4), 11, dtype=np.int32)
    f2 = np.full((4, 4), 22, dtype=np.int32)
    f3 = np.full((4, 4), 33, dtype=np.int32)
    ds = xr.Dataset(
        {
            "registration_minutes": (["minute", "y", "x"], np.stack([f1, f2, f3])),
            "cell_projections": (["frame_offset", "y", "x"], np.zeros((1, 4, 4), dtype=np.int32)),
        },
        coords={"minute": [1, 2, 3]},
    )
    out = select_registration_labels(ds, dt_s=130.0)  # 2.17 min → nearest minute 2
    assert int(out[0, 0]) == 22


def test_registration_falls_back_to_cell_projections():
    """Without minute frames the whole-step cell_projections[0] is used."""
    ds = xr.Dataset(
        {"cell_projections": (["frame_offset", "y", "x"], np.full((1, 4, 4), 7, dtype=np.int32))}
    )
    out = select_registration_labels(ds, dt_s=300.0)
    assert int(out[0, 0]) == 7
