# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tracking-quality behaviours: hard gap limits, physical motion gates, joint
assignment, the heading term, and persisted diagnostics.

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
from adapt.modules.tracking.module import CellTracker

pytestmark = pytest.mark.unit


def _make_config(**overrides):
    d = tempfile.mkdtemp()
    try:
        param = ParamConfig()
        param.tracker.split_overlap_threshold = 0.4
        param.tracker.merge_overlap_threshold = 0.4
        for key, val in overrides.items():
            # The scan-gap bound is global now — one value shared by the seed
            # carry, projection and tracking — so it no longer lives on tracker.
            if key == "max_tracking_gap_minutes":
                param.global_.max_scan_gap_minutes = val
            else:
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
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:20:00")  # 20 min > 10-min hard limit

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0, scan_id="site001scan")
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    ds1, stats1 = _one_cell_scan(t1, 2)
    tracked1, events1 = tracker.track(ds1, stats1, scan_id="site002scan")

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
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:05:00")
    t_back = np.datetime64("2024-01-01T12:00:00")  # earlier than t0

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0, scan_id="site003scan")
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    ds1, stats1 = _one_cell_scan(t_back, 2)
    _, events1 = tracker.track(ds1, stats1, scan_id="site004scan")  # must not raise

    assert (events1["event_type"] == "CONTINUE").sum() == 0
    assert uid0 in set(events1[events1["event_type"] == "TERMINATION"]["source_cell_uid"])
    assert (events1["event_type"] == "INITIATION").sum() == 1


def test_normal_gap_still_continues():
    """A within-limit gap with an exact projection still produces CONTINUE."""
    cfg = _make_config(max_tracking_gap_minutes=20.0)
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")  # 5 min < 20-min limit

    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0, scan_id="site005scan")
    ds1, stats1 = _one_cell_scan(t1, 2, proj_labels=ds0["cell_labels"].values)
    _, events1 = tracker.track(ds1, stats1, scan_id="site006scan")

    assert (events1["event_type"] == "CONTINUE").sum() == 1


# ---------------------------------------------------------------------------
# B3 — physical motion constraints (hard reject before matching)
# ---------------------------------------------------------------------------


def test_velocity_exceeded_rejects_match():
    """An over-speed candidate is rejected even with a perfect projection overlap."""
    # x=2 → x=6 is 4000 m in 300 s = 13.3 m/s; cap at 5 m/s rejects it.
    cfg = _make_config(max_speed_ms=5.0, max_tracking_gap_minutes=60.0)
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0, scan_id="site007scan")
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    # Projection predicts the jumped position exactly → overlap exists, but speed cap bites.
    ds1, stats1 = _one_cell_scan(t1, 6)
    _, events1 = tracker.track(ds1, stats1, scan_id="site008scan")

    assert (events1["event_type"] == "CONTINUE").sum() == 0
    # Not continued: the track is kept latent for possible resumption.
    assert uid0 in set(events1[events1["event_type"] == "LATENT"]["source_cell_uid"])
    assert (events1["event_type"] == "INITIATION").sum() == 1


def _one_cell_scan_wide(time, x_pix, proj_labels=None):
    """Like ``_one_cell_scan`` on a 16-column grid, for multi-step motion."""
    labels = np.zeros((8, 16), dtype=np.int32)
    labels[2:4, x_pix : x_pix + 2] = 1
    stats = _cell_stats(
        time,
        [
            {
                "id": 1,
                "area": 4.0,
                "cx": (x_pix + 0.5) * 1000.0,
                "cy": 2500.0,
                "mean_refl": 40.0,
                "max_refl": 45.0,
            }
        ],
    )
    return _synthetic_ds(time, labels, proj_labels=proj_labels), stats


def test_speed_jump_beyond_the_additive_limit_rejects_match():
    """After a 3.3 m/s step the next may reach 3.3 + a·Δt + 2√2σ/Δt ≈ 24.8 m/s
    (Δt = 300 s); an 8 km jump (26.7 m/s) is rejected although under v_max."""
    tracker = CellTracker(_make_config())
    t0, t1, t2 = (np.datetime64(f"2024-01-01T12:{m:02d}:00") for m in (0, 5, 10))
    tracker.track(*_one_cell_scan_wide(t0, 2), scan_id="site009scan")
    _, events1 = tracker.track(*_one_cell_scan_wide(t1, 3), scan_id="site010scan")
    assert (events1["event_type"] == "CONTINUE").sum() == 1, "slow step must continue"
    _, events2 = tracker.track(*_one_cell_scan_wide(t2, 11), scan_id="site011scan")
    assert (events2["event_type"] == "CONTINUE").sum() == 0
    assert [p.gate for p in tracker.decisions().pairs] == ["SPEED_CHANGE"]


def test_a_slow_cell_may_start_moving():
    """The limit is additive: from 3.3 m/s a 20 m/s step (6×) is admitted."""
    tracker = CellTracker(_make_config())
    t0, t1, t2 = (np.datetime64(f"2024-01-01T12:{m:02d}:00") for m in (0, 5, 10))
    tracker.track(*_one_cell_scan_wide(t0, 2), scan_id="site032scan")
    tracker.track(*_one_cell_scan_wide(t1, 3), scan_id="site033scan")
    _, events2 = tracker.track(*_one_cell_scan_wide(t2, 9), scan_id="site034scan")
    assert (events2["event_type"] == "CONTINUE").sum() == 1


# ---------------------------------------------------------------------------
# Joint assignment
# ---------------------------------------------------------------------------


def test_unique_candidate_is_assigned_and_keeps_its_uid():
    """A cell facing a single footprint is linked when the cost is under the ceiling."""
    cfg = _make_config()
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    _, events0 = tracker.track(ds0, stats0, scan_id="site012scan")
    uid0 = str(events0[events0["event_type"] == "INITIATION"].iloc[0]["target_cell_uid"])

    ds1, stats1 = _one_cell_scan(t1, 3)  # single cell, single prediction → unique
    tracked1, events1 = tracker.track(ds1, stats1, scan_id="site013scan")

    cont = events1[events1["event_type"] == "CONTINUE"]
    assert len(cont) == 1
    assert cont.iloc[0]["match_method"] == "ASSIGNED"
    assert str(tracked1.iloc[0]["cell_uid"]) == uid0, "continued cell keeps its uid"


def test_ambiguous_group_is_resolved_jointly():
    """Two projected hulls that each overlap both current cells form one 2×2
    group, resolved jointly: both tracks continue, to different cells. The hulls
    are kept disjoint by placing them on different rows (one label per pixel)."""
    cfg = _make_config(max_tracking_gap_minutes=60.0)
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    labels0 = np.zeros((6, 12), dtype=np.int32)
    labels0[2:4, 3:5] = 1
    labels0[2:4, 6:8] = 2
    stats0 = _cell_stats(
        t0,
        [
            {"id": 1, "area": 4.0, "cx": 3500.0, "cy": 2500.0, "mean_refl": 40.0, "max_refl": 45.0},
            {"id": 2, "area": 4.0, "cx": 6500.0, "cy": 2500.0, "mean_refl": 40.0, "max_refl": 45.0},
        ],
    )
    tracker.track(_synthetic_ds(t0, labels0, proj_labels=labels0), stats0, scan_id="site014scan")

    labels1 = labels0.copy()  # current cells at the same positions
    # Disjoint hulls on separate rows, each spanning both current cells' columns.
    proj1 = np.zeros((6, 12), dtype=np.int32)
    proj1[2, 3:8] = 1  # row 2 spans cols 3..7 → overlaps both cells' row 2
    proj1[3, 3:8] = 2  # row 3 spans cols 3..7 → overlaps both cells' row 3
    _, events1 = tracker.track(
        _synthetic_ds(t1, labels1, proj_labels=proj1), stats0, scan_id="site015scan"
    )

    cont = events1[events1["event_type"] == "CONTINUE"]
    assert len(cont) == 2
    assert cont["target_cell_label"].nunique() == 2
    assert {p.component_id for p in tracker.decisions().pairs} == {0}


# ---------------------------------------------------------------------------
# B6 — per-match diagnostics on accepted matches
# ---------------------------------------------------------------------------


def test_continue_event_carries_diagnostics():
    """An accepted CONTINUE records overlap, iou, distance, speed, cost, method."""
    cfg = _make_config(max_tracking_gap_minutes=60.0)
    tracker = CellTracker(cfg)

    t0 = np.datetime64("2024-01-01T12:00:00")
    t1 = np.datetime64("2024-01-01T12:05:00")

    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0, scan_id="site016scan")
    # cell moves one pixel; projection predicts it → unique overlap match
    ds1, stats1 = _one_cell_scan(t1, 3)
    _, events1 = tracker.track(ds1, stats1, scan_id="site017scan")

    cont = events1[events1["event_type"] == "CONTINUE"]
    assert len(cont) == 1
    row = cont.iloc[0]
    assert row["match_method"] == "ASSIGNED"
    assert 0.0 <= float(row["candidate_opc"]) <= 1.0
    assert 0.0 <= float(row["candidate_ocp"]) <= 1.0
    # The projection predicts the cell exactly (zero residual); it moved 1000 m in 300 s.
    assert float(row["candidate_centroid_distance_m"]) == pytest.approx(0.0, abs=1e-6)
    assert float(row["candidate_speed_ms"]) == pytest.approx(1000.0 / 300.0)
    assert pd.notna(row["candidate_final_cost"])


def test_initiation_event_has_null_diagnostics():
    """INITIATION rows carry no candidate diagnostics."""
    cfg = _make_config()
    tracker = CellTracker(cfg)
    ds0, stats0 = _one_cell_scan(np.datetime64("2024-01-01T12:00:00"), 2)
    _, events0 = tracker.track(ds0, stats0, scan_id="site018scan")
    init = events0[events0["event_type"] == "INITIATION"].iloc[0]
    assert pd.isna(init["candidate_opc"])
    assert pd.isna(init["match_method"])


# ---------------------------------------------------------------------------
# B5 — motion-state crossing prevention (heading-consistency penalty)
# ---------------------------------------------------------------------------


def test_heading_term_breaks_an_ambiguous_match_toward_the_consistent_track():
    """With an established +x step, two candidates equally far either side of
    the prediction are told apart only by the heading term (w_h = 1)."""
    tracker = CellTracker(_make_config())
    t0, t1, t2 = (np.datetime64(f"2024-01-01T12:{m:02d}:00") for m in (0, 5, 10))
    tracker.track(*_one_cell_scan_wide(t0, 2), scan_id="site019scan")
    tracker.track(*_one_cell_scan_wide(t1, 5), scan_id="site020scan")  # +3 km: heading +x

    labels2 = np.zeros((8, 16), dtype=np.int32)
    labels2[2:4, 8:10] = 1  # P: +3 km, straight on
    labels2[2:4, 2:4] = 2  # Q: −3 km, reversed
    proj2 = np.zeros((8, 16), dtype=np.int32)
    proj2[2:4, 0:12] = 1  # footprint centred on the previous cell, covering both
    stats2 = _cell_stats(
        t2,
        [
            {"id": 1, "area": 4.0, "cx": 8500.0, "cy": 2500.0, "mean_refl": 40.0, "max_refl": 45.0},
            {"id": 2, "area": 4.0, "cx": 2500.0, "cy": 2500.0, "mean_refl": 40.0, "max_refl": 45.0},
        ],
    )
    _, events2 = tracker.track(
        _synthetic_ds(t2, labels2, proj_labels=proj2), stats2, scan_id="site021scan"
    )
    cont = events2[events2["event_type"] == "CONTINUE"]
    assert len(cont) == 1
    assert int(cont.iloc[0]["target_cell_label"]) == 1
    h = {p.curr_cell_label: p.h for p in tracker.decisions().pairs}
    assert h == pytest.approx({1: 0.0, 2: 1.0})


# ---------------------------------------------------------------------------
# B2 — registration-based multi-step projection
# ---------------------------------------------------------------------------


def test_matching_uses_the_hull_registered_to_the_current_scan():
    """The registration hull is the previous labels advected by the full flow
    step (``cell_projections[0]``, fraction 1.0 of the gap) — never an
    under-advected minute frame.

    The minute frames carry a datetime64 ``minute`` coord and an
    ``interpolation_fraction`` coord exactly as the projection module writes
    them; the frame nearest t_prev sits 3 px short of the cell.
    """
    cfg = _make_config(max_tracking_gap_minutes=60.0)
    tracker = CellTracker(cfg)
    t0, t1 = np.datetime64("2024-01-01T12:00:00"), np.datetime64("2024-01-01T12:04:30")

    ds0, stats0 = _one_cell_scan(t0, 2)
    tracker.track(ds0, stats0, scan_id="site030scan")

    ds1, stats1 = _one_cell_scan(t1, 6)  # cell_projections[0]: the cell exactly at x=6
    frames = []
    for x in (3, 4, 5, 5):
        frame = np.zeros((8, 8), dtype=np.int32)
        frame[2:4, x : x + 2] = 1
        frames.append(frame)
    minutes = np.arange(np.datetime64("2024-01-01T12:01:00"), t1, np.timedelta64(1, "m")).astype(
        "datetime64[ns]"
    )
    fractions = ((minutes - t0) / (t1 - t0)).astype(np.float32)
    ds1["registration_minutes"] = (("minute", "y", "x"), np.stack(frames))
    ds1 = ds1.assign_coords(minute=minutes, interpolation_fraction=("minute", fractions))

    _, events1 = tracker.track(ds1, stats1, scan_id="site031scan")
    assert (events1["event_type"] == "CONTINUE").sum() == 1
    assert (events1["event_type"] == "TERMINATION").sum() == 0
