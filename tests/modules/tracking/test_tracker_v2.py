# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""CellTracker v2 end to end on synthetic scans with known answers.

Grid 1 km; cells at 40 dBZ over a 30 dBZ cell threshold unless stated.
``proj`` is the previous scan's labels advected to the current scan
(``cell_projections[0]``); the flow fields carry latent footprints.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.modules.tracking.module import CellTracker

pytestmark = pytest.mark.unit

T0 = np.datetime64("2025-06-17T14:00:00")
DT = np.timedelta64(277, "s")
SHAPE = (40, 60)


def _scan(k, labels, proj=None, refl=None, flow=(0.0, 0.0)):
    if refl is None:
        refl = np.where(labels > 0, 40.0, 0.0)
    proj = labels * 0 if proj is None else proj
    ds = xr.Dataset(
        {
            "cell_labels": (["y", "x"], labels.astype(np.int32)),
            "reflectivity": (["y", "x"], refl.astype(np.float32)),
            "cell_projections": (["frame_offset", "y", "x"], proj[None].astype(np.int32)),
            "heading_x": (["y", "x"], np.full(SHAPE, flow[0], dtype=np.float32)),
            "heading_y": (["y", "x"], np.full(SHAPE, flow[1], dtype=np.float32)),
        },
        coords={
            "y": np.arange(SHAPE[0]) * 1000.0,
            "x": np.arange(SHAPE[1]) * 1000.0,
            "frame_offset": [0],
        },
    ).assign_coords(time=T0 + k * DT)
    stats = pd.DataFrame(
        [
            {
                "cell_label": int(lbl),
                "cell_area_sqkm": float((labels == lbl).sum()),
                "radar_reflectivity_mean": float(refl[labels == lbl].mean()),
                "radar_reflectivity_max": float(refl[labels == lbl].max()),
                "time_volume_start": T0 + k * DT,
                "cell_centroid_mass_lat": 35.0,
                "cell_centroid_mass_lon": -97.0,
            }
            for lbl in np.unique(labels[labels > 0])
        ]
    )
    return ds, stats


def _box(r0, r1, c0, c1, label=1, grid=None):
    grid = np.zeros(SHAPE, dtype=np.int32) if grid is None else grid
    grid[r0:r1, c0:c1] = label
    return grid


def _run(tracker, scans):
    out = []
    for k, (labels, proj, *rest) in enumerate(scans):
        refl = rest[0] if rest else None
        ds, stats = _scan(k, labels, proj, refl)
        out.append(tracker.track(ds, stats, scan_id=f"scan{k:03d}"))
    return out


def _types(events):
    return sorted(events["event_type"])


def _uid(tracked, label):
    return tracked.set_index("cell_label").loc[label, "cell_uid"]


@pytest.fixture
def tracker(make_tracking_config):
    return CellTracker(make_tracking_config())


# ── continuation ────────────────────────────────────────────────────────────


def test_first_scan_initiates_every_cell(tracker):
    labels = _box(5, 9, 5, 9, 1, _box(20, 24, 30, 34, 2))
    ((tracked, events),) = _run(tracker, [(labels, None)])
    assert _types(events) == ["INITIATION", "INITIATION"]
    assert tracked["cell_uid"].nunique() == 2


def test_a_cell_where_its_footprint_predicts_continues_its_identity(tracker):
    a = _box(10, 14, 10, 14)
    b = _box(10, 14, 12, 16)  # moved 2 km east, as the flow predicted
    (t0, _), (t1, e1) = _run(tracker, [(a, None), (b, b)])
    assert _types(e1) == ["CONTINUE"]
    assert _uid(t1, 1) == _uid(t0, 1)


def test_continue_event_carries_the_link_evidence(tracker):
    a = _box(10, 14, 10, 14)
    _, (_, e1) = _run(tracker, [(a, None), (a, a)])
    row = e1.iloc[0]
    assert row["candidate_opc"] == pytest.approx(1.0)
    assert row["candidate_ocp"] == pytest.approx(1.0)
    assert row["candidate_final_cost"] == pytest.approx(0.0)
    assert row["match_method"] == "ASSIGNED"
    assert row["source_scan_id"] == "scan000"


def test_a_cell_far_from_every_footprint_is_an_initiation(tracker):
    a = _box(10, 14, 10, 14)
    far = _box(30, 34, 45, 49)
    _, (_, e1) = _run(tracker, [(a, None), (far, a)])
    assert "CONTINUE" not in set(e1["event_type"])
    assert "INITIATION" in set(e1["event_type"])


def test_a_link_faster_than_the_speed_cap_is_rejected(make_tracking_config):
    tracker = CellTracker(make_tracking_config(tracker={"max_speed_ms": 5.0}))
    a = _box(10, 14, 10, 14)
    b = _box(10, 14, 12, 16)  # 2 km in 277 s = 7.2 m/s
    _, (_, e1) = _run(tracker, [(a, None), (b, b)])
    assert "CONTINUE" not in set(e1["event_type"])
    pairs = tracker.decisions().pairs
    assert [p.gate for p in pairs] == ["SPEED"]


def test_no_two_continuing_links_cross(tracker):
    # Two cells whose footprints predict they exchanged places diagonally
    # (8.5 km in 277 s, within the speed cap).
    prev = _box(5, 9, 5, 9, 1, _box(5, 9, 13, 17, 2))
    curr = _box(8, 12, 5, 9, 1, _box(8, 12, 13, 17, 2))
    proj = _box(8, 12, 13, 17, 1, _box(8, 12, 5, 9, 2))  # 1 → right, 2 → left
    _, (_, e1) = _run(tracker, [(prev, None), (curr, proj)])
    assert (e1["event_type"] == "CONTINUE").sum() == 1
    outcomes = sorted(p.outcome for p in tracker.decisions().pairs)
    assert outcomes == ["CONTINUE", "CROSSING"]


# ── split, merge, identity ─────────────────────────────────────────────────


def test_a_fragment_inside_a_continuing_footprint_is_a_split_child(tracker):
    parent = _box(10, 16, 10, 22)
    children = _box(10, 16, 10, 15, 1, _box(10, 16, 17, 22, 2))
    (t0, _), (t1, e1) = _run(tracker, [(parent, None), (children, parent)])
    assert _types(e1) == ["CONTINUE", "SPLIT"]
    split = e1[e1["event_type"] == "SPLIT"].iloc[0]
    assert split["source_cell_uid"] == _uid(t0, 1)
    assert split["target_cell_uid"] != _uid(t0, 1)


def test_a_split_child_far_from_the_parent_centre_is_still_a_split(tracker):
    # A 30 km parent sheds a fragment at its far end: 12.5 km from the parent's
    # centre in 277 s (45 m/s) is the parent's size, not motion. Lineage links
    # are tested on the fractions and the crossing test, not on the speed gates.
    parent = _box(10, 16, 5, 35)
    children = _box(10, 16, 5, 28, 1, _box(10, 16, 30, 35, 2))
    _, (_, e1) = _run(tracker, [(parent, None), (children, parent)])
    assert _types(e1) == ["CONTINUE", "SPLIT"]


def test_a_footprint_inside_a_continuing_cell_is_a_merge_source(tracker):
    prev = _box(10, 16, 10, 15, 1, _box(10, 16, 17, 22, 2))
    merged = _box(10, 16, 10, 22)
    _, (_, e1) = _run(tracker, [(prev, None), (merged, prev)])
    assert _types(e1) == ["CONTINUE", "LATENT", "MERGE"]


def test_identity_at_a_merge_follows_the_stronger_source(tracker):
    # Weak large cell 1 (35 dBZ, 20 px) and strong small cell 2 (55 dBZ, 12 px)
    # merge. Cell 1 explains more of the survivor and wins the assignment, but
    # cell 2 carries far more intensity mass, so it keeps the identity.
    prev = _box(10, 15, 10, 14, 1, _box(10, 13, 15, 19, 2))
    refl0 = np.where(prev == 1, 35.0, np.where(prev == 2, 55.0, 0.0))
    merged = _box(10, 15, 10, 19)
    refl1 = np.where(merged > 0, 45.0, 0.0)
    (t0, _), (t1, e1) = _run(tracker, [(prev, None, refl0), (merged, prev, refl1)])
    cont = e1[e1["event_type"] == "CONTINUE"].iloc[0]
    assert cont["source_cell_uid"] == _uid(t0, 2)
    assert cont["match_method"] == "IDENTITY_MERGE"
    assert _uid(t1, 1) == _uid(t0, 2)
    merge = e1[e1["event_type"] == "MERGE"].iloc[0]
    assert merge["source_cell_uid"] == _uid(t0, 1)
    assert any(r.transferred and r.winner for r in tracker.decisions().identity)


def test_identity_never_moves_to_a_physically_impossible_continuation(make_tracking_config):
    # The same merge as above, but with a speed cap the stronger source's jump to
    # the survivor's centre violates: the identity stays with the assigned cell.
    tracker = CellTracker(make_tracking_config(tracker={"max_speed_ms": 9.3}))
    prev = _box(10, 15, 10, 14, 1, _box(10, 13, 15, 19, 2))
    refl0 = np.where(prev == 1, 35.0, np.where(prev == 2, 55.0, 0.0))
    merged = _box(10, 15, 10, 19)
    refl1 = np.where(merged > 0, 45.0, 0.0)
    (t0, _), (t1, e1) = _run(tracker, [(prev, None, refl0), (merged, prev, refl1)])
    # Centre to survivor centre: 2.5 km (9.0 m/s) for cell 1, 2.69 km (9.7 m/s) for cell 2.
    assert _uid(t1, 1) == _uid(t0, 1)
    assert e1[e1["event_type"] == "CONTINUE"].iloc[0]["match_method"] == "ASSIGNED"


def test_every_split_and_merge_event_has_one_chosen_lineage_record(tracker):
    prev = _box(10, 15, 10, 14, 1, _box(10, 13, 15, 19, 2))
    refl0 = np.where(prev == 1, 35.0, np.where(prev == 2, 55.0, 0.0))
    merged = _box(10, 15, 10, 19)
    refl1 = np.where(merged > 0, 45.0, 0.0)
    (_, _), (_, e1) = _run(tracker, [(prev, None, refl0), (merged, prev, refl1)])
    lineage = tracker.decisions().lineage
    for kind in ("MERGE", "SPLIT"):
        sources = set(e1.loc[e1["event_type"] == kind, "source_cell_uid"])
        chosen = [r for r in lineage if r.hypothesis == kind and r.chosen]
        side_uid = {r.cell_uid if kind == "MERGE" else r.partner_uid for r in chosen}
        assert len(chosen) == len(e1[e1["event_type"] == kind])
        assert side_uid == sources
    latent_chosen = {r.cell_uid for r in lineage if r.hypothesis == "LATENT" and r.chosen}
    assert latent_chosen == set()  # the only unlinked track merged


# ── latent tracks ──────────────────────────────────────────────────────────


def test_a_cell_missing_for_one_scan_resumes_its_identity(tracker):
    a = _box(10, 14, 10, 14)
    empty = np.zeros(SHAPE, dtype=np.int32)
    (t0, _), (_, e1), (t2, e2) = _run(tracker, [(a, None), (empty, a), (a, empty)])
    assert _types(e1) == ["LATENT"]
    assert _types(e2) == ["RESUMED"]
    assert _uid(t2, 1) == _uid(t0, 1)
    resumed = e2.iloc[0]
    assert resumed["source_scan_id"] == "scan000"


def test_an_unresumed_latent_track_terminates_after_n_scans(tracker):
    a = _box(10, 14, 10, 14)
    empty = np.zeros(SHAPE, dtype=np.int32)
    (t0, _), (_, e1), (_, e2) = _run(tracker, [(a, None), (empty, a), (empty, empty)])
    assert _types(e2) == ["TERMINATION"]
    term = e2.iloc[0]
    assert term["source_cell_uid"] == _uid(t0, 1)
    assert term["source_scan_id"] == "scan000"


def test_without_latency_a_track_terminates_at_once(make_tracking_config):
    tracker = CellTracker(make_tracking_config(tracker={"latent_scans": 0}))
    a = _box(10, 14, 10, 14)
    empty = np.zeros(SHAPE, dtype=np.int32)
    _, (_, e1) = _run(tracker, [(a, None), (empty, a)])
    assert _types(e1) == ["TERMINATION"]


def test_a_latent_footprint_moves_with_the_flow(tracker):
    a = _box(10, 14, 10, 14)
    empty = np.zeros(SHAPE, dtype=np.int32)
    moved = _box(10, 14, 16, 20)  # 6 km east of the last footprint after one more scan
    scans = []
    for k, (labels, proj) in enumerate([(a, None), (empty, a), (moved, empty)]):
        scans.append(_scan(k, labels, proj, flow=(6.0, 0.0) if k == 2 else (0.0, 0.0)))
    t0, _ = tracker.track(*scans[0], scan_id="s0")
    tracker.track(*scans[1], scan_id="s1")
    t2, e2 = tracker.track(*scans[2], scan_id="s2")
    assert _types(e2) == ["RESUMED"]
    assert _uid(t2, 1) == _uid(t0, 1)


# ── decisions and determinism ───────────────────────────────────────────────


def test_every_candidate_pair_and_cell_is_logged(tracker):
    a = _box(10, 14, 10, 14, 1, _box(10, 14, 20, 24, 2))
    _run(tracker, [(a, None), (a, a)])
    d = tracker.decisions()
    assert d.scan.n_pairs == len(d.pairs) == 2
    assert {c.fate for c in d.cells} == {"CONTINUE"}
    assert d.scan.n_continue == 2


def test_identical_input_gives_identical_output(make_tracking_config):
    prev = _box(10, 16, 10, 15, 1, _box(10, 16, 17, 22, 2))
    merged = _box(10, 16, 10, 22)
    runs = []
    for _ in range(2):
        tracker = CellTracker(make_tracking_config())
        outs = _run(tracker, [(prev, None), (merged, prev), (prev, merged)])
        runs.append(outs)
    for (ta, ea), (tb, eb) in zip(*runs, strict=True):
        pd.testing.assert_frame_equal(ta, tb)
        pd.testing.assert_frame_equal(ea, eb)


def test_gap_beyond_the_limit_resets_every_track(tracker):
    a = _box(10, 14, 10, 14)
    ds0, s0 = _scan(0, a)
    ds1, s1 = _scan(5, a, a)  # 23 min later, limit 10 min
    tracker.track(ds0, s0, scan_id="s0")
    _, e1 = tracker.track(ds1, s1, scan_id="s1")
    assert _types(e1) == ["INITIATION", "TERMINATION"]
    assert tracker.decisions().scan.reset_code == "TRACK_GAP_EXCEEDED"
