# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""build_snapshot against a synthetic on-disk repository.

Extends the shared synthetic repo test-locally (ALTER TABLE + inserts);
the shared helper itself is never modified.
"""

import sqlite3
from datetime import UTC, datetime

import pytest

from adapt.api.client import RepositoryClient
from adapt.consumers.target_selection.repository_source import build_snapshot
from tests.api.synthetic_repo import build_synthetic_repo

pytestmark = pytest.mark.integration

RUN_ID = "2024JUN01-1200-KDIX"
RADAR = "KDIX"
T0 = "2024-06-01T12:00:00Z"
T1 = "2024-06-01T14:00:00Z"  # 120 min after T0

# Real catalog schema: forward projections are 1-indexed (index 0 is the
# registration centroid, stored separately) — projection{k} = k intervals ahead.
_CBS_INSERT = (
    "INSERT INTO cells_by_scan (run_id, scan_time, cell_label, cell_uid, "
    "cell_area_sqkm, cell_centroid_mass_lat, cell_centroid_mass_lon, "
    "radar_reflectivity_max, age_seconds, "
    "cell_centroid_projection1_lat, cell_centroid_projection1_lon, "
    "cell_centroid_projection2_lat, cell_centroid_projection2_lon) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

_TRACK_INSERT = (
    "INSERT INTO cell_tracks (run_id, cell_uid, first_seen_time, last_seen_time, "
    "n_scans, origin_type, duration_seconds) VALUES (?,?,?,?,?,?,?)"
)


def _extend_catalog(db_path):
    conn = sqlite3.connect(str(db_path))
    for k in (1, 2):
        for ax in ("lat", "lon"):
            conn.execute(
                f"ALTER TABLE cells_by_scan ADD COLUMN cell_centroid_projection{k}_{ax} REAL"
            )
    # uid_beta: two scans (area 100 -> 160), projections at T1.
    conn.execute(
        _CBS_INSERT,
        (RUN_ID, T0, 2, "uid_beta", 100.0, 35.0, -97.0, 48.1, 0.0, None, None, None, None),
    )
    conn.execute(
        _CBS_INSERT,
        (RUN_ID, T1, 2, "uid_beta", 160.0, 35.05, -97.0, 55.0, 7200.0, 35.1, -97.0, 35.2, -97.0),
    )
    # uid_gamma: first seen at T1, no projections.
    conn.execute(
        _CBS_INSERT,
        (RUN_ID, T1, 3, "uid_gamma", 50.0, 34.9, -97.2, 30.0, 0.0, None, None, None, None),
    )
    conn.execute(_TRACK_INSERT, (RUN_ID, "uid_gamma", T1, T1, 1, "INITIATION", 0.0))
    # uid_delta: seen only at T0, WITH projections — at T0 the run has a single
    # scan time, so lead times are underivable (the first-scan realtime case).
    conn.execute(
        _CBS_INSERT,
        (RUN_ID, T0, 4, "uid_delta", 60.0, 35.2, -97.1, 40.0, 0.0, 35.25, -97.1, 35.3, -97.1),
    )
    conn.execute(_TRACK_INSERT, (RUN_ID, "uid_delta", T0, T0, 1, "INITIATION", 0.0))
    conn.commit()
    conn.close()


@pytest.fixture
def make_client():
    """Build RepositoryClients and release every one at teardown.

    A client holds catalog connections open; left unclosed they pin the
    repository files, which on Windows blocks tmp_path cleanup.
    """
    clients = []

    def _make(root):
        client = RepositoryClient(root)
        clients.append(client)
        return client

    yield _make
    for client in clients:
        client.close()


@pytest.fixture
def snapshot(tmp_path, make_client):
    root = build_synthetic_repo(tmp_path)
    _extend_catalog(root / RADAR / "catalog.db")
    client = make_client(root)
    return build_snapshot(client, RUN_ID, RADAR, growth_window_scans=4)


def _cell(snapshot, uid):
    return next(c for c in snapshot.cells if c.uid == uid)


def test_latest_scan_only(snapshot):
    assert snapshot.scan_time == datetime(2024, 6, 1, 14, 0, tzinfo=UTC)
    # uid_alpha exists only at T0 and must not appear.
    assert {c.uid for c in snapshot.cells} == {"uid_beta", "uid_gamma"}


def test_growth_rate_slope(snapshot):
    # (160 - 100) km2 over 120 min = 0.5 km2/min.
    assert _cell(snapshot, "uid_beta").growth_rate_sqkm_per_min == pytest.approx(0.5)


def test_trajectory_lead_seconds(snapshot):
    trajectory = _cell(snapshot, "uid_beta").trajectory
    assert [p.lead_seconds for p in trajectory] == [7200.0, 14400.0]
    assert [p.lat for p in trajectory] == [35.1, 35.2]


def test_values_include_track_columns(snapshot):
    assert _cell(snapshot, "uid_beta").values["n_scans"] == 6


def test_null_projections_empty_trajectory(snapshot):
    assert _cell(snapshot, "uid_gamma").trajectory == ()


def test_single_scan_growth_zero(snapshot):
    assert _cell(snapshot, "uid_gamma").growth_rate_sqkm_per_min == 0.0


def test_no_rows_raises(tmp_path, make_client):
    root = build_synthetic_repo(tmp_path)
    client = make_client(root)
    with pytest.raises(ValueError, match="bogus"):
        build_snapshot(client, "bogus", RADAR, growth_window_scans=4)


def test_at_replays_earlier_scan(tmp_path, make_client):
    root = build_synthetic_repo(tmp_path)
    _extend_catalog(root / RADAR / "catalog.db")
    client = make_client(root)
    snap = build_snapshot(
        client,
        RUN_ID,
        RADAR,
        growth_window_scans=4,
        at=datetime(2024, 6, 1, 12, 0, tzinfo=UTC),
    )
    assert snap.scan_time == datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    # uid_gamma does not exist yet at T0; uid_alpha does.
    assert {c.uid for c in snap.cells} == {"uid_alpha", "uid_beta", "uid_delta"}
    beta = _cell(snap, "uid_beta")
    assert beta.growth_rate_sqkm_per_min == 0.0  # single scan of history at T0
    assert beta.trajectory == ()


def test_first_scan_projections_have_no_lead_times(tmp_path, make_client):
    # At the first scan of a run, projections exist but no scan cadence does:
    # the trajectory is empty (defined condition), not an error.
    root = build_synthetic_repo(tmp_path)
    _extend_catalog(root / RADAR / "catalog.db")
    client = make_client(root)
    snap = build_snapshot(
        client,
        RUN_ID,
        RADAR,
        growth_window_scans=4,
        at=datetime(2024, 6, 1, 12, 0, tzinfo=UTC),
    )
    assert _cell(snap, "uid_delta").trajectory == ()


def test_at_before_first_scan_raises(tmp_path, make_client):
    root = build_synthetic_repo(tmp_path)
    _extend_catalog(root / RADAR / "catalog.db")
    client = make_client(root)
    with pytest.raises(ValueError, match="2024-05-01"):
        build_snapshot(
            client,
            RUN_ID,
            RADAR,
            growth_window_scans=4,
            at=datetime(2024, 5, 1, 0, 0, tzinfo=UTC),
        )
