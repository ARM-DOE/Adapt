# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""decisions.db: bundles land in their tables with run/scan stamps, replace on re-write,
and the router dispatches ``DecisionTableWrite`` there without touching products."""

import sqlite3
from dataclasses import fields
from datetime import UTC, datetime

import pytest

from adapt.contracts import (
    DecisionTableWrite,
    PersistenceMeta,
    SegmentationComponent,
    SegmentationDecisions,
    SegmentationFrame,
    SegmentationSeed,
    TrackingCell,
    TrackingDecisions,
    TrackingIdentity,
    TrackingLatent,
    TrackingLineage,
    TrackingPair,
    TrackingScan,
)
from adapt.persistence.decision_store import DecisionStore
from adapt.persistence.output_router import StoreOutputRouter
from adapt.persistence.store import Store, init_store

pytestmark = pytest.mark.unit

SCAN_COUNTS = [f.name for f in fields(TrackingScan) if f.name.startswith("n_")]

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)
META = PersistenceMeta(
    scan_time=SCAN_TIME, scan_id="abc123", run_id="run-1", source_file="f", collection_id="KILX"
)


def _tracking() -> TrackingDecisions:
    scan = TrackingScan(270.0, None, **dict.fromkeys(SCAN_COUNTS, 1))
    cell = TrackingCell(1, "AAAA", 4.0, 40.0, 10.0, 3500.0, 2500.0, 3.69, 4.0, "CONTINUE")
    pair = TrackingPair(
        prev_cell_uid="AAAA",
        prev_cell_label=1,
        curr_cell_label=1,
        source="LIVE",
        steps=1,
        growth_radius_km=1.67,
        footprint_area_px=4,
        footprint_grown_px=16,
        cell_grown_px=16,
        intersection_px=12,
        o_c=0.75,
        o_h=0.75,
        u=0.25,
        predicted_x=2500.0,
        predicted_y=2500.0,
        curr_centre_x=3500.0,
        curr_centre_y=2500.0,
        d=1.2,
        speed_ms=3.7,
        prev_speed_ms=None,
        speed_limit_ms=None,
        heading_change_deg=None,
        h=0.0,
        cost=0.55,
        gate="PASS",
        component_id=0,
        cost_rank=1,
        margin=1.25,
        crossing_with_uid=None,
        outcome="CONTINUE",
    )
    lineage = (
        TrackingLineage("curr", "BBBB", 2, "SPLIT", "AAAA", 1, 0.8, None, 0.65, True, True, None),
        TrackingLineage(
            "curr", "BBBB", 2, "INITIATION", None, None, None, None, None, True, False, None
        ),
    )
    identity = (TrackingIdentity("SPLIT", "AAAA", "curr", 1, 3.69, 0.4, True, "SCORE", False),)
    latent = (
        TrackingLatent("CCCC", 3, "prev-scan", 1, "TERMINATED", None, "CREATED", 5, 0.0, 0.0),
    )
    return TrackingDecisions(scan, (cell,), (pair,), lineage, identity, latent)


def _segmentation() -> SegmentationDecisions:
    frame = SegmentationFrame(10, 10, 1, 1, 1, 0, 1, 0, 0, 1)
    seeds = (
        SegmentationSeed(1, "HMAXIMA", 5, 3, 48.0, 1, 1, *([None] * 8), "SEEDED", 10, 1),
        SegmentationSeed(
            None,
            "CARRIED",
            5,
            7,
            None,
            None,
            1,
            1,
            0,
            9,
            4.0,
            False,
            1,
            1,
            0,
            "NO_DEFICIT",
            None,
            None,
        ),
    )
    components = (SegmentationComponent(1, 10, 48.0, 40.0, 1, 1, 0, 0, 1),)
    return SegmentationDecisions(frame, seeds, components)


@pytest.fixture
def collection(tmp_path):
    store = Store.open(init_store(tmp_path / "store"))
    coll = store.collection("KILX")
    yield coll
    store.close()


def _rows(path, sql):
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql)]
    finally:
        conn.close()


def test_tracking_bundle_lands_in_its_tables_with_stamps(collection):
    with DecisionStore(collection.decisions_path) as store:
        store.write(_tracking(), META)
    (scan,) = _rows(collection.decisions_path, "select * from tracking_scans")
    (pair,) = _rows(collection.decisions_path, "select * from tracking_pairs")
    (cell,) = _rows(collection.decisions_path, "select * from tracking_cells")
    assert (scan["run_id"], scan["scan_id"], scan["n_prev"]) == ("run-1", "abc123", 1)
    assert scan["scan_time"].startswith("2026-08-13T17:31:58")
    assert (pair["prev_cell_uid"], pair["outcome"], pair["gate"]) == ("AAAA", "CONTINUE", "PASS")
    assert pair["prev_speed_ms"] is None
    assert (cell["cell_uid"], cell["fate"]) == ("AAAA", "CONTINUE")


def test_every_lineage_hypothesis_is_kept_in_order(collection):
    with DecisionStore(collection.decisions_path) as store:
        store.write(_tracking(), META)
    rows = _rows(collection.decisions_path, "select * from tracking_lineage order by seq")
    assert [(r["hypothesis"], r["chosen"]) for r in rows] == [("SPLIT", 1), ("INITIATION", 0)]


def test_identity_and_latent_records_land_in_their_tables(collection):
    with DecisionStore(collection.decisions_path) as store:
        store.write(_tracking(), META)
    (identity,) = _rows(collection.decisions_path, "select * from tracking_identity")
    (latent,) = _rows(collection.decisions_path, "select * from tracking_latent")
    assert (identity["rule"], identity["winner"]) == ("SCORE", 1)
    assert (latent["cell_uid"], latent["status"]) == ("CCCC", "CREATED")


def test_segmentation_bundle_numbers_seeds_in_order(collection):
    with DecisionStore(collection.decisions_path) as store:
        store.write(_segmentation(), META)
    seeds = _rows(collection.decisions_path, "select * from segmentation_seeds order by seq")
    assert [s["seq"] for s in seeds] == [0, 1]
    assert [s["decision"] for s in seeds] == ["SEEDED", "NO_DEFICIT"]
    assert seeds[1]["footprint_holds_seed"] == 0 and seeds[1]["final_label"] is None


def test_rewriting_a_scan_replaces_its_rows(collection):
    with DecisionStore(collection.decisions_path) as store:
        store.write(_tracking(), META)
        store.write(_tracking(), META)
    assert len(_rows(collection.decisions_path, "select * from tracking_pairs")) == 1


def test_router_dispatches_decision_writes_without_a_scan_product(collection):
    class _Module:
        name = "tracking"
        persistence = (DecisionTableWrite(key="tracking_decisions"),)

    StoreOutputRouter(collection).persist([_Module()], {"tracking_decisions": _tracking()}, META)
    assert len(_rows(collection.decisions_path, "select * from tracking_scans")) == 1
    assert _rows(collection.products_path, "select * from table_schemas") == []


def test_router_skips_when_the_key_is_absent(collection):
    class _Module:
        name = "tracking"
        persistence = (DecisionTableWrite(key="tracking_decisions"),)

    StoreOutputRouter(collection).persist([_Module()], {}, META)
    assert _rows(collection.decisions_path, "select * from tracking_scans") == []


def test_a_decisions_db_from_the_previous_tracker_still_takes_new_rows(collection):
    # Runs into an existing store: the v1 tracking tables stay as they are and
    # the v2 records go to their own tables.
    conn = sqlite3.connect(collection.decisions_path)
    conn.execute("CREATE TABLE IF NOT EXISTS tracking_candidates (run_id TEXT, opc REAL)")
    conn.commit()
    conn.close()
    with DecisionStore(collection.decisions_path) as store:
        store.write(_tracking(), META)
    assert len(_rows(collection.decisions_path, "select * from tracking_pairs")) == 1
