# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreClient track reads: population, history, events, split/merge graph."""

import pandas as pd
import pytest

from adapt.api.store_client import StoreClient
from adapt.persistence.errors import StoreError
from adapt.persistence.products import SchemaLedger
from adapt.persistence.store import Store
from adapt.persistence.track_store import TrackStore
from tests.api.synthetic_store import COLLECTION, RUN_1, RUN_2, T0, build_synthetic_store

pytestmark = pytest.mark.unit


def _split_third_scan(built) -> None:
    """Process the pending third scan directly through TrackStore: uid-1 splits into uid-2."""
    store = Store.open(built.root)
    collection = store.collection(COLLECTION)
    ledger = SchemaLedger(collection.products_path, collection.catalog)
    events = pd.DataFrame(
        [
            {
                "event_type": "SPLIT",
                "source_cell_uid": "uid-1",
                "target_cell_uid": "uid-2",
                "source_cell_label": 1,
                "target_cell_label": 2,
                "cost": 0.1,
                "is_dominant": True,
                "event_group_id": "g-split",
            }
        ]
    )
    with TrackStore(collection.products_path, ledger=ledger) as ts:
        ts.write_scan(
            run_id=RUN_1,
            scan_time=T0.replace(minute=10),
            cell_stats_df=pd.DataFrame({"cell_label": [1, 2], "cell_area_sqkm": [30.0, 5.0]}),
            tracked_cells_df=pd.DataFrame(
                {
                    "cell_label": [1, 2],
                    "cell_uid": ["uid-1", "uid-2"],
                    "area": [30.0, 5.0],
                    "max_reflectivity": [47.0, 41.0],
                }
            ),
            cell_events_df=events,
            cell_adjacency_df=pd.DataFrame(
                columns=["cell_label_a", "cell_label_b", "touching_boundary_pixels"]
            ),
            scan_id=built.scan_ids[2],
        )
    store.close()


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    b = build_synthetic_store(tmp_path_factory.mktemp("storetr"))
    _split_third_scan(b)
    return b


@pytest.fixture
def client(built):
    c = StoreClient(built.root)
    yield c
    c.close()


class TestTracks:
    def test_tracks_run_scoped(self, client):
        df = client.tracks(COLLECTION, run_id=RUN_1)
        assert set(df["cell_uid"]) == {"uid-1", "uid-2"}

    def test_tracks_cross_run_union(self, client):
        df = client.tracks(COLLECTION)
        assert set(zip(df["run_id"], df["cell_uid"], strict=True)) == {
            (RUN_1, "uid-1"),
            (RUN_1, "uid-2"),
            (RUN_2, "uid-r2"),
        }

    def test_track_object(self, client):
        track = client.track(RUN_1, "uid-1", COLLECTION)
        assert track.n_scans == 3
        assert track.lifetime_s == pytest.approx(600.0)
        assert track.max_reflectivity_dbz == pytest.approx(47.0)

    def test_unknown_track_raises(self, client):
        with pytest.raises(StoreError, match="ghost"):
            client.track(RUN_1, "ghost", COLLECTION)

    def test_cells_run_scoped_and_time_ordered(self, client, built):
        df = client.cells(RUN_1, COLLECTION)
        assert set(df["scan_id"]) == set(built.scan_ids)
        assert df["scan_time"].is_monotonic_increasing

    def test_cells_empty_before_tracking(self, tmp_path):
        from adapt.persistence.store import init_store

        root = init_store(tmp_path / "empty")
        # provision an empty collection through the Store
        store = Store.open(root)
        store.collection("KTST")
        store.close()
        with StoreClient(root) as client:
            assert client.cells("none", "KTST").empty

    def test_track_history_time_ordered(self, client, built):
        df = client.track_history(RUN_1, "uid-1", COLLECTION)
        assert df["scan_id"].tolist() == built.scan_ids
        assert df["scan_time"].is_monotonic_increasing


class TestEventsAndGraph:
    def test_track_events_for_one_cell_covers_both_directions(self, client):
        events = client.track_events(RUN_1, "uid-1", COLLECTION)
        assert set(events["event_type"]) == {"INITIATION", "SPLIT"}

        child_events = client.track_events(RUN_1, "uid-2", COLLECTION)
        assert set(child_events["event_type"]) == {"SPLIT"}

    def test_track_graph_returns_connected_component(self, client):
        graph = client.track_graph(RUN_1, "uid-2", COLLECTION)

        assert set(graph.cell_uids) == {"uid-1", "uid-2"}
        assert {t.cell_uid for t in graph.tracks} == {"uid-1", "uid-2"}
        assert set(graph.events["event_type"]) == {"INITIATION", "SPLIT"}

    def test_track_graph_isolated_cell(self, client):
        graph = client.track_graph(RUN_2, "uid-r2", COLLECTION)
        assert set(graph.cell_uids) == {"uid-r2"}
