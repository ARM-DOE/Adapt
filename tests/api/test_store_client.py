# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreClient core reads: collections, runs, complete-only scans, table filters."""

from datetime import UTC, datetime

import pytest

from adapt.api.store_client import StoreClient
from adapt.persistence.errors import StoreError
from tests.api.synthetic_store import COLLECTION, RUN_1, RUN_2, T0, build_synthetic_store

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    return build_synthetic_store(tmp_path_factory.mktemp("storeapi"))


@pytest.fixture
def client(built):
    c = StoreClient(built.root)
    yield c
    c.close()


class TestOpen:
    def test_uninitialized_root_raises_naming_adapt_init(self, tmp_path):
        with pytest.raises(StoreError, match="adapt init"):
            StoreClient(tmp_path)

    def test_legacy_root_raises_obsolete_layout(self, tmp_path):
        (tmp_path / "adapt_registry.db").touch()
        with pytest.raises(StoreError, match="obsolete"):
            StoreClient(tmp_path)

    def test_client_is_read_only_at_open(self, built):
        before = sorted(p.name for p in built.root.rglob("*") if not p.name.startswith("."))
        client = StoreClient(built.root)
        client.collections()
        client.close()
        after = sorted(p.name for p in built.root.rglob("*") if not p.name.startswith("."))
        assert after == before

    def test_unknown_collection_raises(self, client):
        with pytest.raises(StoreError, match="NOPE"):
            client.scans("NOPE")


class TestCollectionsAndRuns:
    def test_collections(self, client):
        (coll,) = client.collections()
        assert coll.collection_id == COLLECTION
        assert coll.source_kind == "nexrad"
        assert coll.location_lat == pytest.approx(40.0)

    def test_runs_newest_first(self, client):
        runs = client.runs(COLLECTION)
        assert [r.run_id for r in runs] == [RUN_2, RUN_1]
        assert runs[0].status == "running"
        assert runs[1].status == "completed"
        assert runs[1].collection_id == COLLECTION

    def test_latest_run(self, client):
        assert client.latest_run(COLLECTION).run_id == RUN_2

    def test_run_by_id(self, client):
        run = client.run(RUN_1)
        assert run.run_id == RUN_1
        assert run.ended_at is not None

    def test_unknown_run_raises(self, client):
        with pytest.raises(StoreError, match="ghost"):
            client.run("ghost")


class TestScans:
    def test_run_scoped_scans_are_complete_only_and_ordered(self, client, built):
        scans = client.scans(COLLECTION, run_id=RUN_1)

        assert [s.scan_id for s in scans] == built.scan_ids[:2]  # pending 3rd excluded
        assert [s.scan_time for s in scans] == built.scan_times[:2]
        assert all(s.status == "complete" for s in scans)

    def test_cross_run_union_carries_run_id(self, client, built):
        scans = client.scans(COLLECTION)

        assert len(scans) == 3  # 2 complete in run-1 + 1 complete in run-2
        assert {(s.run_id, s.scan_id) for s in scans} == {
            (RUN_1, built.scan_ids[0]),
            (RUN_1, built.scan_ids[1]),
            (RUN_2, built.scan_ids[0]),
        }

    def test_time_window_and_limit(self, client, built):
        early = client.scans(COLLECTION, run_id=RUN_1, end=T0)
        assert [s.scan_id for s in early] == [built.scan_ids[0]]

        later = client.scans(
            COLLECTION, run_id=RUN_1, start=datetime(2026, 6, 1, 12, 1, tzinfo=UTC)
        )
        assert [s.scan_id for s in later] == [built.scan_ids[1]]

        limited = client.scans(COLLECTION, run_id=RUN_1, limit=1)
        assert len(limited) == 1


class TestTable:
    def test_reads_products_rows(self, client):
        df = client.table("cell_stats", COLLECTION, run_id=RUN_1)
        assert len(df) == 2
        assert set(df["run_id"]) == {RUN_1}
        assert "scan_id" in df.columns

    def test_run_id_optional_cross_run_union(self, client):
        df = client.table("cell_stats", COLLECTION)
        assert set(df["run_id"]) == {RUN_1, RUN_2}

    def test_equality_filter(self, client, built):
        df = client.table(
            "cell_stats", COLLECTION, run_id=RUN_1, filters={"scan_id": built.scan_ids[0]}
        )
        assert len(df) == 1

    def test_operator_filter(self, client):
        df = client.table(
            "cell_stats",
            COLLECTION,
            run_id=RUN_1,
            filters={"radar_reflectivity_max": {"op": "ge", "value": 46.0}},
        )
        assert len(df) == 1
        assert df.iloc[0]["radar_reflectivity_max"] == pytest.approx(46.0)

    def test_in_list_filter(self, client, built):
        df = client.table(
            "cell_stats", COLLECTION, filters={"scan_id": {"op": "in", "value": built.scan_ids[:1]}}
        )
        assert len(df) == 2  # scan 1 processed by both runs

    def test_unknown_table_raises(self, client):
        with pytest.raises(StoreError, match="no_such_table"):
            client.table("no_such_table", COLLECTION)

    def test_unknown_operator_raises(self, client):
        with pytest.raises(ValueError, match="op"):
            client.table(
                "cell_stats", COLLECTION, filters={"cell_label": {"op": "like", "value": "x"}}
            )


class TestRunConfig:
    def test_run_config_returns_parsed_resolved_config(self, client):
        cfg = client.run_config(RUN_1)
        assert cfg["global_"]["tracking_field"] == "reflectivity"
        assert cfg["global_"]["z_level"] == 2000.0

    def test_run_config_unknown_run_raises(self, client):
        with pytest.raises(StoreError, match="no-such-run"):
            client.run_config("no-such-run")
