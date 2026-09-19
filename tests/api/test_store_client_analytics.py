# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreClient analytics surface: select, read-only SQL, annotations, status."""

import pytest

from adapt.api.selection import FilterSpec
from adapt.api.store_client import StoreClient
from tests.api.synthetic_store import COLLECTION, RUN_1, RUN_2, build_synthetic_store

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    return build_synthetic_store(tmp_path_factory.mktemp("storean"))


@pytest.fixture
def client(built):
    c = StoreClient(built.root)
    yield c
    c.close()


class TestSelect:
    def test_population_selection_by_property(self, client):
        df = client.select(RUN_1, FilterSpec(max_refl_min_dbz=46.0), COLLECTION)
        assert df["cell_uid"].tolist() == ["uid-1"]

        assert client.select(RUN_1, FilterSpec(n_scans_min=5), COLLECTION).empty

    def test_selection_by_required_tag(self, client, built):
        client.annotate(
            RUN_1, built.uid, "confirmed", note="visually checked", collection=COLLECTION
        )

        tagged = client.select(
            RUN_1, FilterSpec(required_tags=frozenset({"confirmed"})), COLLECTION
        )
        assert tagged["cell_uid"].tolist() == [built.uid]

        assert client.select(RUN_1, FilterSpec(required_tags=frozenset({"nope"})), COLLECTION).empty

    def test_annotations_read_back(self, client, built):
        client.annotate(RUN_1, built.uid, "interesting", collection=COLLECTION)

        df = client.annotations(RUN_1, COLLECTION)
        assert set(df["tag"]) >= {"interesting"}
        assert set(df["cell_uid"]) == {built.uid}


class TestSql:
    def test_read_only_sql_over_products(self, client):
        df = client.sql("SELECT COUNT(*) AS n FROM cell_stats", COLLECTION)
        assert df.iloc[0]["n"] == 3  # 2 scans in run-1 + 1 in run-2

    def test_catalog_attached_for_scan_joins(self, client):
        df = client.sql(
            "SELECT COUNT(*) AS n FROM catalog.scans WHERE status = 'complete'", COLLECTION
        )
        assert df.iloc[0]["n"] == 3

    def test_writes_are_rejected(self, client):
        with pytest.raises(Exception, match="readonly|read-only|attempt to write"):
            client.sql("DELETE FROM cell_stats", COLLECTION)


class TestStatus:
    def test_artifacts_metadata(self, client, built):
        rows = client.artifacts(COLLECTION, run_id=RUN_1, artifact_type="segmentation2d")
        assert len(rows) == 2
        assert [r["scan_id"] for r in rows] == built.scan_ids[:2]

    def test_is_pipeline_running(self, client):
        assert client.is_pipeline_running(COLLECTION) is True  # run-2 is running

    def test_pipeline_progress(self, client, built):
        progress = client.pipeline_progress(COLLECTION)
        assert progress["run_id"] == RUN_2
        assert progress["status"] == "running"
        assert progress["complete_scans"] == 1

    def test_repository_info(self, client, built):
        info = client.repository_info()
        assert info["root"] == str(built.root)
        assert info["collections"] == [COLLECTION]
        assert info["runs"] == 2
