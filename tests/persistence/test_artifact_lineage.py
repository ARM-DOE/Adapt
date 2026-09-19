# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Artifact lineage: multi-parent edges, traversable both directions."""

from datetime import UTC, datetime

import pytest

from adapt.persistence.objects import ArtifactMeta, ObjectStore
from adapt.persistence.store import Store, StoreError, init_store

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)


@pytest.fixture
def collection(tmp_path):
    root = init_store(tmp_path / "store")
    store = Store.open(root)
    coll = store.collection("KILX")
    yield coll
    store.close()


@pytest.fixture
def objects(collection):
    return ObjectStore(collection.objects_dir, collection.catalog)


def _commit_artifact(objects, artifact_type: str, parents=()) -> str:
    handle = objects.begin(suffix=".bin")
    handle.staging_path.write_bytes(artifact_type.encode())
    meta = ArtifactMeta(
        artifact_type=artifact_type,
        producer="test",
        run_id="run-1",
        scan_id="abc123",
        observation_time=SCAN_TIME,
    )
    return objects.commit(handle, meta, parents=parents).artifact_id


class TestLineage:
    def test_two_parent_artifact_traversable_both_directions(self, objects, collection):
        parent_a = _commit_artifact(objects, "raw_volume")
        parent_b = _commit_artifact(objects, "lma_flashes")
        child = _commit_artifact(objects, "analysis", parents=(parent_a, parent_b))

        assert set(collection.catalog.parents_of(child)) == {parent_a, parent_b}
        assert collection.catalog.children_of(parent_a) == [child]
        assert collection.catalog.children_of(parent_b) == [child]

    def test_add_lineage_unknown_parent_raises(self, objects, collection):
        child = _commit_artifact(objects, "gridded3d")

        with pytest.raises(StoreError, match="ghost"):
            collection.catalog.add_lineage(child, "ghost")

    def test_add_lineage_unknown_child_raises(self, objects, collection):
        parent = _commit_artifact(objects, "raw_volume")

        with pytest.raises(StoreError, match="ghost"):
            collection.catalog.add_lineage("ghost", parent)

    def test_commit_with_unknown_parent_raises(self, objects):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(b"data")
        meta = ArtifactMeta(
            artifact_type="gridded3d",
            producer="test",
            run_id="run-1",
            scan_id="abc123",
            observation_time=SCAN_TIME,
        )

        with pytest.raises(StoreError, match="ghost"):
            objects.commit(handle, meta, parents=("ghost",))

    def test_duplicate_edge_raises(self, objects, collection):
        parent = _commit_artifact(objects, "raw_volume")
        child = _commit_artifact(objects, "gridded3d", parents=(parent,))

        with pytest.raises(StoreError, match="lineage"):
            collection.catalog.add_lineage(child, parent)

    def test_leaf_artifacts_have_no_edges(self, objects, collection):
        artifact = _commit_artifact(objects, "raw_volume")

        assert collection.catalog.parents_of(artifact) == []
        assert collection.catalog.children_of(artifact) == []
