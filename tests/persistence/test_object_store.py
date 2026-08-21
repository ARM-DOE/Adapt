# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""ObjectStore: uuid before write, invisible until atomic commit, clean abort."""

import hashlib
from datetime import UTC, datetime

import pytest

from adapt.persistence.objects import ArtifactMeta, ObjectStore
from adapt.persistence.store import Store, StoreError, init_store

pytestmark = pytest.mark.unit

PAYLOAD = b"level-two radar volume bytes"
SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)


def _meta(scan_id: str | None = "abc123") -> ArtifactMeta:
    return ArtifactMeta(
        artifact_type="raw_volume",
        producer="acquisition",
        run_id="run-1",
        scan_id=scan_id,
        observation_time=SCAN_TIME,
        original_filename="KILX20260813_173158_V06",
        source_uri="s3://noaa-nexrad-level2/KILX20260813_173158_V06",
    )


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


def _visible_objects(collection) -> set[str]:
    return {p.name for p in collection.objects_dir.iterdir() if p.name != ".staging"}


class TestBegin:
    def test_allocates_uuid_before_any_bytes_exist(self, objects, collection):
        handle = objects.begin(suffix=".bin")

        assert len(handle.artifact_id) == 32
        int(handle.artifact_id, 16)  # uuid4 hex
        assert handle.staging_path.parent == collection.objects_dir / ".staging"
        assert _visible_objects(collection) == set()

    def test_staged_bytes_are_invisible_and_uncataloged(self, objects, collection):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(PAYLOAD)

        assert _visible_objects(collection) == set()
        assert collection.catalog.get_artifact(handle.artifact_id) is None


class TestCommit:
    def test_commit_publishes_object_and_full_catalog_row(self, objects, collection):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(PAYLOAD)

        record = objects.commit(handle, _meta())

        object_name = f"{handle.artifact_id}.bin"
        assert _visible_objects(collection) == {object_name}
        assert (collection.objects_dir / object_name).read_bytes() == PAYLOAD

        row = collection.catalog.get_artifact(handle.artifact_id)
        assert row is not None
        assert row["artifact_type"] == "raw_volume"
        assert row["producer"] == "acquisition"
        assert row["run_id"] == "run-1"
        assert row["scan_id"] == "abc123"
        assert row["observation_time"] == "2026-08-13T17:31:58Z"
        assert row["original_filename"] == "KILX20260813_173158_V06"
        assert row["source_uri"].startswith("s3://")
        assert row["object_name"] == object_name
        assert row["checksum_sha256"] == hashlib.sha256(PAYLOAD).hexdigest()
        assert row["size_bytes"] == len(PAYLOAD)
        assert record.artifact_id == handle.artifact_id

    def test_commit_clears_staging(self, objects, collection):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(PAYLOAD)

        objects.commit(handle, _meta())

        assert not handle.staging_path.exists()

    def test_commit_twice_raises(self, objects):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(PAYLOAD)
        objects.commit(handle, _meta())

        with pytest.raises(StoreError, match="staged"):
            objects.commit(handle, _meta())

    def test_commit_without_staged_bytes_raises(self, objects):
        handle = objects.begin(suffix=".bin")

        with pytest.raises(StoreError, match="staged"):
            objects.commit(handle, _meta())

    def test_commit_onto_existing_object_raises(self, objects, collection):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(PAYLOAD)
        (collection.objects_dir / f"{handle.artifact_id}.bin").write_bytes(b"squatter")

        with pytest.raises(StoreError, match="exists"):
            objects.commit(handle, _meta())


class TestAbort:
    def test_abort_leaves_no_trace(self, objects, collection):
        handle = objects.begin(suffix=".bin")
        handle.staging_path.write_bytes(PAYLOAD)

        objects.abort(handle)

        assert not handle.staging_path.exists()
        assert _visible_objects(collection) == set()
        assert collection.catalog.get_artifact(handle.artifact_id) is None

    def test_abort_before_write_is_clean(self, objects, collection):
        handle = objects.begin(suffix=".bin")

        objects.abort(handle)

        assert _visible_objects(collection) == set()
