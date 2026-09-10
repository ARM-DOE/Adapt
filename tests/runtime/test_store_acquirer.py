# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreAcquirer: raw scans become cataloged objects at the source boundary."""

import hashlib
from datetime import UTC, datetime

import pytest

from adapt.persistence.errors import StoreError
from adapt.persistence.store import Store, init_store
from adapt.runtime.acquire import StoreAcquirer

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)
PAYLOAD = b"level-two volume bytes"


@pytest.fixture
def collection(tmp_path):
    root = init_store(tmp_path / "store")
    store = Store.open(root)
    yield store.collection("KILX")
    store.close()


@pytest.fixture
def volume(tmp_path):
    path = tmp_path / "KILX20260813_173158_V06"
    path.write_bytes(PAYLOAD)
    return path


def _objects(collection):
    return [p for p in collection.objects_dir.iterdir() if p.name != ".staging"]


class TestAcquireFile:
    def test_commits_object_and_registers_pending_scan(self, collection, volume):
        acquirer = StoreAcquirer(collection, "run-1")

        message = acquirer.acquire_file(volume, source_uri=str(volume), scan_time=SCAN_TIME)

        expected_scan_id = hashlib.sha256(PAYLOAD).hexdigest()[:16]
        assert message["scan_id"] == expected_scan_id
        assert message["scan_time"] == SCAN_TIME
        assert message["queued_at"] > 0

        row = collection.catalog.get_artifact(message["artifact_id"])
        assert row["artifact_type"] == "raw_volume"
        assert row["producer"] == "acquisition"
        assert row["original_filename"] == "KILX20260813_173158_V06"
        assert row["source_uri"] == str(volume)
        assert (collection.objects_dir / row["object_name"]).read_bytes() == PAYLOAD

        scan = collection.catalog.get_scan("run-1", expected_scan_id)
        assert scan is not None
        assert scan["status"] == "pending"
        assert scan["source_file_name"] == "KILX20260813_173158_V06"

    def test_reacquire_same_uri_reuses_object(self, collection, volume):
        acquirer = StoreAcquirer(collection, "run-1")
        first = acquirer.acquire_file(volume, source_uri=str(volume), scan_time=SCAN_TIME)

        second = acquirer.acquire_file(volume, source_uri=str(volume), scan_time=SCAN_TIME)

        assert second["artifact_id"] == first["artifact_id"]
        assert len(_objects(collection)) == 1

    def test_new_run_reuses_object_but_gets_own_scan_row(self, collection, volume):
        first = StoreAcquirer(collection, "run-1").acquire_file(
            volume, source_uri=str(volume), scan_time=SCAN_TIME
        )

        second = StoreAcquirer(collection, "run-2").acquire_file(
            volume, source_uri=str(volume), scan_time=SCAN_TIME
        )

        assert second["artifact_id"] == first["artifact_id"]
        assert len(_objects(collection)) == 1
        assert collection.catalog.get_scan("run-2", first["scan_id"]) is not None

    def test_missing_scan_time_raises_naming_file(self, collection, volume):
        acquirer = StoreAcquirer(collection, "run-1")

        with pytest.raises(StoreError, match="KILX20260813_173158_V06"):
            acquirer.acquire_file(volume, source_uri=str(volume), scan_time=None)


class TestSkipLookups:
    def test_is_acquired_by_source_uri(self, collection, volume):
        acquirer = StoreAcquirer(collection, "run-1")
        assert not acquirer.is_acquired(str(volume))

        acquirer.acquire_file(volume, source_uri=str(volume), scan_time=SCAN_TIME)

        assert acquirer.is_acquired(str(volume))

    def test_acquire_existing_returns_message_without_bytes(self, collection, volume):
        StoreAcquirer(collection, "run-1").acquire_file(
            volume, source_uri=str(volume), scan_time=SCAN_TIME
        )
        volume.unlink()  # bytes only live in the store now

        message = StoreAcquirer(collection, "run-2").acquire_existing(
            str(volume), scan_time=SCAN_TIME
        )

        assert message["scan_id"] == hashlib.sha256(PAYLOAD).hexdigest()[:16]
        assert collection.catalog.get_scan("run-2", message["scan_id"]) is not None

    def test_acquire_existing_unknown_uri_raises(self, collection):
        with pytest.raises(StoreError, match="nowhere"):
            StoreAcquirer(collection, "run-1").acquire_existing("s3://nowhere", scan_time=SCAN_TIME)
