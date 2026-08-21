# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Sources acquire raw scans into the store at the boundary.

Every queue message comes from the acquisition gateway: the raw bytes become a
cataloged object, the scan is registered for the run, and the message carries
the content-derived scan_id. Identity is minted exactly once, where the raw
file first enters the system — consumers downstream never re-derive it.
"""

import time
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from types import SimpleNamespace

import pytest

from adapt.runtime.sources import LocalDirectorySource
from adapt.utils.identity import scan_id_from_bytes

pytestmark = pytest.mark.unit


def _write_volume(directory: Path, name: str, payload: bytes) -> Path:
    path = directory / name
    path.write_bytes(payload)
    return path


def _local_source(source_dir: Path, acquire) -> LocalDirectorySource:
    config = SimpleNamespace(
        source_dir=str(source_dir),
        downloader=SimpleNamespace(min_file_size=1),
    )
    return LocalDirectorySource(config, result_queue=Queue(), acquire=acquire)


def test_local_source_acquires_and_queues_identity(tmp_path, store_env):
    payload = b"level2-volume-bytes"
    src_dir = tmp_path / "incoming"
    src_dir.mkdir()
    path = _write_volume(src_dir, "KLOT20240518_123456_V06", payload)

    source = _local_source(src_dir, store_env.acquirer)
    source.run()

    msg = source._result_queue.get_nowait()
    assert msg["scan_id"] == scan_id_from_bytes(payload)
    assert msg["scan_time"] == datetime(2024, 5, 18, 12, 34, 56, tzinfo=UTC)
    assert msg["queued_at"] <= time.time()

    # The raw bytes are now a cataloged store object and the scan is registered.
    raw = store_env.collection.catalog.get_artifact(msg["artifact_id"])
    assert raw["artifact_type"] == "raw_volume"
    assert raw["source_uri"] == str(path)
    assert store_env.collection.catalog.get_scan(store_env.run_id, msg["scan_id"]) is not None


def test_local_source_without_filename_stamp_fails_loudly(tmp_path, store_env):
    # The store cannot register a scan without its observation time; a
    # stampless file fails at the boundary instead of poisoning the run.
    src_dir = tmp_path / "incoming"
    src_dir.mkdir()
    _write_volume(src_dir, "opaque_volume.raw", b"stampless-volume-bytes")

    source = _local_source(src_dir, store_env.acquirer)

    with pytest.raises(ValueError):
        source.run()


def test_local_source_requires_gateway(tmp_path):
    config = SimpleNamespace(source_dir=str(tmp_path), downloader=SimpleNamespace(min_file_size=1))

    with pytest.raises(ValueError, match="gateway"):
        LocalDirectorySource(config, result_queue=Queue())
