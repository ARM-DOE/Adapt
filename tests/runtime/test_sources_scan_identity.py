# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Sources mint scan identity at the boundary.

Every queue message carries the content-derived scan_id (and the source's
scan_time when the source can supply one). Identity is minted exactly once,
where the raw file first enters the system — consumers downstream never
re-derive it.
"""

import time
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from types import SimpleNamespace

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader
from adapt.runtime.sources import LocalDirectorySource
from adapt.utils.identity import scan_id_from_bytes

pytestmark = pytest.mark.unit


def _write_volume(directory: Path, name: str, payload: bytes) -> Path:
    path = directory / name
    path.write_bytes(payload)
    return path


def _local_source(source_dir: Path) -> LocalDirectorySource:
    config = SimpleNamespace(
        source_dir=str(source_dir),
        downloader=SimpleNamespace(min_file_size=1),
    )
    return LocalDirectorySource(config, result_queue=Queue())


def test_local_source_queues_scan_id_and_scan_time(tmp_path):
    payload = b"level2-volume-bytes"
    path = _write_volume(tmp_path, "KLOT20240518_123456_V06", payload)

    source = _local_source(tmp_path)
    source.run()

    msg = source._result_queue.get_nowait()
    assert msg["path"] == str(path)
    assert msg["scan_id"] == scan_id_from_bytes(payload)
    assert msg["scan_time"] == datetime(2024, 5, 18, 12, 34, 56, tzinfo=UTC)
    assert "queued_at" in msg


def test_local_source_without_filename_stamp_queues_none_scan_time(tmp_path):
    # Identity comes from content, so a stampless file still gets a scan_id;
    # the missing scan_time fails loudly later, at ingest — not silently here.
    payload = b"stampless-volume-bytes"
    _write_volume(tmp_path, "opaque_volume.raw", payload)

    source = _local_source(tmp_path)
    source.run()

    msg = source._result_queue.get_nowait()
    assert msg["scan_id"] == scan_id_from_bytes(payload)
    assert msg["scan_time"] is None


def test_notify_queue_includes_content_scan_id(tmp_path, make_config):
    q = Queue()
    d = AwsNexradDownloader(make_config(), output_dir=tmp_path, result_queue=q)
    payload = b"downloaded-volume-bytes"
    path = _write_volume(tmp_path, "KLOT20240518_123456_V06", payload)

    d._notify_queue(path=path, scan_time=datetime(2024, 5, 18, 12, 34, 56, tzinfo=UTC), is_new=True)

    msg = q.get_nowait()
    assert msg["scan_id"] == scan_id_from_bytes(payload)
    assert msg["queued_at"] <= time.time()
