# tests/test_downloader_realtime.py
from datetime import UTC, datetime
from queue import Queue

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def _count_downloads(conn, calls):
    """Wrap conn.download to record the scan keys of each download call."""
    original = conn.download

    def counting_download(scans, *a, **k):
        calls.append([s.key for s in scans])
        return original(scans, *a, **k)

    conn.download = counting_download


def test_realtime_download_hybrid(fake_scan, fake_aws_conn, fake_gateway, make_config):
    """Scans already in the store are acquired without downloading; new ones download."""
    now = datetime(2024, 1, 1, tzinfo=UTC)

    scans = [
        fake_scan("scan1", now),
        fake_scan("scan2", now),
    ]

    q = Queue()
    conn = fake_aws_conn(scans)
    download_calls: list = []
    _count_downloads(conn, download_calls)

    fake_gateway.acquired.add("scan1")  # already committed by a previous run

    config = make_config(
        radar_id="KDIX",
        latest_n=2,
        minutes=30,
    )

    d = AwsNexradDownloader(
        config,
        result_queue=q,
        acquire=fake_gateway,
        conn=conn,
        clock=lambda: now,
        sleeper=lambda _: None,
    )

    downloads = d._download_realtime()

    assert downloads == ["scan2"]  # only the missing scan hit S3
    assert download_calls == [["scan2"]]
    assert fake_gateway.acquire_existing_calls == ["scan1"]
    assert [uri for _, uri in fake_gateway.acquire_file_calls] == ["scan2"]
    assert q.qsize() == 2  # both scans are queued for the processor


def test_realtime_idempotent(fake_scan, fake_aws_conn, fake_gateway, make_config):
    scans = [fake_scan("same")]

    conn = fake_aws_conn(scans)
    download_calls: list = []
    _count_downloads(conn, download_calls)

    config = make_config()
    d = AwsNexradDownloader(
        config,
        acquire=fake_gateway,
        conn=conn,
        sleeper=lambda _: None,
    )

    d._download_realtime()
    d._download_realtime()

    assert len(d._known_files) == 1
    assert download_calls == [["same"]]  # second pass skips the known URI
