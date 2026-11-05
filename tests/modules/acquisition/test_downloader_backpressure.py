# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The downloader pauses at the queue's high-water mark and resumes at the low one.

Only the downloader waits — the processor is never paused. The two levels are
apart so a full queue does not degrade into one download per consumed scan.
"""

from queue import Queue

import pytest

from adapt.configuration.schemas.user import UserDownloaderConfig
from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def _downloader(make_config, q, fake_gateway, fake_aws_conn, scans, high=10, frac=0.3):
    cfg = make_config(
        downloader=UserDownloaderConfig(max_queue_size=high, queue_resume_fraction=frac)
    )
    return AwsNexradDownloader(
        cfg, result_queue=q, acquire=fake_gateway, conn=fake_aws_conn(scans), sleeper=lambda _: None
    )


def test_levels_come_from_config(make_config, fake_gateway, fake_aws_conn):
    d = _downloader(make_config, Queue(), fake_gateway, fake_aws_conn, [], high=100, frac=0.1)
    assert d._queue_high == 100
    assert d._queue_low == 10


def test_low_water_is_at_least_one(make_config, fake_gateway, fake_aws_conn):
    d = _downloader(make_config, Queue(), fake_gateway, fake_aws_conn, [], high=3, frac=0.1)
    assert d._queue_low == 1


def test_no_pause_below_high_water(make_config, fake_gateway, fake_aws_conn, fake_scan):
    q = Queue(maxsize=10)
    scans = [fake_scan(f"s{i}") for i in range(5)]
    d = _downloader(make_config, q, fake_gateway, fake_aws_conn, scans, high=10)

    d._process_scans(scans)

    assert q.qsize() == 5
    assert d.queue_backpressure_stats()["pauses"] == 0


def test_pauses_at_high_water_and_resumes_at_low(
    make_config, fake_gateway, fake_aws_conn, fake_scan
):
    """With the queue full, downloading waits until a consumer drains it to `low`."""
    q = Queue(maxsize=4)
    scans = [fake_scan(f"s{i}") for i in range(6)]
    d = _downloader(make_config, q, fake_gateway, fake_aws_conn, scans, high=4, frac=0.5)
    assert d._queue_low == 2

    # A consumer that removes one item per poll, recording the queue depth it saw.
    seen: list[int] = []

    def consume_one(_seconds):
        seen.append(q.qsize())
        q.get_nowait()

    d._sleep = consume_one
    d._process_scans(scans)

    # 4 downloaded before the first pause; the pause released once qsize <= 2
    # (two polls), then the remaining 2 were downloaded without a second pause.
    stats = d.queue_backpressure_stats()
    assert stats["pauses"] == 1
    assert seen == [4, 3]
    assert len(fake_gateway.acquire_file_calls) == 6
    assert q.qsize() == 4  # 6 put, 2 consumed


def test_stop_request_releases_a_paused_downloader(
    make_config, fake_gateway, fake_aws_conn, fake_scan
):
    q = Queue(maxsize=2)
    scans = [fake_scan(f"s{i}") for i in range(5)]
    d = _downloader(make_config, q, fake_gateway, fake_aws_conn, scans, high=2, frac=0.5)

    polls = []

    def stop_on_first_poll(_seconds):
        polls.append(q.qsize())
        d.stop()

    d._sleep = stop_on_first_poll
    d._process_scans(scans)

    assert polls == [2]
    assert len(fake_gateway.acquire_file_calls) == 2  # nothing downloaded while stopped
