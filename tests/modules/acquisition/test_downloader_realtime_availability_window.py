"""Realtime downloader should check availability over the whole lookback window.

This prevents spammy false warnings around UTC midnight when the lookback window
spans two dates (yesterday inventory has radar, today may not yet).
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def test_realtime_availability_check_uses_start_and_end_dates(fake_gateway):
    config = MagicMock()
    config.downloader.mode = "realtime"
    config.downloader.radar = "KPOE"
    config.downloader.poll_interval_sec = 1
    config.downloader.latest_files = 5
    config.downloader.latest_minutes = 60
    config.downloader.start_time = None
    config.downloader.end_time = None
    config.downloader.min_file_size = 1024

    fake_conn = MagicMock()
    fake_conn.get_avail_scans_in_range.return_value = []
    fake_conn.get_avail_radars.return_value = ["KPOE"]

    # 2026-04-05 00:10Z lookback spans previous date.
    now = datetime(2026, 4, 5, 0, 10, 0, tzinfo=UTC)

    downloader = AwsNexradDownloader(
        config=config,
        acquire=fake_gateway,
        conn=fake_conn,
        clock=lambda: now,
    )

    captured = {}

    def _capture(start, end):
        captured["start"] = start
        captured["end"] = end
        return AwsNexradDownloader._check_radar_available(downloader, start, end)

    downloader._check_radar_available = _capture

    downloader._download_realtime()

    assert captured["end"] == now
    assert captured["start"] == now - timedelta(minutes=60)
