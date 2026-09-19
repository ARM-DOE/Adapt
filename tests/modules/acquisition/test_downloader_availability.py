"""Test radar availability warning behavior.

Verifies that availability warnings are only logged when the check explicitly
succeeds and finds no radar (not when the check fails/times out).
"""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from adapt.modules.acquisition.module import AwsNexradDownloader

pytestmark = pytest.mark.unit


def test_availability_check_warns_when_radar_explicitly_not_found(caplog, fake_gateway):
    """Warn when availability check succeeds but radar not found."""
    config = MagicMock()
    config.downloader.radar = "KOHX"
    config.downloader.poll_interval_sec = 1
    config.downloader.latest_files = 5
    config.downloader.latest_minutes = 60
    config.downloader.start_time = None
    config.downloader.end_time = None
    config.downloader.min_file_size = 1024

    fake_conn = MagicMock()
    fake_conn.get_avail_radars.return_value = ["KDFW", "KLBB"]  # KOHX not in list

    downloader = AwsNexradDownloader(
        config=config,
        acquire=fake_gateway,
        conn=fake_conn,
    )

    start = datetime(2025, 12, 18, tzinfo=UTC)
    end = datetime(2025, 12, 18, tzinfo=UTC)

    with caplog.at_level(logging.WARNING):
        downloader._check_radar_available(start, end)

    # Should warn because check succeeded but radar not found
    assert any("Radar KOHX not found in AWS" in record.message for record in caplog.records)


def test_availability_check_does_not_warn_when_check_fails(caplog, fake_gateway):
    """Do NOT warn when availability check fails (exception or all failures)."""
    config = MagicMock()
    config.downloader.radar = "KOHX"
    config.downloader.poll_interval_sec = 1
    config.downloader.latest_files = 5
    config.downloader.latest_minutes = 60
    config.downloader.start_time = None
    config.downloader.end_time = None
    config.downloader.min_file_size = 1024

    fake_conn = MagicMock()
    fake_conn.get_avail_radars.side_effect = Exception("AWS unavailable")

    downloader = AwsNexradDownloader(
        config=config,
        acquire=fake_gateway,
        conn=fake_conn,
    )

    start = datetime(2025, 12, 18, tzinfo=UTC)
    end = datetime(2025, 12, 18, tzinfo=UTC)

    with caplog.at_level(logging.WARNING):
        downloader._check_radar_available(start, end)

    # Should NOT warn because check failed (exception)
    assert not any("Radar KOHX not found in AWS" in record.message for record in caplog.records)


def test_availability_check_does_not_warn_when_radar_found(caplog, fake_gateway):
    """Do NOT warn when radar is found in availability check."""
    config = MagicMock()
    config.downloader.radar = "KOHX"
    config.downloader.poll_interval_sec = 1
    config.downloader.latest_files = 5
    config.downloader.latest_minutes = 60
    config.downloader.start_time = None
    config.downloader.end_time = None
    config.downloader.min_file_size = 1024

    fake_conn = MagicMock()
    fake_conn.get_avail_radars.return_value = [
        "KDFW",
        "KOHX",
        "KLBB",
    ]  # KOHX IS in list

    downloader = AwsNexradDownloader(
        config=config,
        acquire=fake_gateway,
        conn=fake_conn,
    )

    start = datetime(2025, 12, 18, tzinfo=UTC)
    end = datetime(2025, 12, 18, tzinfo=UTC)

    with caplog.at_level(logging.WARNING):
        downloader._check_radar_available(start, end)

    # Should NOT warn because radar was found
    assert not any("Radar KOHX not found in AWS" in record.message for record in caplog.records)
