# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Value-object behavior for the downloaders package."""

import dataclasses
from datetime import UTC, datetime
from pathlib import Path

import pytest

from adapt.downloaders.models import ArchiveScan, DownloadError, DownloadResult, LocalScan

pytestmark = pytest.mark.unit


def test_archive_scan_is_frozen():
    scan = ArchiveScan(key="k", scan_time=datetime(2013, 5, 20, tzinfo=UTC), size=10)
    assert scan.key == "k"
    assert scan.size == 10
    with pytest.raises(dataclasses.FrozenInstanceError):
        scan.size = 11


def test_download_result_iter_success_yields_only_successes():
    good = LocalScan(filepath=Path("/tmp/good"))
    bad = ArchiveScan(key="bad", scan_time=datetime(2013, 5, 20, tzinfo=UTC), size=1)
    result = DownloadResult(success=[good], failed=[bad])

    assert list(result.iter_success()) == [good]
    assert result.failed == [bad]


def test_download_result_defaults_are_independent():
    a = DownloadResult()
    b = DownloadResult()
    a.success.append(LocalScan(filepath=Path("/tmp/x")))
    assert b.success == []


def test_download_error_carries_scan():
    scan = ArchiveScan(key="k", scan_time=datetime(2013, 5, 20, tzinfo=UTC), size=1)
    err = DownloadError("boom", scan)
    assert err.scan is scan
    assert str(err) == "boom"
