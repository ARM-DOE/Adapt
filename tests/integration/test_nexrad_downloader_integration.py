# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Live-AWS parity for the native NEXRAD downloader (excluded from default CI).

Mirrors the strongest nexradaws pins against the public unidata-nexrad-level2
bucket: the exact 53-scan range count (test_get_available_scans_in_range) and a
real, size-verified single download (test_download_single).
"""

from datetime import UTC, datetime

import pytest

from adapt.downloaders import ArchiveScan, NexradS3

pytestmark = pytest.mark.integration


def test_range_search_returns_53_scans_for_ktlx_2013_05_20():
    conn = NexradS3()
    scans = conn.get_avail_scans_in_range(
        datetime(2013, 5, 20, 18, 0, tzinfo=UTC),
        datetime(2013, 5, 20, 22, 0, tzinfo=UTC),
        "KTLX",
    )
    assert len(scans) == 53
    assert all(isinstance(s, ArchiveScan) for s in scans)
    assert scans == sorted(scans, key=lambda s: s.scan_time)


def test_download_single_writes_a_size_verified_file(tmp_path):
    conn = NexradS3()
    scans = conn.get_avail_scans_in_range(
        datetime(2013, 5, 20, 18, 0, tzinfo=UTC),
        datetime(2013, 5, 20, 18, 30, tzinfo=UTC),
        "KTLX",
    )
    assert scans, "expected at least one scan in the half-hour window"

    result = conn.download(scans[:1], tmp_path)

    success = list(result.iter_success())
    assert result.failed == []
    assert len(success) == 1
    downloaded = success[0].filepath
    assert downloaded.exists()
    assert downloaded.stat().st_size == scans[0].size
