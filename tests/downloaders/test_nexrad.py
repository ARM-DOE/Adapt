# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""NEXRAD archive logic, mirroring the behaviors the nexradaws suite pinned.

Source spec: nexradaws-master/tests/{test_nexradAwsInterface,test_nexradAwsFile}.py.
The drill-down (years/months/days) and pyart hooks are dropped; everything kept is
re-pinned here against a stubbed client — no network, no fixtures. The strongest
live pins (KTLX 2013-05-20 18-22 -> 53 scans, real download) are in test_integration.
"""

from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from adapt.downloaders.models import ArchiveScan
from adapt.downloaders.nexrad import BUCKET, NexradS3, _to_utc, build_prefix, parse_scan_time

pytestmark = pytest.mark.unit


# ── prefix + filename parsing (mirrors test_prefix_build, test_scan_time) ──────


def test_build_prefix():
    # nexradaws test_prefix_build pins exactly this output.
    assert build_prefix("KTLX", date(2016, 5, 30)) == "2016/05/30/KTLX/"


def test_parse_scan_time_matches_nexradaws_example():
    # nexradaws test_scan_time: KTLX20130531_000358_V06.gz -> 2013-05-31 00:03:58 UTC.
    assert parse_scan_time("KTLX20130531_000358_V06.gz") == datetime(
        2013, 5, 31, 0, 3, 58, tzinfo=UTC
    )


def test_parse_scan_time_2017_era_without_gz():
    # Filename-era drift: 2017+ scans are bare _V06 (no .gz).
    assert parse_scan_time("KTLX20170531_000412_V06") == datetime(2017, 5, 31, 0, 4, 12, tzinfo=UTC)


def test_parse_scan_time_rejects_unparseable():
    with pytest.raises(ValueError):
        parse_scan_time("not-a-scan.txt")


# ── UTC normalization (mirrors test_formattimerange_localtime / _utc) ──────────


def test_to_utc_treats_naive_as_utc():
    naive = datetime(2013, 5, 20, 18, 0)
    assert _to_utc(naive) == datetime(2013, 5, 20, 18, 0, tzinfo=UTC)


def test_to_utc_converts_aware_offset_to_utc():
    central = timezone(timedelta(hours=-5))
    aware = datetime(2013, 5, 20, 18, 0, tzinfo=central)
    assert _to_utc(aware) == datetime(2013, 5, 20, 23, 0, tzinfo=UTC)


# ── range search (mirrors test_get_available_scans_in_range, _missing) ─────────


def _listing_client(contents):
    """A client whose list_objects_v2 paginator returns one page of ``contents``."""
    client = MagicMock()
    client.get_paginator.return_value.paginate.side_effect = lambda **kwargs: iter(
        [{"Contents": contents}]
    )
    return client


def test_get_avail_scans_in_range_filters_sorts_and_excludes_mdm():
    p = "2013/05/20/KTLX/"
    contents = [
        {"Key": f"{p}KTLX20130520_190000_V06.gz", "Size": 12},  # in range
        {"Key": f"{p}KTLX20130520_180000_V06.gz", "Size": 10},  # in range (== start)
        {"Key": f"{p}KTLX20130520_170000_V06.gz", "Size": 11},  # before start
        {"Key": f"{p}KTLX20130520_181500_V06_MDM", "Size": 1},  # MDM sidecar
        {"Key": f"{p}KTLX20130520_index.html", "Size": 2},  # not a volume file
    ]
    store = NexradS3(client=_listing_client(contents))

    scans = store.get_avail_scans_in_range(
        datetime(2013, 5, 20, 18, 0, tzinfo=UTC),
        datetime(2013, 5, 20, 19, 30, tzinfo=UTC),
        "KTLX",
    )

    assert all(isinstance(s, ArchiveScan) for s in scans)
    assert [s.key for s in scans] == [
        f"{p}KTLX20130520_180000_V06.gz",
        f"{p}KTLX20130520_190000_V06.gz",
    ]
    assert [s.size for s in scans] == [10, 12]
    assert scans[0].scan_time == datetime(2013, 5, 20, 18, 0, tzinfo=UTC)


def test_get_avail_scans_in_range_empty_when_no_data():
    # nexradaws test_get_available_scan_missing: missing data -> [] (never raises).
    store = NexradS3(client=_listing_client([]))
    scans = store.get_avail_scans_in_range(
        datetime(2013, 5, 20, 18, 0, tzinfo=UTC),
        datetime(2013, 5, 20, 19, 0, tzinfo=UTC),
        "KTLX",
    )
    assert scans == []


# ── future-date guard (mirrors nexradawsinterface.py:192-197) ──────────────────


def test_future_start_raises():
    store = NexradS3(client=MagicMock())
    start = datetime.now(UTC) + timedelta(days=1)
    with pytest.raises(ValueError):
        store.get_avail_scans_in_range(start, start + timedelta(hours=1), "KTLX")


def test_future_end_is_clamped_so_future_days_are_not_listed():
    # A far-future end must not fire one S3 listing per non-existent future day.
    client = _listing_client([])
    store = NexradS3(client=client)
    store.get_avail_scans_in_range(
        datetime.now(UTC) - timedelta(hours=1),
        datetime.now(UTC) + timedelta(days=30),
        "KTLX",
    )
    assert client.get_paginator.return_value.paginate.call_count <= 2


# ── radar listing (mirrors test_get_available_radars, _missing) ────────────────


def _prefix_client(prefixes):
    client = MagicMock()
    client.get_paginator.return_value.paginate.side_effect = lambda **kwargs: iter(
        [{"CommonPrefixes": [{"Prefix": p} for p in prefixes]}]
    )
    return client


def test_get_avail_radars_parses_common_prefixes():
    store = NexradS3(client=_prefix_client(["2013/05/20/KTLX/", "2013/05/20/KOUN/"]))
    assert store.get_avail_radars("2013", "05", "20") == ["KTLX", "KOUN"]


def test_get_avail_radars_empty_when_missing():
    # nexradaws test_get_available_radars_missing: absent day -> [].
    store = NexradS3(client=_prefix_client([]))
    assert store.get_avail_radars("1900", "05", "31") == []


# ── download (mirrors test_download_single/_multiple, test_failed/_count) ───────


def _download_client(sizes):
    """A client whose download_file writes ``sizes[key]`` bytes to the path."""
    client = MagicMock()
    client.download_file.side_effect = lambda bucket, key, path: Path(path).write_bytes(
        b"x" * sizes[key]
    )
    return client


def test_download_single_writes_file_and_reports_success(tmp_path):
    key = "2013/05/31/KTLX/KTLX20130531_000358_V06.gz"
    scan = ArchiveScan(key=key, scan_time=datetime(2013, 5, 31, 0, 3, 58, tzinfo=UTC), size=5)
    store = NexradS3(client=_download_client({key: 5}))

    result = store.download([scan], tmp_path / "out")

    success = list(result.iter_success())
    assert len(success) == 1
    assert success[0].filepath == tmp_path / "out" / "KTLX20130531_000358_V06.gz"
    assert success[0].filepath.read_bytes() == b"x" * 5
    assert result.failed == []


def test_download_multiple_all_succeed(tmp_path):
    p = "2013/05/31/KTLX/"
    sizes = {
        f"{p}KTLX20130531_000358_V06.gz": 3,
        f"{p}KTLX20130531_000834_V06.gz": 4,
        f"{p}KTLX20130531_001311_V06.gz": 5,
    }
    scans = [
        ArchiveScan(key=k, scan_time=datetime(2013, 5, 31, tzinfo=UTC), size=v)
        for k, v in sizes.items()
    ]
    store = NexradS3(client=_download_client(sizes))

    result = store.download(scans, tmp_path)

    assert len(list(result.iter_success())) == 3
    assert result.failed == []
    for key in sizes:
        assert (tmp_path / Path(key).name).exists()


def test_download_bad_key_is_recorded_as_failure(tmp_path):
    # nexradaws test_failed_count forces a failure by corrupting a key to 'blah/blah'.
    good = "2013/05/31/KTLX/KTLX20130531_000358_V06.gz"
    bad = "blah/blah"
    client = MagicMock()

    def _dl(bucket, key, path):
        if key == bad:
            raise RuntimeError("NoSuchKey")
        Path(path).write_bytes(b"x" * 4)

    client.download_file.side_effect = _dl
    scans = [
        ArchiveScan(key=good, scan_time=datetime(2013, 5, 31, tzinfo=UTC), size=4),
        ArchiveScan(key=bad, scan_time=datetime(2013, 5, 31, tzinfo=UTC), size=4),
    ]
    store = NexradS3(client=client)

    result = store.download(scans, tmp_path)

    assert len(list(result.iter_success())) == 1
    assert [s.key for s in result.failed] == [bad]


def test_download_rejects_keep_aws_folders(tmp_path):
    # We deliberately do not implement the AWS folder structure (acquisition uses flat).
    scan = ArchiveScan(key="k/scan.gz", scan_time=datetime(2013, 5, 20, tzinfo=UTC), size=1)
    store = NexradS3(client=MagicMock())
    with pytest.raises(NotImplementedError):
        store.download([scan], tmp_path, keep_aws_folders=True)


def test_bucket_is_unidata_mirror():
    assert BUCKET == "unidata-nexrad-level2"
