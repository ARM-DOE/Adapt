# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Native NEXRAD Level-II archive access over unsigned S3.

Replaces the third-party ``nexradaws`` library. ``NexradS3`` exposes the exact
surface the acquisition module consumes (``get_avail_scans_in_range`` /
``get_avail_radars`` / ``download``) so it is a drop-in ``conn`` backend.

Bucket layout: ``unidata-nexrad-level2`` with keys ``YYYY/MM/DD/RADAR/<file>``.
Filenames carry the scan time, e.g. ``KTLX20130520_180000_V06[.gz]``.
"""

import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from . import s3
from .models import ArchiveScan, DownloadResult, LocalScan

__all__ = ["NexradS3", "build_prefix", "parse_scan_time"]

BUCKET = "unidata-nexrad-level2"

# YYYYMMDD_HHMMSS embedded in every NEXRAD volume filename.
_TIMESTAMP_RE = re.compile(r"(\d{8})_(\d{6})")


def build_prefix(radar: str, day: date) -> str:
    """Return the S3 key prefix for one radar on one UTC day."""
    return f"{day:%Y/%m/%d}/{radar}/"


def parse_scan_time(filename: str) -> datetime:
    """Parse the UTC scan time from a NEXRAD volume filename."""
    match = _TIMESTAMP_RE.search(filename)
    if match is None:
        raise ValueError(f"no scan timestamp in filename: {filename!r}")
    stamp = f"{match.group(1)}_{match.group(2)}"
    return datetime.strptime(stamp, "%Y%m%d_%H%M%S").replace(tzinfo=UTC)


def _is_volume_file(key: str) -> bool:
    """True for real volume scans; excludes ``_MDM`` and non-volume sidecars."""
    name = key.rsplit("/", 1)[-1]
    if "_MDM" in name:
        return False
    if _TIMESTAMP_RE.search(name) is None:
        return False
    return name.endswith(".gz") or "_V0" in name


def _to_utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC, treating a naive value as already UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class NexradS3:
    """NEXRAD Level-II archive client over an anonymous S3 connection."""

    def __init__(self, client=None):
        """Use ``client`` if given (for testing), else one unsigned S3 client."""
        self._client = client or s3.client()

    def get_avail_scans_in_range(
        self, start: datetime, end: datetime, radar: str
    ) -> list[ArchiveScan]:
        """Return volume scans for ``radar`` with scan time in ``[start, end]``.

        A future ``start`` raises ``ValueError``; a future ``end`` is clamped to
        now (so the day loop never lists non-existent future days). Missing data
        yields an empty list. Results are sorted by scan time.
        """
        start = _to_utc(start)
        end = _to_utc(end)
        now = datetime.now(UTC)
        if start > now:
            raise ValueError(f"start time {start.isoformat()} is in the future")
        end = min(end, now)
        scans: list[ArchiveScan] = []
        day = start.date()
        while day <= end.date():
            for entry in s3.list_objects(self._client, BUCKET, build_prefix(radar, day)):
                key = entry["Key"]
                if not _is_volume_file(key):
                    continue
                scan_time = parse_scan_time(key.rsplit("/", 1)[-1])
                if start <= scan_time <= end:
                    scans.append(ArchiveScan(key=key, scan_time=scan_time, size=entry["Size"]))
            day += timedelta(days=1)
        scans.sort(key=lambda scan: scan.scan_time)
        return scans

    def get_avail_radars(self, y: str, m: str, d: str) -> list[str]:
        """Return the radar IDs with data for the given UTC year/month/day."""
        prefix = f"{y}/{m}/{d}/"
        return [
            cp.rstrip("/").rsplit("/", 1)[-1]
            for cp in s3.list_common_prefixes(self._client, BUCKET, prefix)
        ]

    def download(
        self, scans: list[ArchiveScan], target_dir: Path, keep_aws_folders: bool = False
    ) -> DownloadResult:
        """Download each scan into ``target_dir``, verifying byte size.

        Failures are collected in ``DownloadResult.failed`` so the caller can
        re-submit them; the batch is not aborted.
        """
        if keep_aws_folders:
            raise NotImplementedError("keep_aws_folders=True is not supported")
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        result = DownloadResult()
        for scan in scans:
            dest = target_dir / Path(scan.key).name
            try:
                s3.download_object(self._client, BUCKET, scan.key, dest, scan.size)
                result.success.append(LocalScan(filepath=dest))
            except Exception:
                result.failed.append(scan)
        return result
