# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Value objects for archived-scan discovery and download.

Pure data — no S3, no I/O. ``ArchiveScan`` describes a remote object,
``LocalScan`` a downloaded one, and ``DownloadResult`` aggregates a batch.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

__all__ = ["ArchiveScan", "DownloadError", "DownloadResult", "LocalScan"]


@dataclass(frozen=True)
class ArchiveScan:
    """A remote scan object in the S3 bucket.

    ``size`` is the S3 ``Size`` captured at listing time so a download can be
    verified without an extra ``head_object`` round-trip.
    """

    key: str
    scan_time: datetime
    size: int


@dataclass(frozen=True)
class LocalScan:
    """A scan that has been downloaded to ``filepath``."""

    filepath: Path


@dataclass
class DownloadResult:
    """The outcome of a download batch: successes and per-file failures."""

    success: list[LocalScan] = field(default_factory=list)
    failed: list[ArchiveScan] = field(default_factory=list)

    def iter_success(self):
        """Iterate the successfully downloaded scans."""
        return iter(self.success)


class DownloadError(Exception):
    """A single object failed to download (carries the offending scan)."""

    def __init__(self, message: str, scan: ArchiveScan | None = None):
        super().__init__(message)
        self.scan = scan
