# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Native downloaders for public, unsigned S3 datasets.

Currently provides NEXRAD Level-II via :class:`NexradS3`. The unsigned-S3
mechanics in :mod:`adapt.downloaders.s3` are dataset-agnostic and form the
reuse seam for future datasets.
"""

from .models import ArchiveScan, DownloadError, DownloadResult, LocalScan
from .nexrad import NexradS3

__all__ = ["ArchiveScan", "DownloadError", "DownloadResult", "LocalScan", "NexradS3"]
