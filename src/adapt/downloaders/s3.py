# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unsigned (anonymous) S3 I/O — the only module that imports boto3.

These primitives know nothing about NEXRAD: they operate on plain bucket/key
strings. They are the reuse seam for any other public, unsigned S3 dataset
(e.g. GOES `noaa-goes16..19`, MRMS `noaa-mrms-pds`): a future dataset module
supplies its own bucket, key-prefix builder, filename parser and key filter,
and reuses ``client`` / ``list_objects`` / ``list_common_prefixes`` /
``download_object`` unchanged. No such dataset exists yet, so none is built.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from .models import DownloadError

__all__ = ["client", "download_object", "list_common_prefixes", "list_objects"]


def client():
    """Return one anonymous (unsigned) S3 client, safe to share across threads."""
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def _pages(s3_client, bucket: str, prefix: str, delimiter: str | None):
    """Yield every page of a paginated ``list_objects_v2`` call."""
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    if delimiter is not None:
        kwargs["Delimiter"] = delimiter
    yield from s3_client.get_paginator("list_objects_v2").paginate(**kwargs)


def list_objects(s3_client, bucket: str, prefix: str) -> Iterator[dict]:
    """Yield each object entry (``Key``, ``Size``, ``LastModified``) under ``prefix``."""
    for page in _pages(s3_client, bucket, prefix, delimiter=None):
        yield from page.get("Contents", [])


def list_common_prefixes(s3_client, bucket: str, prefix: str) -> Iterator[str]:
    """Yield each immediate sub-prefix (folder) under ``prefix``."""
    for page in _pages(s3_client, bucket, prefix, delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            yield entry["Prefix"]


def download_object(s3_client, bucket: str, key: str, dest_path: Path, expected_size: int) -> None:
    """Download ``key`` to ``dest_path``, verifying byte size, atomically.

    Writes to ``dest_path.part``, asserts the on-disk size equals
    ``expected_size``, then ``os.replace`` to the final path (atomic on POSIX).
    Any failure removes the partial file and raises, so a partial download never
    occupies the real path.
    """
    part = dest_path.with_name(dest_path.name + ".part")
    ok = False
    try:
        s3_client.download_file(bucket, key, str(part))
        actual = part.stat().st_size
        if actual != expected_size:
            raise DownloadError(
                f"size mismatch for {key}: expected {expected_size} bytes, got {actual}"
            )
        os.replace(part, dest_path)
        ok = True
    finally:
        if not ok:
            part.unlink(missing_ok=True)
