# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Unsigned-S3 primitives: pagination and verified atomic download.

All tests use a stubbed client (unittest.mock) — no network, no fixtures.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from adapt.downloaders import s3
from adapt.downloaders.models import DownloadError

pytestmark = pytest.mark.unit


def _paginating_client(pages):
    """A client whose list_objects_v2 paginator yields ``pages`` (fresh each call)."""
    client = MagicMock()
    client.get_paginator.return_value.paginate.side_effect = lambda **kwargs: iter(pages)
    return client


def test_list_objects_spans_multiple_pages():
    pages = [
        {"Contents": [{"Key": "a", "Size": 1}, {"Key": "b", "Size": 2}]},
        {"Contents": [{"Key": "c", "Size": 3}]},
    ]
    client = _paginating_client(pages)

    entries = list(s3.list_objects(client, "bucket", "prefix/"))

    assert [e["Key"] for e in entries] == ["a", "b", "c"]
    client.get_paginator.assert_called_once_with("list_objects_v2")
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket", Prefix="prefix/"
    )


def test_list_objects_handles_empty_page():
    client = _paginating_client([{}])
    assert list(s3.list_objects(client, "bucket", "prefix/")) == []


def test_list_common_prefixes_uses_delimiter():
    pages = [{"CommonPrefixes": [{"Prefix": "2013/05/20/KTLX/"}, {"Prefix": "2013/05/20/KOUN/"}]}]
    client = _paginating_client(pages)

    prefixes = list(s3.list_common_prefixes(client, "bucket", "2013/05/20/"))

    assert prefixes == ["2013/05/20/KTLX/", "2013/05/20/KOUN/"]
    client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="bucket", Prefix="2013/05/20/", Delimiter="/"
    )


def _writing_client(content: bytes):
    """A client whose download_file writes ``content`` to the requested path."""
    client = MagicMock()
    client.download_file.side_effect = lambda bucket, key, path: Path(path).write_bytes(content)
    return client


def test_download_object_verifies_size_and_renames_atomically(tmp_path):
    dest = tmp_path / "KTLX20130520_180000_V06.gz"
    client = _writing_client(b"x" * 100)

    s3.download_object(client, "bucket", "key", dest, 100)

    assert dest.read_bytes() == b"x" * 100
    assert not dest.with_name(dest.name + ".part").exists()


def test_download_object_size_mismatch_raises_and_cleans_up(tmp_path):
    dest = tmp_path / "scan.gz"
    client = _writing_client(b"x" * 50)

    with pytest.raises(DownloadError):
        s3.download_object(client, "bucket", "key", dest, 100)

    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()


def test_download_object_transient_failure_cleans_up_part(tmp_path):
    dest = tmp_path / "scan.gz"
    client = MagicMock()
    client.download_file.side_effect = RuntimeError("network drop")

    with pytest.raises(RuntimeError):
        s3.download_object(client, "bucket", "key", dest, 100)

    assert not dest.exists()
    assert not dest.with_name(dest.name + ".part").exists()
