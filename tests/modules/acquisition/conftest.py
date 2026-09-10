# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Shared fixtures for the acquisition downloader unit tests."""

import time
from pathlib import Path

import pytest


class FakeAcquisitionGateway:
    """Records gateway calls and returns acquired-scan queue messages.

    ``acquired`` holds source URIs the store already contains; pre-seed it to
    simulate scans acquired by a previous run.
    """

    def __init__(self):
        self.acquired: set[str] = set()
        self.is_acquired_calls: list[str] = []
        self.acquire_file_calls: list[tuple[Path, str]] = []
        self.acquire_existing_calls: list[str] = []
        self.messages: list[dict] = []
        self._count = 0

    def _message(self, source_uri, scan_time) -> dict:
        self._count += 1
        message = {
            "artifact_id": f"art-{self._count}",
            "scan_id": f"sid-{source_uri}",
            "scan_time": scan_time,
            "queued_at": time.time(),
        }
        self.messages.append(message)
        return message

    def is_acquired(self, source_uri) -> bool:
        self.is_acquired_calls.append(source_uri)
        return source_uri in self.acquired

    def acquire_file(self, path, *, source_uri, scan_time) -> dict:
        self.acquire_file_calls.append((Path(path), source_uri))
        self.acquired.add(source_uri)
        return self._message(source_uri, scan_time)

    def acquire_existing(self, source_uri, *, scan_time) -> dict:
        self.acquire_existing_calls.append(source_uri)
        return self._message(source_uri, scan_time)


@pytest.fixture
def fake_gateway():
    return FakeAcquisitionGateway()
