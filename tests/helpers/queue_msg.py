# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Acquired-scan queue-message builder for tests.

The processor resolves the raw object through the store, so a test message
must come from a real acquisition: this helper writes a synthetic raw file
and commits it through the run's acquisition gateway.
"""

from datetime import UTC, datetime
from pathlib import Path

_DEFAULT_SCAN_TIME = datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC)


def msg(
    store_env,
    tmp_path,
    name: str,
    scan_time: datetime | None = _DEFAULT_SCAN_TIME,
    payload: bytes | None = None,
) -> dict:
    """Acquire a synthetic raw file named *name*; returns the queue message."""
    filename = Path(name).name
    path = Path(tmp_path) / filename
    path.write_bytes(payload if payload is not None else filename.encode())
    return store_env.acquirer.acquire_file(path, source_uri=str(path), scan_time=scan_time)
