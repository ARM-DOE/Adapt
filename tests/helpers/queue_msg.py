# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Processor queue-message builder for tests.

The processor rejects bare path strings: every message carries the scan_id
minted at the source boundary plus the source-parsed scan_time.
"""

import time
from datetime import UTC, datetime
from pathlib import Path

_DEFAULT_SCAN_TIME = datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC)


def msg(path: str, scan_id: str | None = None, scan_time: datetime | None = _DEFAULT_SCAN_TIME):
    """Queue message for *path*; scan_id defaults to a stem-derived test id."""
    return {
        "path": path,
        "scan_id": scan_id or f"sid-{Path(path).stem}",
        "scan_time": scan_time,
        "queued_at": time.time(),
    }
