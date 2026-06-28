# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""HistoryLogHandler turns existing warning/error logs into searchable events.

Behaviour under test: a WARNING becomes a WarningEvent and an exception log
becomes an ErrorEvent carrying the bound scan_id/module + the real traceback;
emit only buffers (no DB), and drain returns then clears.
"""

import logging
import random
from datetime import UTC, datetime

from adapt.runtime.history_handler import HistoryLogHandler
from adapt.runtime.observability import ObsSettings, build_observability


def _obs():
    return build_observability(
        ObsSettings(),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def test_handler_captures_warning_and_error_with_context_and_traceback():
    obs = _obs()
    handler = HistoryLogHandler()
    log = logging.getLogger("adapt.test.history_handler")
    log.handlers[:] = [handler]
    log.setLevel(logging.DEBUG)
    log.propagate = False

    with obs.bind(scan_id="scan9", stage="detection"):
        log.warning("slow scan", extra={"category": "slow_execution"})
        try:
            raise ValueError("boom")
        except ValueError:
            log.exception("processing failed")

    warnings, errors = handler.drain()
    assert len(warnings) == 1
    assert warnings[0].scan_id == "scan9"
    assert warnings[0].module == "detection"
    assert warnings[0].category == "slow_execution"

    assert len(errors) == 1
    assert errors[0].scan_id == "scan9"
    assert errors[0].exception_type == "ValueError"
    assert "ValueError: boom" in errors[0].traceback

    assert handler.drain() == ([], [])  # drain clears the buffers


def test_emit_only_buffers_without_db_and_defaults_category():
    handler = HistoryLogHandler()
    record = logging.LogRecord("adapt.x", logging.WARNING, __file__, 1, "plain", None, None)
    handler.emit(record)  # no DB handle exists; pure in-memory append
    warnings, errors = handler.drain()
    assert len(warnings) == 1
    assert warnings[0].category == "general"  # default when none provided
    assert errors == []
