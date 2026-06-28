# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Capture WARNING/ERROR log records into structured execution-history events.

Installed on the root logger by the orchestrator. ``emit`` only buffers in
memory (fast, no DB I/O, no reentrancy risk on the hot path); the processor and
orchestrator ``drain`` the buffers and write them via ``ExecutionHistory``. This
turns the existing ~50 ``logger.warning``/``logger.exception`` call sites into
searchable warning/error events with zero new instrumentation in those modules.
"""

from __future__ import annotations

import logging
import threading
from datetime import UTC, datetime

from adapt.contracts.execution_history import ErrorEvent, WarningEvent
from adapt.runtime.observability import current_context

__all__ = ["HistoryLogHandler"]


class HistoryLogHandler(logging.Handler):
    """Buffers WARNING -> WarningEvent and ERROR/CRITICAL -> ErrorEvent."""

    def __init__(self) -> None:
        super().__init__()
        self._lock = threading.Lock()
        self._warnings: list[WarningEvent] = []
        self._errors: list[ErrorEvent] = []

    def emit(self, record: logging.LogRecord) -> None:
        ctx = current_context()
        now = datetime.now(UTC)
        if record.levelno >= logging.ERROR:
            exc_type = ""
            if record.exc_info and record.exc_info[0] is not None:
                exc_type = record.exc_info[0].__name__
            event = ErrorEvent(
                scan_id=ctx.scan_id,
                module=ctx.stage,
                exception_type=exc_type,
                message=record.getMessage(),
                traceback=self._format_traceback(record),
                logger=record.name,
                timestamp=now,
            )
            with self._lock:
                self._errors.append(event)
        elif record.levelno >= logging.WARNING:
            event = WarningEvent(
                scan_id=ctx.scan_id,
                module=ctx.stage,
                category=getattr(record, "category", "general"),
                message=record.getMessage(),
                logger=record.name,
                timestamp=now,
            )
            with self._lock:
                self._warnings.append(event)

    def drain(self) -> tuple[list[WarningEvent], list[ErrorEvent]]:
        """Return buffered warnings + errors, then clear (called per scan / at stop)."""
        with self._lock:
            warnings, errors = self._warnings, self._errors
            self._warnings, self._errors = [], []
            return warnings, errors

    @staticmethod
    def _format_traceback(record: logging.LogRecord) -> str:
        if record.exc_info:
            return logging.Formatter().formatException(record.exc_info)
        return ""
