# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The single logging-configuration site for Adapt.

Replaces the per-orchestrator handler setup and the CLI ``basicConfig``. Installs
a ``ContextFilter`` that stamps the current correlation ids onto every record, a
``JsonFormatter`` for the full file log, and a ``ConsoleFilter`` so the console
stays quiet (warnings/errors + explicitly console-tagged lines only) while the
file/JSON log keeps everything. Scientific modules keep ``getLogger(__name__)``
and never import this — they gain context automatically.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from adapt.runtime.observability import ObsSettings, current_context

if TYPE_CHECKING:
    from adapt.runtime.console_status import ConsoleStatus

__all__ = [
    "ContextFilter",
    "JsonFormatter",
    "ConsoleFilter",
    "StatusAwareStreamHandler",
    "configure_logging",
    "shutdown_logging",
]

# Human-readable line format, shared by the console and the plain-text file log.
_TEXT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# Standard LogRecord attributes to exclude when surfacing structured extras.
_STD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}
_CONTEXT_FIELDS = (
    "pipeline_id",
    "trace_id",
    "span_id",
    "scan_id",
    "dataset_id",
    "experiment_id",
    "worker_id",
    "stage",
)


class ContextFilter(logging.Filter):
    """Stamp the current ObsContext onto every record so modules never repeat ids."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = current_context()
        for field in _CONTEXT_FIELDS:
            setattr(record, field, getattr(ctx, field))
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per record: standard fields + context ids + any extra=."""

    def format(self, record: logging.LogRecord) -> str:
        # NB: the trailing-Z scan-time literal is fitness-pinned to utils/time; keep this no-Z form.
        payload: dict[str, object] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ConsoleFilter(logging.Filter):
    """Pass only what a human needs to watch: records at/above the console threshold,
    plus explicitly console-tagged lines (the run header, progress, and summary).

    The threshold is carried here, not on the handler's level: a handler's level is
    checked *before* its filters, so an INFO threshold-bypassing console-tagged line
    would be dropped by a WARNING handler level before this filter ever ran.
    """

    def __init__(self, level: int = logging.WARNING) -> None:
        super().__init__()
        self._level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= self._level or bool(getattr(record, "console", False))


class StatusAwareStreamHandler(logging.StreamHandler):
    """Console handler that erases the transient status line before each record.

    Holds the ConsoleStatus lock across clear + emit so the orchestrator's ticker
    can't redraw the status line in between; the ticker repaints on its next tick.
    """

    def __init__(self, status: ConsoleStatus) -> None:
        super().__init__(status.stream)
        self._status = status

    def emit(self, record: logging.LogRecord) -> None:
        with self._status.lock:
            self._status.clear()
            super().emit(record)


def shutdown_logging() -> None:
    """Detach and close every root handler. The counterpart to configure_logging.

    Removing a handler does not release what it holds: a FileHandler keeps its
    log file open until closed. Left unclosed, every reconfigure leaks a file
    descriptor for the process's lifetime, and on Windows the open handle also
    makes the log file undeletable. Closing a StreamHandler is safe — it flushes
    but never closes the underlying stream, so sys.stderr survives.
    """
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()


def configure_logging(
    settings: ObsSettings,
    log_path: Path | None,
    *,
    console_status: ConsoleStatus | None = None,
) -> None:
    """Configure the root logger. The one place handlers are constructed.

    Fails loudly: ``json_logs`` with no ``log_path`` raises (no silent default).
    Idempotent — releases existing handlers before re-adding, so repeated calls
    (or a prior ``basicConfig``) never accumulate handlers or leak their files.
    """
    shutdown_logging()
    root = logging.getLogger()
    root.setLevel(getattr(logging, settings.level.upper(), logging.INFO))

    context_filter = ContextFilter()

    if log_path is None:
        if settings.json_logs:
            raise ValueError("json_logs=True requires a log_path")
    else:
        # A log_path always gets a file handler; json_logs only selects the
        # format. Gating the handler itself on json_logs would leave the default
        # configuration writing the whole run to the console and nothing to disk.
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(
            JsonFormatter() if settings.json_logs else logging.Formatter(_TEXT_FORMAT)
        )
        file_handler.addFilter(context_filter)
        root.addHandler(file_handler)

    if settings.console_logs:
        # No handler-level gate: ConsoleFilter does all gating (see its docstring), so
        # console-tagged INFO lines survive a WARNING console threshold. With a
        # console_status, use the status-aware handler so each permanent line erases
        # the transient spinner line first.
        console_threshold = getattr(logging, settings.console_level.upper(), logging.WARNING)
        console: logging.StreamHandler = (
            StatusAwareStreamHandler(console_status)
            if console_status is not None
            else logging.StreamHandler()
        )
        console.setFormatter(logging.Formatter(_TEXT_FORMAT))
        console.addFilter(context_filter)
        console.addFilter(ConsoleFilter(console_threshold))
        root.addHandler(console)
