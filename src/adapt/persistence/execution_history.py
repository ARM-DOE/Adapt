# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Execution History — a permanent, queryable scientific metadata product.

Records each module execution (run_modules aggregates) and structured
warnings/errors (run_events) against the ONE run lifecycle in the store
registry, so questions like "which runs failed?", "which module got slower?",
"what config produced these outputs?" are answered with SQL, never by parsing
log files. Consumes only ``contracts`` DTOs — never the observability
implementation.
"""

from __future__ import annotations

import json
from datetime import datetime

from adapt.contracts.execution_history import ErrorEvent, RunSummary, WarningEvent
from adapt.contracts.observability import SpanRecord
from adapt.persistence.store_registry import StoreRegistry

__all__ = ["StoreExecutionHistory"]

_SUMMARY_STATUS_TO_RUN = {"success": "completed", "cancelled": "cancelled"}


class StoreExecutionHistory:
    """Execution history on the store registry (runs / run_modules / run_events).

    The ONE run lifecycle lives in registry.runs (begun by the orchestrator via
    ``StoreRegistry.begin_run``); this class records per-module aggregates and
    warning/error events against it and maps the end-of-run summary onto the
    terminal run status.
    """

    def __init__(self, registry: StoreRegistry) -> None:
        self._registry = registry
        self._durations: dict[str, float] = {}
        self._calls: dict[str, int] = {}
        self._failed: set[str] = set()

    def record_modules(
        self, run_id: str, scan_id: str, spans: list[SpanRecord], recorded_at: datetime
    ) -> None:
        """Fold one scan's module spans into the per-module run aggregates."""
        for span in spans:
            self._durations[span.name] = self._durations.get(span.name, 0.0) + span.duration_s
            self._calls[span.name] = self._calls.get(span.name, 0) + 1
            if span.error:
                self._failed.add(span.name)
            self._registry.record_module_execution(
                run_id,
                span.name,
                status="failed" if span.name in self._failed else "completed",
                duration_seconds=self._durations[span.name],
                detail_json=json.dumps(
                    {"calls": self._calls[span.name], "last_recorded_at": recorded_at.isoformat()}
                ),
            )

    def record_warnings(self, run_id: str, warnings: list[WarningEvent]) -> None:
        for event in warnings:
            self._registry.record_run_event(
                run_id,
                severity="warning",
                module=event.module,
                message=event.message,
                context_json=json.dumps(
                    {
                        "scan_id": event.scan_id,
                        "category": event.category,
                        "logger": event.logger,
                        "timestamp": event.timestamp.isoformat(),
                    }
                ),
            )

    def record_errors(self, run_id: str, errors: list[ErrorEvent]) -> None:
        for event in errors:
            self._registry.record_run_event(
                run_id,
                severity="error",
                module=event.module,
                message=event.message,
                context_json=json.dumps(
                    {
                        "scan_id": event.scan_id,
                        "exception_type": event.exception_type,
                        "traceback": event.traceback,
                        "logger": event.logger,
                        "timestamp": event.timestamp.isoformat(),
                    }
                ),
            )

    def finalize_run(self, summary: RunSummary) -> None:
        """Map the end-of-run summary onto the single registry run lifecycle."""
        status = _SUMMARY_STATUS_TO_RUN.get(summary.status, "failed")
        self._registry.finalize_run(
            summary.run_id,
            status,
            scans_processed=summary.scans_processed,
            scans_failed=summary.failures,
        )


_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_history (
    run_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL, pipeline_version TEXT, git_commit TEXT,
    hostname TEXT, username TEXT,
    start_time TEXT NOT NULL, end_time TEXT, duration_seconds REAL,
    configuration_hash TEXT, configuration_file TEXT,
    dataset TEXT, site TEXT, instrument TEXT,
    files_processed INTEGER DEFAULT 0, scans_processed INTEGER DEFAULT 0,
    objects_detected INTEGER DEFAULT 0,
    warnings INTEGER DEFAULT 0, errors INTEGER DEFAULT 0,
    status TEXT NOT NULL,
    average_scan_time REAL, maximum_scan_time REAL,
    slowest_stage TEXT, slowest_stage_duration REAL,
    software_version TEXT, python_version TEXT, platform TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS module_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL, scan_id TEXT, module TEXT NOT NULL,
    duration_seconds REAL NOT NULL, status TEXT NOT NULL, error TEXT,
    trace_id TEXT, span_id TEXT, recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS warning_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL, scan_id TEXT, module TEXT,
    category TEXT, message TEXT NOT NULL, logger TEXT, timestamp TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS error_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL, scan_id TEXT, module TEXT,
    exception_type TEXT, message TEXT NOT NULL, traceback TEXT,
    logger TEXT, timestamp TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_module_history_run ON module_history(run_id);
CREATE INDEX IF NOT EXISTS idx_module_history_module ON module_history(module);
CREATE INDEX IF NOT EXISTS idx_error_history_run ON error_history(run_id);
CREATE INDEX IF NOT EXISTS idx_warning_history_run ON warning_history(run_id);
"""
