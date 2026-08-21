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

