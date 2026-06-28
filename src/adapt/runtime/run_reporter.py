# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The quiet console: a one-shot run header, a periodic progress line, and an
end-of-run summary. Plain ASCII, no colours/spinners. Lines are emitted at INFO
with ``extra={"console": True}`` so the ConsoleFilter lets them through while
routine per-scan logs stay in the file/JSON log only.
"""

from __future__ import annotations

import logging

from adapt.contracts.execution_history import RunStart, RunSummary
from adapt.contracts.observability import SpanRecord

__all__ = ["RunReporter", "format_header", "format_summary", "format_duration", "format_scan"]

_RULE = "─" * 60


def format_duration(seconds: float) -> str:
    """Human-compact duration, e.g. 862 -> '14m22s', 45 -> '45s', 3725 -> '1h2m5s'."""
    total = int(round(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return "".join(parts)


def format_header(start: RunStart) -> str:
    """One compact block summarising the run at a glance."""
    p = start.provenance
    commit = p.git_commit[:7] if p.git_commit else "—"
    return "\n".join(
        [
            f"── ADAPT run {start.run_id} {_RULE[: max(0, 48 - len(start.run_id))]}",
            f"site {start.site} · {start.instrument} · mode {start.mode}",
            f"pipeline {start.pipeline} v{start.pipeline_version} · commit {commit} "
            f"· python {p.python_version} · {p.platform}",
            f"config {start.configuration_file} (sha {start.configuration_hash[:8]}) "
            f"· modules: {', '.join(start.enabled_modules)}",
            _RULE,
        ]
    )


def format_summary(summary: RunSummary) -> str:
    """One compact block of end-of-run stats."""
    slowest = " · ".join(f"{name} {format_duration(secs)}" for name, secs in summary.slowest_stages)
    return "\n".join(
        [
            f"── run {summary.run_id}  {summary.status.upper()}  "
            f"{format_duration(summary.duration_seconds)} {_RULE[:20]}",
            f"files {summary.files_processed:,} · scans {summary.scans_processed:,} "
            f"· objects {summary.objects_detected:,} · warnings {summary.warnings} "
            f"· errors {summary.errors}",
            f"scan time avg {summary.average_scan_time:.2f}s "
            f"· max {summary.maximum_scan_time:.2f}s",
            f"slowest: {slowest}" if slowest else "slowest: —",
            _RULE,
        ]
    )


def format_scan(scan_id: str, spans: list[SpanRecord], n_cells: int) -> str:
    """One compact per-scan line, built from the scan's drained module spans.

    Reads ``stage duration`` straight off the captured telemetry — so the console
    reports what ran and how long without any module emitting its own progress.
    """
    stages = "  ".join(f"{s.name} {s.duration_s:.1f}s" for s in spans)
    total = sum(s.duration_s for s in spans)
    return f"scan {scan_id}  │  {stages}  │  {n_cells} cells  {total:.1f}s"


class RunReporter:
    """Emits the console-tagged line groups: header, per-scan progress, summary."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger("adapt.run")

    def header(self, start: RunStart) -> None:
        self._log.info(format_header(start), extra={"console": True})

    def progress(self, text: str) -> None:
        self._log.info(text, extra={"console": True})

    def scan(self, scan_id: str, spans: list[SpanRecord], n_cells: int) -> None:
        self._log.info(format_scan(scan_id, spans, n_cells), extra={"console": True})

    def summary(self, summary: RunSummary) -> None:
        self._log.info(format_summary(summary), extra={"console": True})
