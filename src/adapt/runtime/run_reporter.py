# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The quiet console: a one-shot run header, a periodic progress line, and an
end-of-run summary. Plain ASCII, no colours/spinners. Lines are emitted at INFO
with ``extra={"console": True}`` so the ConsoleFilter lets them through while
routine per-scan logs stay in the file/JSON log only.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from adapt.contracts.execution_history import RunStart, RunSummary
from adapt.contracts.observability import SpanRecord

if TYPE_CHECKING:
    from adapt.configuration.schemas.internal import InternalConfig

__all__ = [
    "RunReporter",
    "format_header",
    "format_methods",
    "format_summary",
    "format_duration",
    "format_seconds",
    "format_scan",
    "format_banner",
]

_RULE = "─" * 60
_LICENSE_URL = "https://arm-doe.github.io/Adapt/license.html"


def format_banner(version: str) -> str:
    """The plain name/version/copyright block the CLI prints first, before anything else."""
    return "\n".join(
        [
            f"ARM Adapt v{version}",
            "Copyright © 2026, UChicago Argonne, LLC.",
            f"See the LICENSE ({_LICENSE_URL}) for terms and disclaimer.",
        ]
    )


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


def format_seconds(seconds: float) -> str:
    """Compact duration that keeps sub-second resolution: ms under 1s (so fast stages
    don't collapse to '0s'), tenths under a minute, then h/m/s.
    """
    if seconds < 1.0:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60.0:
        return f"{seconds:.1f}s"
    return format_duration(seconds)


def format_header(start: RunStart) -> str:
    """One compact block summarising the run at a glance."""
    p = start.provenance
    commit = p.git_commit[:7] if p.git_commit else "—"
    return "\n".join(
        [
            f"── Adapt run {start.run_id} {_RULE[: max(0, 48 - len(start.run_id))]}",
            f"site {start.site} · {start.instrument} · mode {start.mode}",
            f"pipeline {start.pipeline} v{start.pipeline_version} · commit {commit} "
            f"· python {p.python_version} · {p.platform}",
            f"config {start.configuration_file} (sha {start.configuration_hash[:8]}) "
            f"· modules: {', '.join(start.enabled_modules)}",
            _RULE,
        ]
    )


# One short "<method> · <key choices>" per module, read straight off the frozen
# InternalConfig. Only algorithmic modules with a real method/approach appear;
# purely parametric ones (analysis, cell_volume_stats) are left out. Add a module
# here to give it a line — nothing else references this map.
_METHOD_LINES = {
    "ingest": lambda c: f"regrid {c.regridder.weighting_function} · roi {c.regridder.roi_func}",
    "detection": lambda c: (
        f"{c.segmenter.method} · min_cellsize {c.segmenter.min_cellsize_gridpoint}"
    ),
    "projection": lambda c: (
        f"{c.projector.method} · horizon {c.projector.projection_horizon_minutes}min "
        f"· steps {c.projector.max_projection_steps}"
    ),
    "tracking": lambda c: (
        f"gap {c.tracker.max_tracking_gap_minutes:g}min "
        f"· overlap≥{c.tracker.minimum_candidate_overlap:g} · vmax {c.tracker.max_speed_ms:g}m/s"
    ),
}


def format_methods(config: InternalConfig, enabled_modules: tuple[str, ...]) -> str:
    """One line per enabled algorithmic module: its chosen method and key choices.

    Built from the frozen config only, in pipeline order. Returns ``""`` when no
    enabled module carries a method (so the caller can skip emitting anything).
    """
    lines = [
        f"  {name:<11}{_METHOD_LINES[name](config)}"
        for name in enabled_modules
        if name in _METHOD_LINES
    ]
    return "\n".join(["methods:", *lines]) if lines else ""


def format_summary(summary: RunSummary) -> str:
    """One compact block of end-of-run stats with a per-module timing table."""
    module_lines = [
        f"  {name:<18} {calls:>4} · {format_seconds(total):>7} · "
        f"{format_seconds(total / calls if calls else 0.0)}"
        for name, calls, total in summary.module_stats
    ]
    if not module_lines:
        module_lines = ["  —"]
    return "\n".join(
        [
            f"── run {summary.run_id}  {summary.status.upper()}  "
            f"{format_duration(summary.duration_seconds)} {_RULE[:20]}",
            f"files {summary.files_processed:,} · scans {summary.scans_processed:,} "
            f"· objects {summary.objects_detected:,} · warnings {summary.warnings} "
            f"· errors {summary.errors} · failures {summary.failures}",
            f"scan time avg {summary.average_scan_time:.2f}s "
            f"· max {summary.maximum_scan_time:.2f}s",
            "per module (calls · total · avg):",
            *module_lines,
            _RULE,
        ]
    )


def format_scan(scan_id: str, spans: list[SpanRecord], n_cells: int) -> str:
    """One compact per-scan line, built from the scan's drained module spans.

    Reads ``stage duration`` straight off the captured telemetry — so the console
    reports what ran and how long without any module emitting its own progress.
    """
    stages = "  ".join(f"{s.name} {format_seconds(s.duration_s)}" for s in spans)
    total = sum(s.duration_s for s in spans)
    return f"scan {scan_id}  │  {stages}  │  {n_cells} cells  {format_seconds(total)}"


class RunReporter:
    """Emits the console-tagged line groups: header, per-scan progress, summary."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._log = logger or logging.getLogger("adapt.run")

    def header(self, start: RunStart) -> None:
        self._log.info(format_header(start), extra={"console": True})

    def methods(self, config: InternalConfig, enabled_modules: tuple[str, ...]) -> None:
        text = format_methods(config, enabled_modules)
        if text:
            self._log.info(text, extra={"console": True})

    def progress(self, text: str) -> None:
        self._log.info(text, extra={"console": True})

    def scan(self, scan_id: str, spans: list[SpanRecord], n_cells: int) -> None:
        self._log.info(format_scan(scan_id, spans, n_cells), extra={"console": True})

    def summary(self, summary: RunSummary) -> None:
        self._log.info(format_summary(summary), extra={"console": True})
