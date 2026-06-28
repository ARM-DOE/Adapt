# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Execution-history data contracts: the DTOs the runtime hands to persistence.

Pure frozen types, stdlib only. The runtime (which holds both telemetry and the
repository) builds these; ``adapt.persistence.ExecutionHistory`` consumes them.
``SpanRecord`` (from ``contracts.observability``) is reused for per-module rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class RunProvenance:
    """Environment snapshot for reproducibility. ``git_commit`` is None outside a checkout."""

    git_commit: str | None
    hostname: str
    username: str
    python_version: str
    platform: str
    software_version: str


@dataclass(frozen=True, slots=True)
class RunStart:
    """Everything known at run start — drives both run_history and the console header."""

    run_id: str
    pipeline: str
    pipeline_version: str
    site: str  # radar id
    dataset: str  # dataset id (= radar today)
    instrument: str
    mode: str
    start_time: datetime
    configuration_hash: str
    configuration_file: str
    provenance: RunProvenance
    enabled_modules: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RunSummary:
    """End-of-run aggregates — drives run_history finalize and the console summary."""

    run_id: str
    status: str  # success | failed | cancelled
    end_time: datetime
    duration_seconds: float
    files_processed: int
    scans_processed: int
    objects_detected: int
    warnings: int
    errors: int
    average_scan_time: float
    maximum_scan_time: float
    slowest_stages: tuple[tuple[str, float], ...]  # (module, total_seconds) desc
    # Per-module aggregates for the console summary: (module, calls, total_seconds) desc.
    module_stats: tuple[tuple[str, int, float], ...] = ()
    failures: int = 0  # module execution failures (errors_total counter)


@dataclass(frozen=True, slots=True)
class WarningEvent:
    scan_id: str
    module: str
    category: str
    message: str
    logger: str
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    scan_id: str
    module: str
    exception_type: str
    message: str
    traceback: str
    logger: str
    timestamp: datetime
