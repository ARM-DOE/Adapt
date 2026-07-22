# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Observability contract: the injectable seam shared by execution and runtime.

Pure types only — zero logic, stdlib imports. ``ObsContext`` is the correlation
identity carried out-of-band in contextvars (never in the science context dict).
``SpanRecord`` is a finished-span snapshot handed to persistence (execution
history). ``Observability`` is the Protocol that ``GraphExecutor`` and the
runtime depend on, so the concrete provider can be injected — and later
relocated — without changing any call site.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class ObsContext:
    """Immutable correlation identity. Travels via contextvars, never mutated.

    These ids never enter the science ``context`` dict, so they cannot collide
    with module inputs/outputs or affect determinism.
    """

    pipeline_id: str
    trace_id: str
    span_id: str = ""
    scan_id: str = ""
    dataset_id: str = ""  # radar / site id, e.g. "KDIX"
    experiment_id: str = ""
    worker_id: str = ""
    stage: str = ""


@dataclass(frozen=True, slots=True)
class SpanRecord:
    """Finished-span snapshot emitted to consumers (e.g. module_history)."""

    name: str
    trace_id: str
    span_id: str
    parent_span_id: str
    start: float
    finish: float
    duration_s: float
    error: str  # "" when no error
    metadata: dict[str, str] = field(default_factory=dict)


@runtime_checkable
class Metrics(Protocol):
    """In-process metrics sink + read-back accessors used to build the run summary."""

    def incr(self, name: str, value: float = ..., **labels: str) -> None: ...

    def gauge(self, name: str, value: float, **labels: str) -> None: ...

    def observe(self, name: str, value: float, **labels: str) -> None: ...

    def counter_total(self, name: str) -> float: ...

    def gauge_value(self, name: str, **labels: str) -> float | None: ...

    def histogram_values(self, name: str) -> list[float]: ...

    def histogram_totals_by_label(self, name: str, label: str) -> dict[str, float]: ...

    def histogram_counts_by_label(self, name: str, label: str) -> dict[str, int]: ...


@runtime_checkable
class Observability(Protocol):
    """The injected observability provider. Real or disabled, same interface."""

    metrics: Metrics

    def span(self, name: str, **ctx: object) -> AbstractContextManager: ...

    def bind(self, **ctx: str) -> AbstractContextManager: ...

    def current(self) -> ObsContext: ...

    def drain_spans(self) -> list[SpanRecord]: ...
