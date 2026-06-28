# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Observability provider: structured-logging context, tracing, and metrics.

Lives in ``runtime`` because the orchestrator (composition root) is its only
importer; ``execution``/``runtime`` receive it injected as the
``adapt.contracts.observability.Observability`` Protocol, so it can be relocated
later without touching call sites. All wall-clock/RNG reads are injected and
required — never defaulted — so durations and ids stay deterministic under test
and no hidden clock read can slip in. Scientific modules never import this.
"""

from __future__ import annotations

import contextvars
import threading
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from random import Random

from adapt.contracts.observability import ObsContext, SpanRecord

__all__ = [
    "ObsSettings",
    "Observability",
    "build_observability",
    "current_context",
    "disabled_observability",
]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# The single correlation context. Per-thread isolated (contextvars do not cross
# threads); each worker re-binds at the top of its run(). Default is blank so a
# fresh/unbound context is harmless.
_BLANK_CONTEXT = ObsContext(pipeline_id="", trace_id="")
_CTX: contextvars.ContextVar[ObsContext] = contextvars.ContextVar(
    "adapt_obs", default=_BLANK_CONTEXT
)


def current_context() -> ObsContext:
    """The current correlation context (blank if nothing is bound on this thread)."""
    return _CTX.get()


def _restore_context(token: contextvars.Token, parent: ObsContext) -> None:
    """Restore the context on scope exit, surviving a cross-Context teardown.

    ``Token.reset`` raises ValueError when the token was created in a different
    contextvars Context — the Ctrl+C shutdown path, where a long-lived span is
    entered and exited across a copied Context. The span record is already
    emitted by then, so the only job left is to put the parent value back; do that
    directly (valid in any Context) instead of letting cleanup crash the shutdown.
    """
    try:
        _CTX.reset(token)
    except ValueError:
        _CTX.set(parent)


@dataclass(frozen=True, slots=True)
class ObsSettings:
    """Resolved observability toggles. Built by the orchestrator from InternalConfig.

    Field defaults are the dev profile; profiles/CLI override explicitly upstream.
    """

    enabled: bool = True
    traces: bool = True
    metrics: bool = True
    json_logs: bool = False
    console_logs: bool = True
    level: str = "INFO"
    console_level: str = "WARNING"
    progress_every: float = 30.0


class Observability:
    """Injectable provider for context, spans, and metrics."""

    def __init__(
        self,
        settings: ObsSettings,
        *,
        clock: Callable[[], float],
        wall_clock: Callable[[], datetime],
        rng: Random,
    ) -> None:
        self._settings = settings
        self._clock = clock
        self._wall = wall_clock
        self._rng = rng
        self._enabled = settings.enabled
        self._spans: list[SpanRecord] = []
        self.metrics = _Meter(enabled=settings.enabled and settings.metrics)

    # ── context propagation ───────────────────────────────────────────────────
    def current(self) -> ObsContext:
        return current_context()

    @contextmanager
    def bind(self, **fields: str):
        """Push correlation fields onto the current context for the block's scope."""
        if not self._enabled:
            yield
            return
        parent = _CTX.get()
        token = _CTX.set(replace(parent, **fields))
        try:
            yield
        finally:
            _restore_context(token, parent)

    # ── tracing ───────────────────────────────────────────────────────────────
    def span(self, name: str, **seed: object):
        """Open a span; use as a context manager. Nesting is automatic via contextvars."""
        if not self._enabled:
            return _NULL_SPAN
        return _Span(self, name, {k: str(v) for k, v in seed.items()})

    def drain_spans(self) -> list[SpanRecord]:
        """Return finished spans recorded since the last drain, then clear the buffer."""
        spans, self._spans = self._spans, []
        return spans

    def _trace_id(self) -> str:
        return f"{self._rng.getrandbits(128):032x}"

    def _span_id(self) -> str:
        return f"{self._rng.getrandbits(64):016x}"

    def _record(self, record: SpanRecord) -> None:
        if self._settings.traces:
            self._spans.append(record)


class _Span:
    """A single span. Reads the current context as its parent; restores on exit."""

    def __init__(self, obs: Observability, name: str, seed: dict[str, str]) -> None:
        self._obs = obs
        self._name = name
        self._seed = seed
        self._error = ""
        self._meta: dict[str, str] = {}
        self.duration_s = 0.0

    def set(self, **metadata: object) -> None:
        self._meta.update({k: str(v) for k, v in metadata.items()})

    def record_error(self, exc: BaseException) -> None:
        self._error = f"{type(exc).__name__}: {exc}"

    def __enter__(self) -> _Span:
        parent = _CTX.get()
        self._parent = parent
        self._parent_id = parent.span_id
        self._trace_id = parent.trace_id or self._obs._trace_id()
        self._span_id = self._obs._span_id()
        self._start = self._obs._clock()
        self._token = _CTX.set(
            replace(
                parent,
                trace_id=self._trace_id,
                span_id=self._span_id,
                stage=self._name,
                **self._seed,
            )
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        finish = self._obs._clock()
        if exc is not None and not self._error:
            self._error = f"{exc_type.__name__}: {exc}"
        self.duration_s = finish - self._start
        self._obs.metrics.observe("module_duration_seconds", self.duration_s, stage=self._name)
        if self._error:
            self._obs.metrics.incr("errors_total", stage=self._name)
        self._obs._record(
            SpanRecord(
                name=self._name,
                trace_id=self._trace_id,
                span_id=self._span_id,
                parent_span_id=self._parent_id,
                start=self._start,
                finish=finish,
                duration_s=self.duration_s,
                error=self._error,
                metadata=dict(self._meta),
            )
        )
        _restore_context(self._token, self._parent)
        return False  # never swallow — fail loudly


class _NullSpan:
    """Shared zero-cost span used when observability is disabled."""

    duration_s = 0.0

    def set(self, **metadata: object) -> None: ...

    def record_error(self, exc: BaseException) -> None: ...

    def __enter__(self) -> _NullSpan:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


_NULL_SPAN = _NullSpan()


def _key(labels: dict[str, str]) -> tuple:
    return tuple(sorted(labels.items()))


class _Meter:
    """Lock-guarded, accumulate-only in-memory metrics. Counters/histograms are
    order-insensitive; gauges are last-write-wins (observational, not asserted for
    determinism). Never read back into the science path.
    """

    def __init__(self, *, enabled: bool) -> None:
        self._enabled = enabled
        self._lock = threading.Lock()
        self._counters: dict[tuple, float] = {}
        self._gauges: dict[tuple, float] = {}
        self._hist: dict[tuple, list[float]] = {}

    def incr(self, name: str, value: float = 1.0, **labels: str) -> None:
        if not self._enabled:
            return
        k = (name, _key(labels))
        with self._lock:
            self._counters[k] = self._counters.get(k, 0.0) + value

    def gauge(self, name: str, value: float, **labels: str) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._gauges[(name, _key(labels))] = value

    def observe(self, name: str, value: float, **labels: str) -> None:
        if not self._enabled:
            return
        with self._lock:
            self._hist.setdefault((name, _key(labels)), []).append(value)

    def counter_total(self, name: str) -> float:
        with self._lock:
            return sum(v for (n, _), v in self._counters.items() if n == name)

    def gauge_value(self, name: str, **labels: str) -> float | None:
        with self._lock:
            return self._gauges.get((name, _key(labels)))

    def histogram_values(self, name: str) -> list[float]:
        with self._lock:
            out: list[float] = []
            for (n, _), values in self._hist.items():
                if n == name:
                    out.extend(values)
            return out

    def histogram_totals_by_label(self, name: str, label: str) -> dict[str, float]:
        with self._lock:
            out: dict[str, float] = {}
            for (n, lk), values in self._hist.items():
                if n != name:
                    continue
                key = dict(lk).get(label)
                if key is None:
                    continue
                out[key] = out.get(key, 0.0) + sum(values)
            return out


def build_observability(
    settings: ObsSettings,
    *,
    clock: Callable[[], float],
    wall_clock: Callable[[], datetime],
    rng: Random,
) -> Observability:
    """The one constructor (DI entry). ``clock``/``wall_clock``/``rng`` are required."""
    return Observability(settings, clock=clock, wall_clock=wall_clock, rng=rng)


def disabled_observability() -> Observability:
    """A real provider with telemetry OFF — a clean explicit default for runtime
    components when none is injected (not a silent fallback: telemetry is optional)."""
    return build_observability(
        ObsSettings(enabled=False),
        clock=lambda: 0.0,
        wall_clock=lambda: _EPOCH,
        rng=Random(0),
    )
