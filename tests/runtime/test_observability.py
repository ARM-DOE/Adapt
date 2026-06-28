# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Observability provider: context propagation, spans, metrics, disabled path.

The provider reads an injected clock/wall-clock/rng (never the wall clock by
default) so durations and ids are deterministic in tests.
"""

import random
import threading
from datetime import UTC, datetime

import pytest

from adapt.runtime.observability import (
    ObsSettings,
    build_observability,
    disabled_observability,
)


def _obs(**settings):
    return build_observability(
        ObsSettings(**settings),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def _obs_seq_clock(values, **settings):
    it = iter(values)
    return build_observability(
        ObsSettings(**settings),
        clock=lambda: next(it),
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def test_fresh_provider_has_blank_context() -> None:
    obs = _obs()
    assert obs.current().pipeline_id == ""
    assert obs.current().scan_id == ""


def test_bind_sets_then_restores() -> None:
    obs = _obs()
    with obs.bind(scan_id="512", dataset_id="KDIX"):
        assert obs.current().scan_id == "512"
        assert obs.current().dataset_id == "KDIX"
    assert obs.current().scan_id == ""
    assert obs.current().dataset_id == ""


def test_nested_bind_overrides_then_restores() -> None:
    obs = _obs()
    with obs.bind(scan_id="1"):
        with obs.bind(scan_id="2"):
            assert obs.current().scan_id == "2"
        assert obs.current().scan_id == "1"


def test_fresh_thread_sees_blank_context_until_it_binds() -> None:
    """contextvars do NOT cross threads — each worker must bind its own context."""
    obs = _obs()
    seen: dict[str, str] = {}
    with obs.bind(scan_id="outer"):

        def worker() -> None:
            seen["before"] = obs.current().scan_id
            with obs.bind(scan_id="inner"):
                seen["during"] = obs.current().scan_id

        t = threading.Thread(target=worker)
        t.start()
        t.join()
    assert seen["before"] == ""
    assert seen["during"] == "inner"


def test_span_records_duration_from_injected_clock() -> None:
    obs = _obs_seq_clock([10.0, 12.5])
    with obs.span("detection"):
        pass
    spans = obs.drain_spans()
    assert len(spans) == 1
    assert spans[0].name == "detection"
    assert spans[0].duration_s == 2.5


def test_span_binds_stage_and_mints_trace_in_context() -> None:
    obs = _obs_seq_clock([0.0, 1.0])
    with obs.span("ingest"):
        assert obs.current().stage == "ingest"
        assert obs.current().trace_id != ""
        assert obs.current().span_id != ""
    assert obs.current().stage == ""  # restored on exit


def test_nested_spans_share_trace_and_link_parent() -> None:
    obs = _obs_seq_clock([0.0, 1.0, 2.0, 3.0])
    with obs.span("parent"):
        parent_ctx = obs.current()
        with obs.span("child"):
            child_ctx = obs.current()
            assert child_ctx.trace_id == parent_ctx.trace_id
            assert child_ctx.span_id != parent_ctx.span_id
    spans = {s.name: s for s in obs.drain_spans()}
    assert spans["child"].parent_span_id == spans["parent"].span_id
    assert spans["child"].trace_id == spans["parent"].trace_id


def test_span_records_error_and_reraises() -> None:
    obs = _obs_seq_clock([0.0, 1.0])
    with pytest.raises(ValueError), obs.span("boom"):
        raise ValueError("nope")
    spans = obs.drain_spans()
    assert spans[0].error.startswith("ValueError")


def test_span_set_adds_metadata() -> None:
    obs = _obs_seq_clock([0.0, 1.0])
    with obs.span("detection") as s:
        s.set(n_cells=42)
    assert obs.drain_spans()[0].metadata["n_cells"] == "42"


def test_drain_spans_returns_then_clears() -> None:
    obs = _obs_seq_clock([0.0, 1.0])
    with obs.span("a"):
        pass
    assert len(obs.drain_spans()) == 1
    assert obs.drain_spans() == []


def test_counter_accumulates_across_calls() -> None:
    obs = _obs()
    obs.metrics.incr("files_processed_total")
    obs.metrics.incr("files_processed_total", 2.0)
    assert obs.metrics.counter_total("files_processed_total") == 3.0


def test_counter_total_sums_over_labels() -> None:
    obs = _obs()
    obs.metrics.incr("errors_total", stage="detection")
    obs.metrics.incr("errors_total", stage="tracking")
    assert obs.metrics.counter_total("errors_total") == 2.0


def test_gauge_is_last_write_wins() -> None:
    obs = _obs()
    obs.metrics.gauge("queue_depth", 5)
    obs.metrics.gauge("queue_depth", 2)
    assert obs.metrics.gauge_value("queue_depth") == 2


def test_histogram_collects_values() -> None:
    obs = _obs()
    obs.metrics.observe("scan_processing_time", 1.0)
    obs.metrics.observe("scan_processing_time", 3.0)
    assert obs.metrics.histogram_values("scan_processing_time") == [1.0, 3.0]


def test_histogram_totals_by_stage_label() -> None:
    obs = _obs()
    obs.metrics.observe("module_duration_seconds", 2.0, stage="detection")
    obs.metrics.observe("module_duration_seconds", 1.0, stage="detection")
    obs.metrics.observe("module_duration_seconds", 4.0, stage="tracking")
    totals = obs.metrics.histogram_totals_by_label("module_duration_seconds", "stage")
    assert totals == {"detection": 3.0, "tracking": 4.0}


def test_concurrent_counter_increments_sum_correctly() -> None:
    obs = _obs()

    def bump() -> None:
        for _ in range(1000):
            obs.metrics.incr("c")

    threads = [threading.Thread(target=bump) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert obs.metrics.counter_total("c") == 4000.0


def test_span_emits_duration_histogram_and_error_counter() -> None:
    obs = _obs_seq_clock([0.0, 2.0])
    with pytest.raises(ValueError), obs.span("detection"):
        raise ValueError("x")
    assert obs.metrics.histogram_values("module_duration_seconds") == [2.0]
    assert obs.metrics.counter_total("errors_total") == 1.0


def test_disabled_provider_span_is_shared_noop() -> None:
    obs = _obs(enabled=False)
    with obs.span("a") as h:
        h.set(x=1)
    assert obs.drain_spans() == []
    assert obs.span("b") is obs.span("c")  # shared no-op, no per-call allocation


def test_disabled_provider_span_does_not_touch_context() -> None:
    obs = _obs(enabled=False)
    with obs.span("a"):
        assert obs.current().stage == ""


def test_disabled_provider_bind_and_metrics_are_noop() -> None:
    obs = _obs(enabled=False)
    with obs.bind(scan_id="z"):
        assert obs.current().scan_id == ""
    obs.metrics.incr("x")
    assert obs.metrics.counter_total("x") == 0.0


def test_disabled_observability_helper_records_nothing() -> None:
    obs = disabled_observability()
    with obs.span("a"):
        obs.metrics.incr("c")
    assert obs.drain_spans() == []
    assert obs.metrics.counter_total("c") == 0.0


def test_span_exit_survives_cross_context_teardown():
    """Entering a span in one contextvars Context and exiting it in another must
    restore the parent context without raising.

    This is the Ctrl+C shutdown path: the orchestrator opens the root "pipeline"
    span in start() and closes it in stop(); if teardown straddles a copied
    Context the raw Token.reset() raises ValueError. Teardown must stay
    deterministic and crash-free, leaving a consistent (blank) context.
    """
    import contextvars

    obs = _obs()
    span = obs.span("pipeline", pipeline_id="R1")

    # Enter inside an isolated copied Context so the reset Token is foreign to the
    # outer Context where we exit — exactly the straddle that crashes on shutdown.
    contextvars.copy_context().run(span.__enter__)
    span.__exit__(None, None, None)  # must not raise

    assert obs.current().pipeline_id == ""  # parent context restored, not corrupted


def test_bind_exit_survives_cross_context_teardown():
    """bind() teardown must also tolerate a cross-Context exit."""
    import contextvars

    obs = _obs()
    cm = obs.bind(scan_id="s1")

    contextvars.copy_context().run(cm.__enter__)
    cm.__exit__(None, None, None)  # must not raise

    assert obs.current().scan_id == ""
