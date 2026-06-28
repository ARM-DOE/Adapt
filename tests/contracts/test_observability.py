# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Contract types for observability: the injectable DI seam.

ObsContext/SpanRecord are pure frozen data; Observability is the Protocol that
execution/runtime depend on so the concrete provider can be injected (and later
relocated) without touching call sites.
"""

import dataclasses

import pytest

from adapt.contracts.observability import ObsContext, Observability, SpanRecord


def test_obs_context_is_frozen_and_defaults_blank_ids() -> None:
    ctx = ObsContext(pipeline_id="run-1", trace_id="abc")
    assert ctx.pipeline_id == "run-1"
    assert ctx.trace_id == "abc"
    # everything else defaults to empty so a fresh context is harmless
    assert ctx.span_id == ""
    assert ctx.scan_id == ""
    assert ctx.dataset_id == ""
    assert ctx.stage == ""
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.scan_id = "512"  # type: ignore[misc]


def test_span_record_carries_timing_and_error() -> None:
    rec = SpanRecord(
        name="detection",
        trace_id="t",
        span_id="s",
        parent_span_id="p",
        start=10.0,
        finish=12.5,
        duration_s=2.5,
        error="",
        metadata={"n_cells": "42"},
    )
    assert rec.duration_s == 2.5
    assert rec.metadata["n_cells"] == "42"
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.error = "boom"  # type: ignore[misc]


def test_observability_protocol_is_runtime_checkable() -> None:
    class _Impl:
        metrics = object()

        def span(self, name, **ctx): ...

        def bind(self, **ctx): ...

        def current(self): ...

        def drain_spans(self): ...

        def install_logging(self, log_path): ...

    class _NotImpl:
        def span(self, name, **ctx): ...

    assert isinstance(_Impl(), Observability)
    assert not isinstance(_NotImpl(), Observability)
