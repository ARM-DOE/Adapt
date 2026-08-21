# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for RadarProcessor graph-based processing.

The processor delegates scientific work to per-required_history GraphExecutors built
at startup: required_history=1 (ingest + detection), required_history=2 (projection +
analysis + tracking). Post-persistence extensions use pipeline_phase=3 and a separate
_post_executor. These tests verify the orchestration layer: initialization, lifecycle.
"""

import queue

import pytest

from adapt.execution.graph.executor import GraphExecutor
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _make_proc(pipeline_config, store_env):
    return RadarProcessor(
        queue.Queue(),
        pipeline_config,
        collection=store_env.collection,
        registry=store_env.registry,
        run_id=store_env.run_id,
        history=store_env.history,
    )


def test_processor_initializes_phase_executors(pipeline_config, store_env):
    """Processor builds one GraphExecutor per pipeline_phase on init."""
    proc = _make_proc(pipeline_config, store_env)
    assert isinstance(proc._executors, dict)
    assert isinstance(proc._executors[1], GraphExecutor)
    assert isinstance(proc._executors[2], GraphExecutor)


def test_phase1_executor_contains_ingest_and_detection(pipeline_config, store_env):
    """Phase-1 executor graph covers ingest and detection nodes."""
    proc = _make_proc(pipeline_config, store_env)
    phase1_names = {n.name for n in proc._executors[1].nodes}
    assert "ingest" in phase1_names
    assert "detection" in phase1_names


def test_phase2_executor_contains_projection_analysis_tracking(pipeline_config, store_env):
    """Phase-2 executor graph covers projection, analysis, and tracking nodes."""
    proc = _make_proc(pipeline_config, store_env)
    phase2_names = {n.name for n in proc._executors[2].nodes}
    assert "projection" in phase2_names
    assert "analysis" in phase2_names
    assert "tracking" in phase2_names


def test_processor_stop_sets_flag(pipeline_config, store_env):
    """stop() signals the run loop to exit."""
    proc = _make_proc(pipeline_config, store_env)
    assert not proc.stopped()
    proc.stop()
    assert proc.stopped()


def test_processor_stop_is_idempotent(pipeline_config, store_env):
    """Calling stop() twice is safe."""
    proc = _make_proc(pipeline_config, store_env)
    proc.stop()
    proc.stop()
    assert proc.stopped()


def test_processor_requires_store_wiring(pipeline_config, store_env):
    """RadarProcessor construction is keyword-only on its store dependencies."""
    with pytest.raises(TypeError):
        RadarProcessor(queue.Queue(), pipeline_config, store_env.collection)  # type: ignore[misc]
