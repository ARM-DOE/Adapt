# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for RadarProcessor graph-based processing.

The processor delegates scientific work to per-required_history GraphExecutors built
at startup: required_history=1 (ingest + detection), required_history=2 (projection +
analysis + tracking). Post-persistence extensions use pipeline_phase=3 and a separate
_post_executor. These tests verify the orchestration layer: initialization, lifecycle.
"""

import queue

import pandas as pd
import pytest

from adapt.execution.graph.executor import GraphExecutor
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _make_proc(pipeline_config, pipeline_output_dirs, test_repository):
    return RadarProcessor(
        queue.Queue(),
        pipeline_config,
        pipeline_output_dirs,
        repository=test_repository,
    )


def test_processor_initializes_phase_executors(
    pipeline_config, pipeline_output_dirs, test_repository
):
    """Processor builds one GraphExecutor per pipeline_phase on init."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    assert isinstance(proc._executors, dict)
    assert isinstance(proc._executors[1], GraphExecutor)
    assert isinstance(proc._executors[2], GraphExecutor)


def test_phase1_executor_contains_ingest_and_detection(
    pipeline_config, pipeline_output_dirs, test_repository
):
    """Phase-1 executor graph covers ingest and detection nodes."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    phase1_names = {n.name for n in proc._executors[1].nodes}
    assert "ingest" in phase1_names
    assert "detection" in phase1_names


def test_phase2_executor_contains_projection_analysis_tracking(
    pipeline_config, pipeline_output_dirs, test_repository
):
    """Phase-2 executor graph covers projection, analysis, and tracking nodes."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    phase2_names = {n.name for n in proc._executors[2].nodes}
    assert "projection" in phase2_names
    assert "analysis" in phase2_names
    assert "tracking" in phase2_names


def test_processor_stop_sets_flag(pipeline_config, pipeline_output_dirs, test_repository):
    """stop() signals the run loop to exit."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    assert not proc.stopped()
    proc.stop()
    assert proc.stopped()


def test_processor_stop_is_idempotent(pipeline_config, pipeline_output_dirs, test_repository):
    """Calling stop() twice is safe."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    proc.stop()
    proc.stop()
    assert proc.stopped()


def test_processor_get_results_returns_empty_dataframe(
    pipeline_config, pipeline_output_dirs, test_repository
):
    """get_results() returns an empty DataFrame — results live in the repository."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    result = proc.get_results()
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_processor_save_results_returns_none(
    pipeline_config, pipeline_output_dirs, test_repository
):
    """save_results() is a no-op; persistence is handled by the OutputRouter."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    result = proc.save_results()
    assert result is None


def test_processor_close_database_returns_none(
    pipeline_config, pipeline_output_dirs, test_repository
):
    """close_database() is a no-op; the repository owns its own lifecycle."""
    proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
    result = proc.close_database()
    assert result is None


def test_processor_requires_repository(pipeline_config, pipeline_output_dirs):
    """RadarProcessor raises ValueError when repository is None."""
    with pytest.raises(ValueError, match="DataRepository is required"):
        RadarProcessor(queue.Queue(), pipeline_config, pipeline_output_dirs, repository=None)
