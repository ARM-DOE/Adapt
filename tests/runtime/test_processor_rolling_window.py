# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for RadarProcessor rolling-window executor grouping.

The processor groups modules by required_history (not pipeline_phase).
Modules with required_history=1 run on every scan.
Modules with required_history=2 run once 2 scans have accumulated.
"""

import queue

import pytest

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


class TestProcessorExecutorGrouping:
    def test_processor_groups_executors_by_required_history(self, pipeline_config, store_env):
        """Processor builds one GraphExecutor per required_history value."""
        proc = _make_proc(pipeline_config, store_env)
        assert isinstance(proc._executors, dict)
        assert 1 in proc._executors
        assert 2 in proc._executors

    def test_single_scan_executor_contains_ingest_and_detection(self, pipeline_config, store_env):
        """required_history=1 executor covers ingest and detection."""
        proc = _make_proc(pipeline_config, store_env)
        names = {n.name for n in proc._executors[1].nodes}
        assert "ingest" in names
        assert "detection" in names

    def test_multi_scan_executor_contains_projection_analysis_tracking(
        self, pipeline_config, store_env
    ):
        """required_history=2 executor covers projection, analysis, tracking."""
        proc = _make_proc(pipeline_config, store_env)
        names = {n.name for n in proc._executors[2].nodes}
        assert "projection" in names
        assert "analysis" in names
        assert "tracking" in names

    def test_processor_has_empty_scan_history_on_init(self, pipeline_config, store_env):
        """Processor starts with empty scan history."""
        proc = _make_proc(pipeline_config, store_env)
        assert proc._scan_history == []

    def test_no_phase_based_segmented_history(self, pipeline_config, store_env):
        """_segmented_history (old phase-based attr) must not exist."""
        proc = _make_proc(pipeline_config, store_env)
        assert not hasattr(proc, "_segmented_history")

    def test_post_persistence_executor_present_by_default(self, pipeline_config, store_env):
        """cell_volume_stats is a default phase-3 module → post_executor is built."""
        proc = _make_proc(pipeline_config, store_env)
        assert proc._post_executor is not None
        assert "cell_volume_stats" in [m.name for m in proc._post_modules]

    def test_post_persistence_executor_is_none_without_phase3(self, pipeline_config, store_env):
        """Disabling the only phase-3 module (--not) → post_executor is None."""
        cfg = pipeline_config.model_copy(update={"exclude_modules": ["cell_volume_stats"]})
        proc = _make_proc(cfg, store_env)
        assert proc._post_executor is None
        assert proc._post_modules == []
