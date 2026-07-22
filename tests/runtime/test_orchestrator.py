import time

import pytest

from adapt.runtime.orchestrator import PipelineOrchestrator

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def test_orchestrator_build_run_summary_aggregates_metrics_and_history(
    pipeline_config, test_repository
):
    orch = PipelineOrchestrator(pipeline_config)
    orch._obs = orch._build_observability()
    orch.repository = test_repository
    orch.run_id = test_repository.run_id
    orch._start_time = time.time() - 10

    m = orch._obs.metrics
    m.incr("files_processed_total")
    m.incr("files_processed_total")
    m.incr("cells_detected_total", value=18431)
    m.observe("scan_processing_time", 1.0)
    m.observe("scan_processing_time", 3.0)
    m.observe("module_duration_seconds", 5.0, stage="detection")
    m.observe("module_duration_seconds", 2.0, stage="tracking")
    # Wrapper spans share the same histogram but are NOT modules — must be excluded.
    m.observe("module_duration_seconds", 99.0, stage="pipeline")
    m.observe("module_duration_seconds", 25.0, stage="scan")

    summary = orch._build_run_summary("success", warnings=0, errors=0)
    assert summary.status == "success"
    assert summary.files_processed == 2
    assert summary.objects_detected == 18431
    assert summary.scans_processed == 2
    assert summary.average_scan_time == 2.0
    assert summary.maximum_scan_time == 3.0
    assert summary.slowest_stages[0] == ("detection", 5.0)
    assert summary.duration_seconds >= 10.0
    # per-module aggregates: calls + total, sorted by total desc
    assert summary.module_stats[0] == ("detection", 1, 5.0)
    assert ("tracking", 1, 2.0) in summary.module_stats
    # the pipeline/scan wrapper spans are excluded from the module table
    names = {name for name, _calls, _total in summary.module_stats}
    assert "pipeline" not in names and "scan" not in names
    assert summary.slowest_stages[0] == ("detection", 5.0)  # not the 99s pipeline span


def test_finalize_history_prints_summary_even_when_db_write_fails(
    pipeline_config, test_repository, monkeypatch
):
    """The console summary must be emitted on shutdown even if the history DB write
    raises — it is printed before persistence and must not depend on it.
    """
    orch = PipelineOrchestrator(pipeline_config)
    orch._obs = orch._build_observability()
    orch.repository = test_repository
    orch.run_id = test_repository.run_id
    orch._start_time = time.time() - 5
    orch._history_handler = None  # no buffered warnings/errors

    seen = []

    class _Rep:
        def summary(self, s):
            seen.append(s)

    orch._reporter = _Rep()

    def _boom(_summary):
        raise RuntimeError("db locked")

    monkeypatch.setattr(test_repository.history, "finalize_run", _boom)

    orch._finalize_history()  # must not raise

    assert len(seen) == 1  # summary still printed despite the DB failure


def test_orchestrator_initialization(pipeline_config):
    """Orchestrator initializes with config."""
    orch = PipelineOrchestrator(pipeline_config)
    assert orch.downloader_queue is not None


def test_orchestrator_logging_and_tracker(pipeline_config):
    """Orchestrator sets up logging and file tracker."""
    orch = PipelineOrchestrator(pipeline_config)
    orch._setup_logging()

    assert orch.tracker is not None


def test_orchestrator_builds_working_observability_from_config(pipeline_config):
    """The provider built from config actually records spans and metrics."""
    orch = PipelineOrchestrator(pipeline_config)
    obs = orch._build_observability()
    with obs.span("ingest"):
        obs.metrics.incr("files_processed_total")
    assert [s.name for s in obs.drain_spans()] == ["ingest"]
    assert obs.metrics.counter_total("files_processed_total") == 1.0


def test_orchestrator_queue_wiring(pipeline_config):
    """Orchestrator creates queues with correct size limits."""
    orch = PipelineOrchestrator(pipeline_config)

    assert orch.downloader_queue.maxsize == 20


def test_orchestrator_stop_is_idempotent(pipeline_config):
    """Calling stop() multiple times is safe."""
    orch = PipelineOrchestrator(pipeline_config)

    orch.stop()
    orch.stop()  # should not raise


def test_orchestrator_has_stop_event(pipeline_config):
    """Test orchestrator has internal stop flag and stop() sets it."""
    orch = PipelineOrchestrator(pipeline_config)

    assert hasattr(orch, "_stop_event")
    assert orch._stop_event is False

    orch.stop()
    assert orch._stop_event is True


def test_orchestrator_config_storage(pipeline_config):
    """Test orchestrator stores config correctly."""
    orch = PipelineOrchestrator(pipeline_config)

    assert orch.config == pipeline_config
    assert orch.output_dirs is not None  # Should be extracted from config


def test_orchestrator_queue_types(pipeline_config):
    """Test orchestrator creates correct queue types."""
    import queue

    orch = PipelineOrchestrator(pipeline_config)

    assert isinstance(orch.downloader_queue, queue.Queue)


def test_orchestrator_tracker_database_path(pipeline_config):
    """Test orchestrator creates tracker with correct database path (after setup)."""
    orch = PipelineOrchestrator(pipeline_config)
    orch._setup_logging()

    assert orch.tracker is not None
    # Database should be in RADAR_ID/analysis/ directory
    radar_id = pipeline_config.downloader.radar
    expected_db = (
        orch.output_dirs["base"] / radar_id / "analysis" / f"{radar_id}_processing_tracker.db"
    )
    assert expected_db.exists()


def test_orchestrator_mode_from_config(pipeline_config):
    """Test orchestrator respects mode from config."""
    orch = PipelineOrchestrator(pipeline_config)

    # Should use mode from config
    assert orch.config.mode in ["realtime", "historical"]


def test_orchestrator_stop_clears_queues(pipeline_config):
    """Test that stop() sets stop flag and is idempotent."""
    orch = PipelineOrchestrator(pipeline_config)

    # Add some items to queues
    orch.downloader_queue.put("test1")

    orch.stop()

    # Internal stop flag should be set
    assert orch._stop_event is True

    # Calling stop again should be safe
    orch.stop()
    assert orch._stop_event is True


def test_orchestrator_processor_config_accessible(pipeline_config):
    """Test orchestrator can access processor config."""
    orch = PipelineOrchestrator(pipeline_config)

    assert hasattr(orch.config, "processor")
    assert orch.config.processor.max_history >= 0
    assert orch.config.processor.min_file_size > 0


def test_enabled_module_names_includes_post_persistence_modules(pipeline_config):
    """The run header must list phase-3 (post-persistence) modules like cell_volume_stats,
    not only the in-pipeline modules — otherwise the user can't see they're configured.
    """
    orch = PipelineOrchestrator(pipeline_config)

    class _M:
        def __init__(self, name):
            self.name = name

    class _Proc:
        _pipeline_modules = [_M("ingest"), _M("detection")]
        _post_modules = [_M("cell_volume_stats")]

    orch.processor = _Proc()

    names = orch._enabled_module_names()

    assert names[:2] == ("ingest", "detection")
    assert "cell_volume_stats" in names  # phase-3 module surfaced


def test_request_stop_breaks_main_loop(pipeline_config):
    """request_stop() must make the monitoring loop exit so the orchestrator's own
    (joined, non-daemon) thread runs stop()/finalize — instead of a daemon that the
    process kills mid-summary on shutdown.
    """
    orch = PipelineOrchestrator(pipeline_config)
    orch.downloader = object()
    orch.processor = object()
    orch._start_time = time.time()

    orch.request_stop()
    orch._main_loop("realtime")  # returns immediately; would hang if the break is missing
