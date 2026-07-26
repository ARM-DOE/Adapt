import queue

import pytest

from adapt.runtime.orchestrator import PipelineOrchestrator

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


class _FakeDownloader:
    def __init__(self, complete: bool, alive: bool, processed: int = 0, expected: int = 0):
        self._complete = complete
        self._alive = alive
        self._processed = processed
        self._expected = expected
        self.stop_called = False

    def is_historical_complete(self) -> bool:
        return self._complete

    def is_alive(self) -> bool:
        return self._alive

    def get_historical_progress(self):
        return self._processed, self._expected

    def stop(self):
        self.stop_called = True

    def join(self, timeout=None):
        self._alive = False


class _FakeProcessor:
    def __init__(self, activity: str | None = "processing scan-1", stuck: bool = False):
        self._alive = True
        self.stop_called = False
        self._activity = activity
        self._stuck = stuck

    def is_alive(self) -> bool:
        return self._alive

    def current_activity(self) -> str | None:
        return self._activity

    def stop(self):
        self.stop_called = True
        if not self._stuck:
            self._alive = False

    def join(self, timeout=None):
        if not self._stuck:
            self._alive = False


class _FakeRepository:
    def __init__(self):
        self.finalized = False
        self.closed = False

    def finalize_run(self, status: str):
        self.finalized = True

    def close(self):
        self.closed = True


def test_historical_complete_returns_true_and_stops_downloader(pipeline_config):
    pipeline_config = pipeline_config.model_copy(update={"mode": "historical"})
    orch = PipelineOrchestrator(pipeline_config)
    orch.downloader = _FakeDownloader(complete=True, alive=False, processed=5, expected=5)
    orch.processor = _FakeProcessor()

    done = orch._check_historical_complete()

    assert done is True
    assert orch.downloader.stop_called is True
    # The processor keeps consuming until the drain finishes; worker shutdown is
    # stop()'s job, so this method never tears the processor down.
    assert orch.processor.stop_called is False


def test_historical_not_complete_returns_false_when_downloader_dead(pipeline_config):
    pipeline_config = pipeline_config.model_copy(update={"mode": "historical"})
    orch = PipelineOrchestrator(pipeline_config)
    orch.downloader = _FakeDownloader(complete=False, alive=False)

    done = orch._check_historical_complete()

    assert done is False


def test_drain_queue_aborts_on_stop_request(pipeline_config):
    """A stop request abandons the backlog instead of draining the full queue."""
    orch = PipelineOrchestrator(pipeline_config)
    q: queue.Queue = queue.Queue()
    for i in range(5):
        q.put({"path": f"file_{i}"})
    orch.request_stop()

    # Returns at once; without the stop check this would block ~300 s waiting
    # for a processor that will never consume the items.
    orch._drain_queue(q, "processor")

    assert q.qsize() == 5


def test_stop_processor_reports_clean_shutdown_when_scan_finishes(pipeline_config, caplog):
    """A processor finishing its in-flight scan stops cleanly with a success line."""
    orch = PipelineOrchestrator(pipeline_config)
    orch.processor = _FakeProcessor(activity="processing scan-1")

    with caplog.at_level("INFO"):
        orch._stop_processor()

    assert orch.processor.stop_called is True
    messages = " ".join(r.message.lower() for r in caplog.records)
    assert "please wait" in messages
    assert "shutdown clean" in messages
    assert not any(r.levelname == "WARNING" for r in caplog.records)


def test_stop_processor_warns_when_stuck(pipeline_config, caplog):
    """A processor still alive past its grace is reported as possibly stuck."""
    orch = PipelineOrchestrator(pipeline_config)
    orch.processor = _FakeProcessor(activity="processing scan-1", stuck=True)

    with caplog.at_level("INFO"):
        orch._stop_processor()

    warnings = [r.message.lower() for r in caplog.records if r.levelname == "WARNING"]
    assert any("may be stuck" in m for m in warnings)
    assert not any("shutdown clean" in r.message.lower() for r in caplog.records)


def test_stop_processor_quiet_when_idle(pipeline_config, caplog):
    """No in-flight scan -> quiet stop, no 'please wait' / 'clean' chatter."""
    orch = PipelineOrchestrator(pipeline_config)
    orch.processor = _FakeProcessor(activity=None)

    with caplog.at_level("INFO"):
        orch._stop_processor()

    assert orch.processor.stop_called is True
    messages = " ".join(r.message.lower() for r in caplog.records)
    assert "please wait" not in messages
    assert "shutdown clean" not in messages


def test_stop_skips_repository_close_when_owned_externally(pipeline_config):
    orch = PipelineOrchestrator(pipeline_config, close_repository_on_stop=False)
    repo = _FakeRepository()
    orch.repository = repo

    orch.stop()

    assert repo.finalized is True
    assert repo.closed is False

    orch.close_repository()
    assert repo.closed is True
