# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Execution history on the store registry: run_modules aggregates + run_events."""

import json
from datetime import UTC, datetime

import pytest

from adapt.contracts.execution_history import ErrorEvent, RunSummary, WarningEvent
from adapt.contracts.observability import SpanRecord
from adapt.persistence.execution_history import StoreExecutionHistory
from adapt.persistence.store import init_store
from adapt.persistence.store_registry import RunStart, StoreRegistry

pytestmark = pytest.mark.unit

NOW = datetime(2026, 8, 13, 18, 0, 0, tzinfo=UTC)


def _span(name: str, duration: float, error: str = "") -> SpanRecord:
    return SpanRecord(
        name=name,
        trace_id="t",
        span_id="s",
        parent_span_id="",
        start=0.0,
        finish=duration,
        duration_s=duration,
        error=error,
    )


def _summary(status: str = "success") -> RunSummary:
    return RunSummary(
        run_id="run-1",
        status=status,
        end_time=NOW,
        duration_seconds=100.0,
        files_processed=3,
        scans_processed=3,
        objects_detected=7,
        warnings=1,
        errors=0,
        average_scan_time=30.0,
        maximum_scan_time=40.0,
        slowest_stages=(),
        failures=1,
    )


@pytest.fixture
def registry(tmp_path):
    root = init_store(tmp_path / "store")
    reg = StoreRegistry(root)
    reg.register_collection("KILX", source_kind="nexrad")
    reg.begin_run(
        RunStart(
            run_id="run-1",
            collection_id="KILX",
            config_hash="h",
            config_json="{}",
            pipeline_version="1.0",
            environment_json="{}",
        )
    )
    yield reg
    reg.close()


@pytest.fixture
def history(registry):
    return StoreExecutionHistory(registry)


class TestModuleAggregates:
    def test_modules_aggregate_across_scans(self, history, registry):
        history.record_modules("run-1", "s1", [_span("ingest", 2.0)], recorded_at=NOW)
        history.record_modules("run-1", "s2", [_span("ingest", 3.0)], recorded_at=NOW)

        rows = registry.run_modules("run-1")

        assert len(rows) == 1
        row = rows[0]
        assert row["module"] == "ingest"
        assert row["status"] == "completed"
        assert row["duration_seconds"] == pytest.approx(5.0)
        assert json.loads(row["detail_json"])["calls"] == 2

    def test_module_error_marks_failed(self, history, registry):
        history.record_modules(
            "run-1", "s1", [_span("tracking", 1.0, error="boom")], recorded_at=NOW
        )

        (row,) = registry.run_modules("run-1")
        assert row["status"] == "failed"


class TestRunEvents:
    def test_warnings_and_errors_land_in_run_events(self, history, registry):
        history.record_warnings(
            "run-1",
            [WarningEvent("s1", "ingest", "deprecation", "old api", "adapt.x", NOW)],
        )
        history.record_errors(
            "run-1",
            [ErrorEvent("s2", "tracking", "ValueError", "bad frame", "tb", "adapt.y", NOW)],
        )

        events = registry.run_events("run-1")

        severities = {(e["severity"], e["module"], e["message"]) for e in events}
        assert ("warning", "ingest", "old api") in severities
        assert ("error", "tracking", "bad frame") in severities
        error = next(e for e in events if e["severity"] == "error")
        assert json.loads(error["context_json"])["exception_type"] == "ValueError"


class TestFinalize:
    @pytest.mark.parametrize(
        ("summary_status", "run_status"),
        [("success", "completed"), ("cancelled", "cancelled"), ("failed", "failed")],
    )
    def test_finalize_maps_status_and_counts(self, history, registry, summary_status, run_status):
        history.finalize_run(_summary(summary_status))

        run = registry.get_run("run-1")
        assert run["status"] == run_status
        assert run["scans_processed"] == 3
        assert run["scans_failed"] == 1
