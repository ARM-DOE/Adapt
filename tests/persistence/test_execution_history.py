# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Execution History: durable, queryable run/module/error/warning records.

Behaviour under test (round-trips that can break): a run is recorded as
'running' then finalized with real aggregates; one module_history row per span
with the right status; errors/warnings are stored and queryable; failure rate by
module is computed from the stored rows.
"""

from datetime import UTC, datetime

import pytest

from adapt.contracts.execution_history import (
    ErrorEvent,
    RunProvenance,
    RunStart,
    RunSummary,
    WarningEvent,
)
from adapt.contracts.observability import SpanRecord
from adapt.persistence.execution_history import ExecutionHistory


def _prov():
    return RunProvenance(
        git_commit="abc123",
        hostname="host",
        username="user",
        python_version="3.11",
        platform="linux",
        software_version="0.4.1",
    )


def _start(run_id="R1"):
    return RunStart(
        run_id=run_id,
        pipeline="nexrad",
        pipeline_version="0.4.1",
        site="KDIX",
        dataset="KDIX",
        instrument="NEXRAD",
        mode="historical",
        start_time=datetime(2026, 6, 28, 0, 0, tzinfo=UTC),
        configuration_hash="deadbeef",
        configuration_file="cfg.yaml",
        provenance=_prov(),
        enabled_modules=("ingest", "detection"),
    )


@pytest.fixture
def history(tmp_path):
    store = ExecutionHistory(tmp_path / "catalog.db")
    yield store
    store.close()


def test_start_run_inserts_running_row_with_provenance(history):
    h = history
    h.start_run(_start())
    runs = h.query_runs()
    assert len(runs) == 1
    assert runs[0]["run_id"] == "R1"
    assert runs[0]["status"] == "running"
    assert runs[0]["git_commit"] == "abc123"
    assert runs[0]["site"] == "KDIX"


def test_record_modules_one_row_per_span_with_status(history):
    h = history
    h.start_run(_start())
    spans = [
        SpanRecord("detection", "t", "s1", "p", 0.0, 2.0, 2.0, "", {}),
        SpanRecord("tracking", "t", "s2", "p", 2.0, 2.5, 0.5, "ValueError: x", {}),
    ]
    h.record_modules("R1", "scan1", spans, recorded_at=datetime(2026, 6, 28, 0, 1, tzinfo=UTC))
    by = {m["module"]: m for m in h.query_modules(run_id="R1")}
    assert by["detection"]["duration_seconds"] == 2.0
    assert by["detection"]["status"] == "ok"
    assert by["tracking"]["status"] == "error"
    assert by["tracking"]["scan_id"] == "scan1"


def test_finalize_run_updates_status_and_aggregates(history):
    h = history
    h.start_run(_start())
    h.finalize_run(
        RunSummary(
            run_id="R1",
            status="success",
            end_time=datetime(2026, 6, 28, 0, 14, tzinfo=UTC),
            duration_seconds=862.0,
            files_processed=842,
            scans_processed=842,
            objects_detected=18431,
            warnings=7,
            errors=0,
            average_scan_time=1.03,
            maximum_scan_time=3.92,
            slowest_stages=(("detection", 371.0), ("tracking", 182.0)),
        )
    )
    run = h.query_runs()[0]
    assert run["status"] == "success"
    assert run["duration_seconds"] == 862.0
    assert run["scans_processed"] == 842
    assert run["objects_detected"] == 18431
    assert run["slowest_stage"] == "detection"
    assert run["slowest_stage_duration"] == 371.0


def test_errors_and_warnings_stored_and_failure_rate(history):
    h = history
    h.start_run(_start())
    ok = [SpanRecord("detection", "t", "s", "p", 0, 1, 1.0, "", {})]
    bad = [SpanRecord("detection", "t", "s", "p", 0, 1, 1.0, "E: x", {})]
    h.record_modules("R1", "scan1", ok, recorded_at=datetime(2026, 6, 28, tzinfo=UTC))
    h.record_modules("R1", "scan2", bad, recorded_at=datetime(2026, 6, 28, tzinfo=UTC))
    h.record_errors(
        "R1",
        [
            ErrorEvent(
                scan_id="scan2",
                module="detection",
                exception_type="ValueError",
                message="x",
                traceback="tb...",
                logger="adapt.detection",
                timestamp=datetime(2026, 6, 28, tzinfo=UTC),
            )
        ],
    )
    h.record_warnings(
        "R1",
        [
            WarningEvent(
                scan_id="scan1",
                module="detection",
                category="slow_execution",
                message="slow",
                logger="adapt.detection",
                timestamp=datetime(2026, 6, 28, tzinfo=UTC),
            )
        ],
    )
    assert h.failure_rate_by_module()["detection"] == 0.5  # 1 of 2 module runs errored
    errs = h.query_errors(run_id="R1")
    assert errs[0]["exception_type"] == "ValueError"
    assert errs[0]["traceback"] == "tb..."
    warns = h.query_warnings(run_id="R1")
    assert warns[0]["category"] == "slow_execution"
