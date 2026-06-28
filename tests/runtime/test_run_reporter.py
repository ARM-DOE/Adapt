# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""RunReporter: the quiet console's run header, progress line, and summary.

Behaviour under test: the header/summary contain the run's identity and real
stats (durations formatted, thousands-separated counts, slowest stage), and the
reporter emits console-tagged records so the ConsoleFilter lets them through.
"""

import logging
from datetime import UTC, datetime

from adapt.contracts.execution_history import RunProvenance, RunStart, RunSummary
from adapt.runtime.run_reporter import RunReporter, format_duration, format_header, format_summary


def _start():
    return RunStart(
        run_id="2026JUN28-0206-KDIX",
        pipeline="nexrad",
        pipeline_version="0.4.1",
        site="KDIX",
        dataset="KDIX",
        instrument="NEXRAD",
        mode="historical",
        start_time=datetime(2026, 6, 28, 0, 0, tzinfo=UTC),
        configuration_hash="9f2cdeadbeef",
        configuration_file="cfg.yaml",
        provenance=RunProvenance("abc1234def", "host", "user", "3.11.0", "linux", "0.4.1"),
        enabled_modules=("ingest", "detection", "tracking"),
    )


def _summary():
    return RunSummary(
        run_id="2026JUN28-0206-KDIX",
        status="success",
        end_time=datetime(2026, 6, 28, 0, 14, 22, tzinfo=UTC),
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


def test_format_duration_is_human_compact():
    assert format_duration(862.0) == "14m22s"
    assert format_duration(45.0) == "45s"
    assert format_duration(3725.0) == "1h2m5s"


def test_header_shows_identity_mode_version_commit_modules():
    out = format_header(_start())
    assert "2026JUN28-0206-KDIX" in out
    assert "KDIX" in out
    assert "historical" in out
    assert "nexrad" in out and "0.4.1" in out
    assert "abc1234" in out  # short commit
    assert "ingest" in out and "detection" in out and "tracking" in out


def test_summary_shows_status_counts_and_slowest_stage():
    out = format_summary(_summary())
    assert "SUCCESS" in out
    assert "14m22s" in out
    assert "842" in out
    assert "18,431" in out  # thousands separator
    assert "1.03" in out and "3.92" in out
    assert "detection" in out


def test_reporter_emits_console_tagged_records():
    captured: list[logging.LogRecord] = []

    class _Cap(logging.Handler):
        def emit(self, record):
            captured.append(record)

    log = logging.getLogger("adapt.test.run_reporter")
    log.handlers[:] = [_Cap()]
    log.setLevel(logging.DEBUG)
    log.propagate = False

    reporter = RunReporter(logger=log)
    reporter.progress("412/842 scans")
    assert captured[0].console is True
    assert "412/842 scans" in captured[0].getMessage()


def _span(name, duration_s):
    from adapt.contracts.observability import SpanRecord

    return SpanRecord(
        name=name,
        trace_id="t",
        span_id="s",
        parent_span_id="p",
        start=0.0,
        finish=duration_s,
        duration_s=duration_s,
        error="",
        metadata={},
    )


def test_format_scan_shows_each_stage_with_timing_cells_and_total():
    """The per-scan progress line is built from the drained module spans — stage
    names with real per-stage durations, the cell count, and the total — so the
    console tells you what ran and how long without any module-level print().
    """
    spans = [_span("ingest", 1.2), _span("segmentation", 0.8), _span("detection", 0.4)]
    from adapt.runtime.run_reporter import format_scan

    line = format_scan("KOHX_180851", spans, n_cells=5)

    assert "KOHX_180851" in line
    assert "ingest 1.2s" in line
    assert "segmentation 0.8s" in line
    assert "detection 0.4s" in line
    assert "5 cells" in line
    assert "2.4s" in line  # total of the stage durations


def test_reporter_scan_emits_one_console_tagged_line():
    from adapt.runtime.run_reporter import RunReporter

    captured: list[logging.LogRecord] = []

    class _Cap(logging.Handler):
        def emit(self, record):
            captured.append(record)

    logger = logging.getLogger("adapt.test.scan")
    logger.addHandler(_Cap())
    logger.setLevel(logging.INFO)

    RunReporter(logger).scan("KOHX_180851", [_span("ingest", 1.0)], n_cells=3)

    assert len(captured) == 1
    assert captured[0].console is True
    assert "KOHX_180851" in captured[0].getMessage()
