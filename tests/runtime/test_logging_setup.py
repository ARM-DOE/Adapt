# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Structured logging setup: context injection, JSON output, quiet console."""

import json
import logging
import random
from datetime import UTC, datetime

import pytest

from adapt.runtime.logging_setup import (
    ConsoleFilter,
    ContextFilter,
    JsonFormatter,
    configure_logging,
)
from adapt.runtime.observability import ObsSettings, build_observability


@pytest.fixture(autouse=True)
def _restore_root_logger():
    root = logging.getLogger()
    saved = root.handlers[:]
    saved_level = root.level
    yield
    root.handlers[:] = saved
    root.setLevel(saved_level)


def _obs(**settings):
    return build_observability(
        ObsSettings(**settings),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def _record(level=logging.INFO, msg="hello", **extra):
    rec = logging.LogRecord("adapt.x", level, __file__, 1, msg, None, None)
    for k, v in extra.items():
        setattr(rec, k, v)
    return rec


def test_context_filter_attaches_current_ids() -> None:
    obs = _obs()
    rec = _record()
    with obs.bind(scan_id="512", dataset_id="KDIX"):
        ContextFilter().filter(rec)
    assert rec.scan_id == "512"
    assert rec.dataset_id == "KDIX"


def test_json_formatter_emits_structured_fields_without_trailing_z() -> None:
    rec = _record(scan_id="512", n_cells=42)
    rec.scan_id = "512"  # as ContextFilter would set
    out = json.loads(JsonFormatter().format(rec))
    assert out["level"] == "INFO"
    assert out["logger"] == "adapt.x"
    assert out["msg"] == "hello"
    assert out["scan_id"] == "512"
    assert out["n_cells"] == 42  # arbitrary extra field surfaces
    assert "Z" not in out["ts"]  # the "...Z" literal is fitness-pinned to utils/time


def test_json_formatter_includes_exception() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        rec = logging.LogRecord("adapt.x", logging.ERROR, __file__, 1, "fail", None, sys.exc_info())
    out = json.loads(JsonFormatter().format(rec))
    assert "ValueError: boom" in out["exc"]


def test_console_filter_passes_warning_and_console_tagged_only() -> None:
    cf = ConsoleFilter()
    assert cf.filter(_record(level=logging.WARNING)) is True
    assert cf.filter(_record(level=logging.INFO, console=True)) is True
    assert cf.filter(_record(level=logging.INFO)) is False
    assert cf.filter(_record(level=logging.DEBUG)) is False


def test_configure_logging_requires_log_path_for_json() -> None:
    with pytest.raises(ValueError, match="log_path"):
        configure_logging(ObsSettings(json_logs=True), log_path=None)


def test_configure_logging_creates_dir_and_is_idempotent(tmp_path) -> None:
    log_path = tmp_path / "logs" / "pipeline_KDIX.log"
    configure_logging(ObsSettings(json_logs=True, console_logs=True), log_path=log_path)
    first = len(logging.getLogger().handlers)
    configure_logging(ObsSettings(json_logs=True, console_logs=True), log_path=log_path)
    second = len(logging.getLogger().handlers)
    assert log_path.parent.is_dir()
    assert first == second  # handlers cleared+re-added, no accumulation


def _console_stream_output(settings, records):
    """Run records through a freshly configured root logger; return console text."""
    import io

    configure_logging(settings, log_path=None)
    root = logging.getLogger()
    stream = io.StringIO()
    console = next(h for h in root.handlers if isinstance(h, logging.StreamHandler))
    console.stream = stream
    log = logging.getLogger("adapt.run")
    for level, msg, console_tag in records:
        log.log(level, msg, extra={"console": True} if console_tag else {})
    return stream.getvalue()


def test_console_shows_info_console_tagged_lines_despite_warning_threshold() -> None:
    """Console-tagged INFO (run header/progress/summary) must reach the console even
    when console_level is WARNING. The handler level must not pre-empt ConsoleFilter:
    that gating bug made the run header and end-of-run summary vanish entirely.
    """
    out = _console_stream_output(
        ObsSettings(console_logs=True, console_level="WARNING"),
        [
            (logging.INFO, "RUN HEADER", True),
            (logging.INFO, "routine chatter", False),
            (logging.WARNING, "a real warning", False),
        ],
    )
    assert "RUN HEADER" in out  # console-tagged INFO survives
    assert "a real warning" in out  # warnings always survive
    assert "routine chatter" not in out  # plain INFO stays off the console


def test_verbose_console_level_lets_plain_info_through() -> None:
    """console_level=INFO (the -v firehose) shows plain INFO too."""
    out = _console_stream_output(
        ObsSettings(console_logs=True, console_level="INFO"),
        [(logging.INFO, "routine chatter", False), (logging.DEBUG, "debug noise", False)],
    )
    assert "routine chatter" in out
    assert "debug noise" not in out
