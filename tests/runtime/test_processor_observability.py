# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""The processor injects the provider, binds scan context, and emits scan metrics.

Behaviour under test: processing a file opens a "scan" span, increments
files_processed_total, records a scan_processing_time observation, and the
scan_id is visible in the bound context while the executors run.
"""

import logging
import queue
import random
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.runtime.observability import ObsSettings, build_observability
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _fake_ds():
    return xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((4, 4))),
            "cell_labels": (("y", "x"), np.zeros((4, 4), dtype=int)),
        },
        coords={"x": np.arange(4), "y": np.arange(4)},
        attrs={"z_level_m": 2000},
    )


def _obs():
    return build_observability(
        ObsSettings(),
        clock=lambda: 0.0,
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        rng=random.Random(0),
    )


def test_processor_emits_scan_metrics_and_binds_scan_id(
    monkeypatch, pipeline_config, pipeline_output_dirs, test_repository
):
    obs = _obs()
    proc = RadarProcessor(
        queue.Queue(),
        pipeline_config,
        pipeline_output_dirs,
        repository=test_repository,
        observability=obs,
    )

    seen_scan_ids: list[str] = []
    scan_times = [
        datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC),
        datetime(2024, 5, 18, 12, 5, 0, tzinfo=UTC),
    ]

    def _fake_single(context):
        seen_scan_ids.append(obs.current().scan_id)  # context bound by process_file
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": scan_times.pop(0),
            "num_cells": 0,
        }

    fake_multi = {
        "projected_ds": _fake_ds(),
        "cell_stats": pd.DataFrame(),
        "cell_adjacency": pd.DataFrame(),
    }
    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: fake_multi)
    monkeypatch.setattr(proc._router, "persist", lambda modules, result, meta: None)

    assert proc.process_file("/fake/file_1") is True
    assert proc.process_file("/fake/file_2") is True

    assert obs.metrics.counter_total("files_processed_total") == 2.0
    assert len(obs.metrics.histogram_values("scan_processing_time")) == 2
    assert seen_scan_ids == ["file_1", "file_2"]  # scan_id bound while executors ran
    assert obs.drain_spans() == []  # processor drained each scan's spans for history


def test_processor_logs_single_enriched_traceback_on_scan_failure(
    monkeypatch, caplog, pipeline_config, pipeline_output_dirs, test_repository
):
    """A failing scan must log exactly one stack trace, carrying scan/elapsed/type.

    The bare "Error processing <path>" line gave no stage, scan, elapsed, or
    exception type and risked a second trace if the error re-bubbled. The handler
    must report the failure once, with enough context to act on without log-diving.
    """
    obs = _obs()
    proc = RadarProcessor(
        queue.Queue(),
        pipeline_config,
        pipeline_output_dirs,
        repository=test_repository,
        observability=obs,
    )

    def _boom(context):
        raise ValueError("kaboom")

    monkeypatch.setattr(proc._executors[1], "run", _boom)

    with caplog.at_level(logging.ERROR):
        result = proc.process_file("/fake/file_9")

    assert result is False
    traced = [r for r in caplog.records if r.exc_info]
    assert len(traced) == 1  # one trace per failure, never duplicated
    rec = traced[0]
    assert "file_9" in rec.getMessage()
    assert "ValueError" in rec.getMessage()
    assert rec.error_type == "ValueError"
    assert isinstance(rec.elapsed_s, float)


def test_processor_emits_per_scan_progress_from_spans(
    monkeypatch, pipeline_config, pipeline_output_dirs, test_repository
):
    """After each scan the processor hands the captured module spans to the reporter,
    so the console progress line is driven by telemetry, not module-level prints.
    """
    obs = _obs()

    calls: list[tuple] = []

    class _Reporter:
        def scan(self, scan_id, spans, n_cells):
            calls.append((scan_id, [s.name for s in spans], n_cells))

    proc = RadarProcessor(
        queue.Queue(),
        pipeline_config,
        pipeline_output_dirs,
        repository=test_repository,
        observability=obs,
        reporter=_Reporter(),
    )

    def _fake_single(context):
        # Mimic GraphExecutor opening one span per module so the drained telemetry
        # carries real stage records for the progress line.
        for stage in ("ingest", "detection"):
            with obs.span(stage):
                pass
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC),
            "num_cells": 0,
        }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    fake_multi = {
        "projected_ds": _fake_ds(),
        "cell_stats": pd.DataFrame(),
        "cell_adjacency": pd.DataFrame(),
    }
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: fake_multi)
    monkeypatch.setattr(proc._router, "persist", lambda modules, result, meta: None)

    assert proc.process_file("/fake/file_7") is True

    assert len(calls) == 1
    scan_id, stage_names, _ = calls[0]
    assert scan_id == "file_7"
    assert stage_names  # the per-scan line carries the executed stages
