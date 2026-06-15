"""Processor orchestration test using fake module outputs.

The processor runs the phase-1 executor (ingest+detection) to build a
2-frame history, then runs the phase-2 executor (projection+analysis+tracking).
These tests patch the executors to avoid touching real scientific modules.
"""

import queue
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

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


def test_processor_accepts_fake_grid(
    tmp_path, monkeypatch, pipeline_config, pipeline_output_dirs, test_repository
):
    """Processor handles a successful 2-frame pipeline result correctly."""
    in_q = queue.Queue()
    proc = RadarProcessor(in_q, pipeline_config, pipeline_output_dirs, repository=test_repository)

    scan_times = [
        datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC),
        datetime(2024, 5, 18, 12, 5, 0, tzinfo=UTC),
    ]

    def _fake_single(context):
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": scan_times.pop(0),
            "num_cells": 0,
        }

    fake_multi_result = {
        "projected_ds": _fake_ds(),
        "cell_stats": pd.DataFrame(),
        "cell_adjacency": pd.DataFrame(),
    }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: fake_multi_result)
    monkeypatch.setattr(proc._router, "persist", lambda modules, result, meta: None)

    ok1 = proc.process_file("/fake/file_1")
    ok2 = proc.process_file("/fake/file_2")
    assert ok1 is True
    assert ok2 is True
