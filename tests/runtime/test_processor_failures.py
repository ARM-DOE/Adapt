"""Tests for RadarProcessor error handling and success paths.

The processor groups modules by required_history and runs each group when
the rolling scan history has enough entries. These tests patch the executors
to keep the focus on orchestration rather than scientific behavior.
"""

import queue
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.contracts import ContractViolation
from adapt.runtime.processor import RadarProcessor
from tests.helpers.queue_msg import msg as _msg

# Distinct per-file times: two different scans sharing one nominal second
# is rejected by the scans registry (UNIQUE(run_id, scan_time)).
_MT1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
_MT2 = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _make_proc(pipeline_config, store_env):
    q = queue.Queue()
    return RadarProcessor(
        q,
        pipeline_config,
        collection=store_env.collection,
        registry=store_env.registry,
        run_id=store_env.run_id,
        history=store_env.history,
    )


def _fake_ds():
    return xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((4, 4))),
            "cell_labels": (("y", "x"), np.zeros((4, 4), dtype=int)),
        },
        coords={"x": np.arange(4), "y": np.arange(4)},
        attrs={"z_level_m": 2000},
    )


def _fake_single_result(scan_time):
    """Return what _executors[1].run() would produce."""
    return {
        "grid_ds": _fake_ds(),
        "grid_ds_2d": _fake_ds(),
        "segmented_ds": _fake_ds(),
        "scan_time": scan_time,
        "num_cells": 0,
    }


# ── Error paths ───────────────────────────────────────────────────────────────


def test_process_file_pipeline_exception_returns_false(
    monkeypatch, pipeline_config, store_env, tmp_path
):
    """process_file returns False when executor raises a non-contract exception."""
    proc = _make_proc(pipeline_config, store_env)

    # Seed one entry in history so required_history=1 executor is eligible to run
    proc._scan_history.append(_fake_single_result(datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)))

    def _boom(context):
        raise OSError("disk failure")

    monkeypatch.setattr(proc._executors[1], "run", _boom)

    ok = proc.process_file(_msg(store_env, tmp_path, "file"))
    assert ok is False


def test_process_file_contract_violation_stops_processor(
    monkeypatch, pipeline_config, store_env, tmp_path
):
    """ContractViolation during multi-frame executor causes processor to stop."""
    proc = _make_proc(pipeline_config, store_env)

    t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
    scan_times = [t1, t2]

    def _fake_single(context):
        return _fake_single_result(scan_times.pop(0))

    def _boom_multi(context):
        raise ContractViolation("bad grid")

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", _boom_multi)

    ok1 = proc.process_file(_msg(store_env, tmp_path, "file_1", scan_time=t1))
    ok2 = proc.process_file(_msg(store_env, tmp_path, "file_2", scan_time=t2))
    assert ok1 is True
    assert ok2 is False
    assert proc.stopped()


# ── Success path ──────────────────────────────────────────────────────────────


def test_process_file_success_saves_netcdf_and_returns_true(
    monkeypatch, pipeline_config, store_env, tmp_path
):
    """process_file returns True and attempts NetCDF save on success."""
    proc = _make_proc(pipeline_config, store_env)

    scan_times = [
        datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC),
    ]

    def _fake_single(context):
        return _fake_single_result(scan_times.pop(0))

    fake_result = {
        "projected_ds": _fake_ds(),
        "scan_time": datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC),
        "cell_stats": pd.DataFrame({"cell_label": [1]}),
        "cell_adjacency": pd.DataFrame(),
    }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: fake_result)

    persisted = []
    monkeypatch.setattr(
        proc._router, "persist", lambda modules, result, meta: persisted.append(meta)
    )

    ok1 = proc.process_file(_msg(store_env, tmp_path, "file_1", scan_time=_MT1))
    ok2 = proc.process_file(_msg(store_env, tmp_path, "file_2", scan_time=_MT2))
    assert ok1 is True
    assert ok2 is True
    assert len(persisted) == 2  # router invoked once per processed file


def test_process_file_skips_completed_scan(monkeypatch, pipeline_config, store_env, tmp_path):
    """process_file skips a scan the catalog already marks complete."""
    proc = _make_proc(pipeline_config, store_env)
    message = _msg(store_env, tmp_path, "file")
    catalog = store_env.collection.catalog
    for product in ("gridded3d", "segmentation2d"):
        catalog.link_scan_product(
            store_env.run_id, message["scan_id"], product, artifact_id=message["artifact_id"]
        )
    catalog.link_scan_product(
        store_env.run_id, message["scan_id"], "cell_stats", table_name="cell_stats"
    )

    called = []
    monkeypatch.setattr(
        proc._executors[1],
        "run",
        lambda ctx: called.append(1) or _fake_single_result(datetime.now(UTC)),
    )

    ok = proc.process_file(message)
    assert ok is True
    assert called == []  # single executor was NOT called
