"""The processor seeds ``prior_scan`` and keeps injected keys out of history.

Executors are patched to return the full context plus their outputs, exactly
as ``GraphExecutor.run`` does, so the history-retention test exercises the
real merge path.
"""

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from tests.helpers.queue_msg import msg as _msg

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

T1 = datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC)
T2 = T1 + timedelta(minutes=5)


def _fake_ds():
    return xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((4, 4))),
            "cell_labels": (("y", "x"), np.zeros((4, 4), dtype=int)),
        },
        coords={"x": np.arange(4), "y": np.arange(4)},
        attrs={"z_level_m": 2000},
    )


def _wire(proc, monkeypatch, seen: list):
    def _single(ctx):
        seen.append(ctx)
        return {
            **ctx,
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "num_cells": 0,
            "seed_carry": (),
        }

    def _multi(ctx):
        return {
            **ctx,
            "projected_ds": _fake_ds(),
            "cell_stats": pd.DataFrame(),
            "cell_adjacency": pd.DataFrame(),
        }

    monkeypatch.setattr(proc._executors[1], "run", _single)
    monkeypatch.setattr(proc._executors[2], "run", _multi)
    monkeypatch.setattr(proc._router, "persist", lambda modules, result, meta: None)


def test_prior_scan_is_none_first_then_the_previous_entry(
    tmp_path, monkeypatch, make_processor, store_env
):
    proc = make_processor()
    seen: list = []
    _wire(proc, monkeypatch, seen)

    assert proc.process_file(_msg(store_env, tmp_path, "f1", scan_time=T1)) is True
    assert proc.process_file(_msg(store_env, tmp_path, "f2", scan_time=T2)) is True

    assert seen[0]["prior_scan"] is None
    assert seen[1]["prior_scan"]["scan_time"] == T1


def test_prior_scan_is_none_when_the_time_gap_is_too_large(
    tmp_path, monkeypatch, make_processor, store_env
):
    proc = make_processor()
    seen: list = []
    _wire(proc, monkeypatch, seen)
    too_late = T1 + timedelta(minutes=proc._max_time_gap_minutes + 1)

    proc.process_file(_msg(store_env, tmp_path, "f1", scan_time=T1))
    proc.process_file(_msg(store_env, tmp_path, "f2", scan_time=too_late))

    assert seen[1]["prior_scan"] is None


def test_history_entries_do_not_retain_injected_context_keys(
    tmp_path, monkeypatch, make_processor, store_env
):
    proc = make_processor()
    _wire(proc, monkeypatch, [])

    proc.process_file(_msg(store_env, tmp_path, "f1", scan_time=T1))
    proc.process_file(_msg(store_env, tmp_path, "f2", scan_time=T2))

    latest = proc._scan_history[-1]
    assert "prior_scan" not in latest
    assert "scan_history" not in latest
