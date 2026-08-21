# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Scan identity and time ownership through the processor.

The source minted the scan_id (content hash) and parsed the scan_time; the
queue message carries both. The processor must hand them onward — scan_time
into the module context, scan_id into every persisted row — and must reject
messages that lack identity instead of re-deriving it.
"""

import queue
import sqlite3
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.runtime.processor import RadarProcessor
from adapt.utils.time import to_scan_iso

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

_T1 = datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC)
_T2 = datetime(2024, 5, 18, 12, 5, 0, tzinfo=UTC)


def _msg(store_env, tmp_path, name: str, scan_time: datetime) -> dict:
    path = tmp_path / name
    path.write_bytes(name.encode())
    return store_env.acquirer.acquire_file(path, source_uri=str(path), scan_time=scan_time)


def _fake_ds():
    labels = np.zeros((4, 4), dtype=int)
    labels[0, 0] = 1
    return xr.Dataset(
        {
            "reflectivity": (("y", "x"), np.ones((4, 4))),
            "cell_labels": (("y", "x"), labels),
        },
        coords={"x": np.arange(4), "y": np.arange(4)},
        attrs={"z_level_m": 2000},
    )


def _proc(pipeline_config, store_env) -> RadarProcessor:
    return RadarProcessor(
        queue.Queue(),
        pipeline_config,
        collection=store_env.collection,
        registry=store_env.registry,
        run_id=store_env.run_id,
        history=store_env.history,
    )


def _fake_multi_result(scan_time: datetime) -> dict:
    return {
        "projected_ds": _fake_ds(),
        "analysis_ds": _fake_ds(),
        "cell_stats": pd.DataFrame({"cell_label": [1], "area_sqkm": [25.0]}),
        "cell_adjacency": pd.DataFrame(
            {"cell_label_a": [], "cell_label_b": [], "touching_boundary_pixels": []}
        ),
        "tracked_cells": pd.DataFrame(
            {"cell_label": [1], "cell_uid": ["uid-1"], "area": [10.0], "max_reflectivity": [45.0]}
        ),
        "cell_events": pd.DataFrame(
            {
                "event_type": ["INITIATION"],
                "source_cell_uid": [None],
                "target_cell_uid": ["uid-1"],
                "cost": [0.0],
                "event_group_id": [None],
            }
        ),
        "scan_time": scan_time,
    }


def test_queue_scan_time_reaches_module_context(monkeypatch, pipeline_config, store_env, tmp_path):
    proc = _proc(pipeline_config, store_env)
    seen_ctx: dict = {}

    def _fake_single(context):
        seen_ctx.update(context)
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": context.get("scan_time"),
            "num_cells": 1,
        }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: {})
    monkeypatch.setattr(proc, "_post_executor", None)

    # The file name carries NO parseable stamp: the queued value is the only
    # possible source, so this fails if the processor drops it.
    assert proc.process_file(_msg(store_env, tmp_path, "nostamp-a", _T1)) is True

    assert seen_ctx.get("scan_time") == _T1


def test_minted_scan_id_reaches_tracking_rows(monkeypatch, pipeline_config, store_env, tmp_path):
    proc = _proc(pipeline_config, store_env)
    scan_times = [_T1, _T2]

    def _fake_single(context):
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": scan_times.pop(0),
            "num_cells": 1,
        }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: _fake_multi_result(_T2))
    monkeypatch.setattr(proc, "_post_executor", None)

    assert proc.process_file(_msg(store_env, tmp_path, "nostamp-a", _T1)) is True
    second = _msg(store_env, tmp_path, "nostamp-b", _T2)
    assert proc.process_file(second) is True

    conn = sqlite3.connect(str(store_env.collection.products_path))
    try:
        rows = conn.execute(
            "SELECT scan_id, scan_time FROM cells_by_scan WHERE run_id=?",
            (store_env.run_id,),
        ).fetchall()
    finally:
        conn.close()
    assert rows == [(second["scan_id"], to_scan_iso(_T2))]


def test_message_without_identity_raises(pipeline_config, store_env):
    proc = _proc(pipeline_config, store_env)
    with pytest.raises(ValueError, match="scan identity"):
        proc.process_file({"scan_time": _T1, "queued_at": 0.0})


def test_bare_string_path_raises(pipeline_config, store_env):
    proc = _proc(pipeline_config, store_env)
    with pytest.raises(TypeError, match="queue message"):
        proc.process_file("/fake/TEST_20240518_120000")


def test_scan_registration_artifacts_and_attrs_share_identity(
    monkeypatch, pipeline_config, store_env, tmp_path
):
    """After a scan persists, one identity spans scans, artifacts, and ds.attrs."""
    proc = _proc(pipeline_config, store_env)
    scan_times = [_T1, _T2]

    def _fake_single(context):
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "num_cells": 1,
        }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: _fake_multi_result(_T2))
    monkeypatch.setattr(proc, "_post_executor", None)

    first = _msg(store_env, tmp_path, "nostamp-a", scan_times[0])
    second = _msg(store_env, tmp_path, "nostamp-b", scan_times[1])
    assert proc.process_file(first) is True
    assert proc.process_file(second) is True

    catalog = store_env.collection.catalog
    scans = [
        (s["scan_id"], s["scan_time"], s["source_file_name"])
        for s in (
            catalog.get_scan(store_env.run_id, first["scan_id"]),
            catalog.get_scan(store_env.run_id, second["scan_id"]),
        )
    ]
    assert scans == [
        (first["scan_id"], to_scan_iso(_T1), "nostamp-a"),
        (second["scan_id"], to_scan_iso(_T2), "nostamp-b"),
    ]

    # The catalog row for the analysis NetCDF carries the same identity and
    # the same canonical time string as the tracking tables.
    items = catalog.list_artifacts(run_id=store_env.run_id, artifact_type="segmentation2d")
    assert len(items) == 1
    assert items[0]["scan_id"] == second["scan_id"]
    assert items[0]["observation_time"] == to_scan_iso(_T2)

    # And the artifact itself is self-describing: identity travels in attrs.
    ds = xr.open_dataset(store_env.collection.objects_dir / items[0]["object_name"])
    try:
        assert ds.attrs["scan_id"] == second["scan_id"]
        assert ds.attrs["scan_time"] == to_scan_iso(_T2)
    finally:
        ds.close()


def test_message_scan_time_wins_over_module_results(
    monkeypatch, pipeline_config, store_env, tmp_path
):
    """The source boundary owns scan_time: values modules put in their results
    never override the queued time in persisted rows or the scans registry."""
    proc = _proc(pipeline_config, store_env)
    wrong = datetime(2030, 1, 1, 0, 0, 0, tzinfo=UTC)

    def _fake_single(context):
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": wrong,  # a module lying about time must not matter
            "num_cells": 1,
        }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: _fake_multi_result(wrong))
    monkeypatch.setattr(proc, "_post_executor", None)

    assert proc.process_file(_msg(store_env, tmp_path, "nostamp-a", _T1)) is True
    assert proc.process_file(_msg(store_env, tmp_path, "nostamp-b", _T2)) is True

    conn = sqlite3.connect(str(store_env.collection.products_path))
    try:
        cell_times = {
            r[0]
            for r in conn.execute(
                "SELECT scan_time FROM cells_by_scan WHERE run_id=?",
                (store_env.run_id,),
            )
        }
    finally:
        conn.close()
    conn = sqlite3.connect(str(store_env.collection.catalog.db_path))
    try:
        scan_times = {
            r[0]
            for r in conn.execute("SELECT scan_time FROM scans WHERE run_id=?", (store_env.run_id,))
        }
    finally:
        conn.close()
    assert cell_times == {to_scan_iso(_T2)}
    assert scan_times == {to_scan_iso(_T1), to_scan_iso(_T2)}
    assert to_scan_iso(wrong) not in scan_times
