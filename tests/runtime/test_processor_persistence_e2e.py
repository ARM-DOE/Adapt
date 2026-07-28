# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""End-to-end persistence smoke: process_file -> OutputRouter -> repository.

Science modules are faked; persistence is real. After two scans the catalog
must contain the analysis NetCDF (with the uid LUT), the analysis2d Parquet
rows, and the tracking tables — all routed purely from module-declared specs,
and all discoverable through the generic read API.
"""

import queue
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.api.client import RepositoryClient
from adapt.persistence.track_store import TrackStore
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

_T1 = datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC)
_T2 = datetime(2024, 5, 18, 12, 5, 0, tzinfo=UTC)


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


def test_process_file_persists_all_declared_outputs(
    monkeypatch, pipeline_config, pipeline_output_dirs, test_repository
):
    proc = RadarProcessor(
        queue.Queue(), pipeline_config, pipeline_output_dirs, repository=test_repository
    )

    scan_times = [_T1, _T2]

    def _fake_single(context):
        return {
            "grid_ds": _fake_ds(),
            "grid_ds_2d": _fake_ds(),
            "segmented_ds": _fake_ds(),
            "scan_time": scan_times.pop(0),
            "num_cells": 1,
        }

    fake_multi_result = {
        "projected_ds": _fake_ds(),
        "analysis_ds": _fake_ds(),
        "cell_stats": pd.DataFrame({"cell_label": [1], "area_sqkm": [25.0]}),
        "cell_adjacency": pd.DataFrame(
            {"cell_label_a": [], "cell_label_b": [], "touching_boundary_pixels": []}
        ),
        "tracked_cells": pd.DataFrame({"cell_label": [1], "cell_uid": ["uid-1"]}),
        "cell_events": pd.DataFrame(
            {
                "event_type": ["INITIATION"],
                "source_cell_uid": [None],
                "target_cell_uid": ["uid-1"],
                "cost": [0.0],
                "event_group_id": [None],
            }
        ),
        "scan_time": _T2,
    }

    monkeypatch.setattr(proc._executors[1], "run", _fake_single)
    monkeypatch.setattr(proc._executors[2], "run", lambda ctx: fake_multi_result)
    # Enrichment (phase 3) is covered by test_processor_enrich.py; the fake
    # executors don't carry the module configs it needs.
    monkeypatch.setattr(proc, "_post_executor", None)

    assert proc.process_file("/fake/TEST_20240518_120000") is True
    assert proc.process_file("/fake/TEST_20240518_120500") is True

    # Analysis NetCDF artifact written from tracking's declared analysis_ds spec.
    nc_items = test_repository.query(product_type="segmentation2d")
    assert len(nc_items) == 1
    ds = test_repository.open_dataset(nc_items[0]["artifact_id"])
    assert ds.attrs["radar"] == "TEST_RADAR"
    ds.close()

    # Parquet rows from analysis's declared specs.
    pq_items = test_repository.query(product_type="analysis2d")
    assert len(pq_items) >= 1

    # Tracking tables from the declared TrackTablesWrite spec.
    with TrackStore(test_repository.catalog.db_path) as store:
        rows = store.get_cells_by_scan(test_repository.run_id, _T2)
    assert rows["cell_uid"].tolist() == ["uid-1"]

    # Generic read API discovers the core tables in the same repository.
    client = RepositoryClient(test_repository.base_dir)
    try:
        tables = set(client.tables("TEST_RADAR")["table_name"])
        assert {"cells_by_scan", "cell_events", "cell_tracks"} <= tables
        tracked = client.table("cells_by_scan", radar="TEST_RADAR", run_id=test_repository.run_id)
        assert tracked["cell_uid"].tolist() == ["uid-1"]
    finally:
        client.close()
