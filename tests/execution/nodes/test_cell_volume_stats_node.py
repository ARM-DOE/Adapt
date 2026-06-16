# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Node-level tests for CellVolumeStatsModule — covers the three review fixes:

A) build_config does not read non-existent global var_names (no AttributeError).
B) run() with no grid_ds_3d returns an empty frame that PASSES the output contract.
C) a datetime scan_time round-trips through the writer to the canonical join key.
"""

import sqlite3
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

from adapt.contracts import check_cell_volume_stats  # noqa: E402
from adapt.execution.nodes.cell_volume_stats import CellVolumeStatsModule  # noqa: E402
from adapt.modules.cell_volume_stats.config import CellVolumeStatsConfig  # noqa: E402
from adapt.persistence.module_output import ModuleOutputWriter  # noqa: E402
from adapt.utils.time import to_scan_iso  # noqa: E402


@pytest.fixture(autouse=True, scope="module")
def _restore_default_registration():
    """cell_volume_stats is a default-pipeline module; ensure it stays registered
    for other test files even if a test here unregisters it."""
    yield
    from adapt.execution.module_registry import registry

    if "cell_volume_stats" not in registry:
        registry.register(CellVolumeStatsModule)


def _grid_3d() -> xr.Dataset:
    nz, ny, nx = 6, 4, 4
    dbz = np.full((nz, ny, nx), np.nan)
    dbz[:3, 1:3, 1:3] = 45.0  # a compact cell footprint at (y,x) 1:3
    return xr.Dataset(
        {"reflectivity": (["z", "y", "x"], dbz)},
        coords={
            "z": np.arange(nz) * 500.0,
            "y": np.arange(ny) * 1000.0,
            "x": np.arange(nx) * 1000.0,
        },
    )


def _segmented() -> xr.Dataset:
    labels = np.zeros((4, 4), dtype=np.int32)
    labels[1:3, 1:3] = 1
    return xr.Dataset(
        {"cell_labels": (["y", "x"], labels)},
        coords={"y": np.arange(4) * 1000.0, "x": np.arange(4) * 1000.0},
    )


class TestBuildConfigFixA:
    def test_build_config_injects_globals_without_error(self, internal_config):
        cfg = CellVolumeStatsModule.build_config(internal_config)
        assert isinstance(cfg, CellVolumeStatsConfig)
        assert cfg.reflectivity_var == internal_config.global_.var_names.reflectivity
        assert cfg.labels_var == internal_config.global_.var_names.cell_labels
        assert cfg.z_coord == internal_config.global_.coord_names.z
        # polarimetric var names fall back to config defaults (not in global var_names)
        assert cfg.zdr_var == "differential_reflectivity"


class TestEmptyPathFixB:
    def test_no_grid_returns_contract_safe_empty(self, internal_config):
        mod = CellVolumeStatsModule()
        ctx = {
            "cell_volume_stats_config": CellVolumeStatsModule.build_config(internal_config),
            "grid_ds_3d": None,
            "segmented_ds": _segmented(),
            "tracked_cells": pd.DataFrame([{"cell_label": 1, "cell_uid": "a"}]),
            "run_id": "R1",
            "scan_time": datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC),
        }
        out = mod.run(ctx)
        df = out["cell_volume_stats_rows"]
        assert df.empty
        check_cell_volume_stats(df)  # must NOT raise


class TestRunAndWriteFixC:
    def test_datetime_scan_time_round_trips_to_join_key(self, internal_config, tmp_path):
        mod = CellVolumeStatsModule()
        scan_time = datetime(2024, 1, 1, 12, 0, 0)  # naive, like NEXRAD strptime
        ctx = {
            "cell_volume_stats_config": CellVolumeStatsModule.build_config(internal_config),
            "grid_ds_3d": _grid_3d(),
            "segmented_ds": _segmented(),
            "tracked_cells": pd.DataFrame([{"cell_label": 1, "cell_uid": "a"}]),
            "run_id": "R1",
            "scan_time": scan_time,
        }
        out = mod.run(ctx)
        df = out["cell_volume_stats_rows"]
        assert not df.empty
        check_cell_volume_stats(df)

        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, CellVolumeStatsModule.persistence[0]).write(df)
        conn = sqlite3.connect(str(db))
        try:
            iso, unix = conn.execute(
                "SELECT scan_time, scan_time_unix FROM cell_volume_stats LIMIT 1"
            ).fetchone()
            vol = conn.execute("SELECT cell_volume_km3 FROM cell_volume_stats").fetchone()[0]
        finally:
            conn.close()
        assert iso == to_scan_iso(scan_time)  # joins to cells_by_scan
        assert unix == int(scan_time.replace(tzinfo=UTC).timestamp())
        assert vol > 0  # the cell has volume


class TestNodeDeclarations:
    def test_enrich_declarations(self):
        assert CellVolumeStatsModule.pipeline_phase == 3
        assert "grid_ds_3d" in CellVolumeStatsModule.inputs
        assert CellVolumeStatsModule.persistence[0].table == "cell_volume_stats"
        assert CellVolumeStatsModule.persistence[0].primary_key == (
            "run_id",
            "scan_time",
            "cell_uid",
        )

    def test_registered(self):
        from adapt.execution.module_registry import registry
        from adapt.execution.pipeline_builder import _ensure_modules_registered

        _ensure_modules_registered(["adapt.execution.nodes.cell_volume_stats"])
        assert "cell_volume_stats" in registry
        # cell_volume_stats is a default module — leave it registered.
