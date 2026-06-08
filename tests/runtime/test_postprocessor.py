# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""PostProcessor enriches an existing repository by composing the shared infra.

It reuses the registry, GraphBuilder/GraphExecutor (the real resolution path),
and ModuleOutputWriter — writing only extension tables, never core tables.
"""

import sqlite3
import sys
from datetime import UTC, datetime

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from adapt.execution.module_registry import registry
from adapt.modules.base import POSTPROCESS_PHASE, BaseModule
from adapt.persistence import ProductType
from adapt.persistence.module_output import OutputTableSpec
from adapt.runtime.postprocessor import PostProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


@pytest.fixture(autouse=True)
def _clean_lma_registration():
    """PostProcessor.run auto-imports the real `lma` node (postprocess_defaults).

    Remove it from the registry and the import cache after each test so the global
    singleton stays clean for the rest of the session and re-imports register it
    fresh next time.
    """
    yield
    registry.unregister("lma")
    sys.modules.pop("adapt.execution.nodes.lma", None)


class _FakePostModule(BaseModule):
    name = "fake_post"
    pipeline_phase = POSTPROCESS_PHASE
    inputs = ["run_id"]
    outputs = ["fake_rows"]
    output_table = OutputTableSpec(name="fake_ext", primary_key=("run_id", "cell_uid"))

    def run(self, context: dict) -> dict:
        return {
            "fake_rows": pd.DataFrame(
                [{"run_id": context["run_id"], "cell_uid": "abc", "value": 1.0}]
            )
        }


class _CoreWritingModule(BaseModule):
    name = "rogue_post"
    pipeline_phase = POSTPROCESS_PHASE
    inputs = ["run_id"]
    outputs = ["rogue_rows"]
    output_table = OutputTableSpec(name="cells_by_scan", primary_key=("run_id", "cell_uid"))

    def run(self, context: dict) -> dict:
        return {"rogue_rows": pd.DataFrame([{"run_id": context["run_id"], "cell_uid": "x"}])}


class _TwoTableModule(BaseModule):
    name = "two_table_post"
    pipeline_phase = POSTPROCESS_PHASE
    inputs = ["run_id"]
    outputs = ["rows_a", "rows_b"]
    output_tables = {
        "rows_a": OutputTableSpec(name="ext_a", primary_key=("cell_uid",)),
        "rows_b": OutputTableSpec(name="ext_b", primary_key=("flash_id",)),
    }

    def run(self, context: dict) -> dict:
        return {
            "rows_a": pd.DataFrame([{"cell_uid": "c1", "v": 1.0}]),
            "rows_b": pd.DataFrame([{"flash_id": 7, "w": 2.0}]),
        }


def _register(module_cls):
    registry.register(module_cls)
    return module_cls.name


def test_module_can_write_multiple_extension_tables(pipeline_config, test_repository):
    _register(_TwoTableModule)
    try:
        PostProcessor(test_repository, pipeline_config).run(modules=["two_table_post"])
        conn = sqlite3.connect(test_repository.catalog.db_path)
        try:
            a = conn.execute("SELECT cell_uid, v FROM ext_a").fetchall()
            b = conn.execute("SELECT flash_id, w FROM ext_b").fetchall()
        finally:
            conn.close()
        assert a == [("c1", 1.0)]
        assert b == [(7, 2.0)]
    finally:
        registry.unregister("two_table_post")


def test_runs_single_module_and_writes_extension_table(
    pipeline_config, test_repository
):
    _register(_FakePostModule)
    try:
        pp = PostProcessor(test_repository, pipeline_config)
        result = pp.run(modules=["fake_post"])

        # DAG resolution path produced the module's output into the context.
        assert "fake_rows" in result

        conn = sqlite3.connect(test_repository.catalog.db_path)
        try:
            rows = conn.execute("SELECT cell_uid, value FROM fake_ext").fetchall()
        finally:
            conn.close()
        assert rows == [("abc", 1.0)]
    finally:
        registry.unregister("fake_post")


def test_cannot_write_core_table(pipeline_config, test_repository):
    _register(_CoreWritingModule)
    try:
        pp = PostProcessor(test_repository, pipeline_config)
        with pytest.raises(ValueError, match="core table"):
            pp.run(modules=["rogue_post"])
    finally:
        registry.unregister("rogue_post")


class _NeedsMasksModule(BaseModule):
    name = "needs_masks_post"
    pipeline_phase = POSTPROCESS_PHASE
    inputs = ["scan_masks", "radar_origin"]
    outputs = ["probe_rows"]
    output_table = OutputTableSpec(name="probe_ext", primary_key=("n",))
    captured: dict = {}

    def run(self, context: dict) -> dict:
        _NeedsMasksModule.captured = {
            "n_masks": len(context["scan_masks"]),
            "radar_origin": context["radar_origin"],
        }
        return {"probe_rows": pd.DataFrame([{"n": len(context["scan_masks"]), "ok": 1}])}


def test_injects_scan_masks_and_radar_origin(pipeline_config, test_repository):
    ds = xr.Dataset(
        {
            "cell_labels": (("y", "x"), np.zeros((3, 3), dtype=np.int32)),
            "cell_uid": ("cell_label", np.array(["NONE"], dtype=np.str_)),
        },
        coords={"x": [0.0, 200.0, 400.0], "y": [0.0, 200.0, 400.0], "cell_label": [0]},
    )
    test_repository.write_netcdf(
        ds=ds,
        product_type=ProductType.ANALYSIS_NC,
        scan_time=datetime(2024, 5, 18, 12, 0, 0, tzinfo=UTC),
        producer="test",
    )
    test_repository.registry.ensure_radar_location("TEST_RADAR", 40.0, -88.0)

    _register(_NeedsMasksModule)
    try:
        PostProcessor(test_repository, pipeline_config).run(modules=["needs_masks_post"])
        assert _NeedsMasksModule.captured["n_masks"] == 1
        assert _NeedsMasksModule.captured["radar_origin"] == (40.0, -88.0)
    finally:
        registry.unregister("needs_masks_post")


def test_unknown_module_name_raises(pipeline_config, test_repository):
    _register(_FakePostModule)
    try:
        pp = PostProcessor(test_repository, pipeline_config)
        with pytest.raises(ValueError, match="Unknown module"):
            pp.run(modules=["does_not_exist"])
    finally:
        registry.unregister("fake_post")
