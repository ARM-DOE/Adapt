# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Enrich modules receive the in-memory 3D grid — no store round-trip.

The ingest node returns ``grid_ds`` in the scan result; when a post-pipeline
module declares ``grid_ds_3d`` the processor injects that same dataset into
the enrichment context directly.
"""

import queue
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pytest
import xarray as xr

from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]

SCAN_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def _grid_ds() -> xr.Dataset:
    return xr.Dataset(
        {"reflectivity": (("z", "y", "x"), np.zeros((3, 4, 4), dtype=np.float32))},
        coords={"z": [0, 1000, 2000], "y": range(4), "x": range(4)},
    )


@pytest.fixture
def proc(pipeline_config, store_env):
    return RadarProcessor(
        queue.Queue(),
        pipeline_config,
        collection=store_env.collection,
        registry=store_env.registry,
        run_id=store_env.run_id,
        history=store_env.history,
    )


class TestEnrichGridContext:
    def test_declared_grid_input_gets_in_memory_dataset(self, proc):
        proc._post_modules = [SimpleNamespace(inputs=["grid_ds_3d"])]
        grid = _grid_ds()

        ctx = proc._build_enrich_context({"grid_ds": grid}, SCAN_TIME, "sid-test")

        assert ctx["grid_ds_3d"] is grid

    def test_missing_grid_raises_when_declared(self, proc):
        proc._post_modules = [SimpleNamespace(inputs=["grid_ds_3d"])]

        with pytest.raises(RuntimeError, match="grid_ds"):
            proc._build_enrich_context({}, SCAN_TIME, "sid-test")

    def test_grid_not_injected_when_no_module_declares_it(self, proc):
        proc._post_modules = [SimpleNamespace(inputs=["cells"])]

        ctx = proc._build_enrich_context({"grid_ds": _grid_ds()}, SCAN_TIME, "sid-test")

        assert "grid_ds_3d" not in ctx
