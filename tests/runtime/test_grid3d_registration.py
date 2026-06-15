# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests that the 3D gridded NetCDF becomes a queryable catalog artifact.

The loader writes the 3D NetCDF to disk; the ingest node returns its path and
declares a RegisterFileArtifact spec; the router registers it as a gridded3d
artifact so enrich modules (via the processor reader) can open it by scan_time.
"""

import queue
from datetime import UTC, datetime

import numpy as np
import pytest
import xarray as xr

from adapt.contracts import PersistenceMeta
from adapt.persistence.repository import ProductType
from adapt.runtime.processor import RadarProcessor

pytestmark = [pytest.mark.unit, pytest.mark.pipeline]


def _make_proc(pipeline_config, pipeline_output_dirs, test_repository):
    return RadarProcessor(
        queue.Queue(),
        pipeline_config,
        pipeline_output_dirs,
        repository=test_repository,
    )


def _persist(proc, test_repository, result, scan_time):
    meta = PersistenceMeta(
        scan_time=scan_time,
        run_id=test_repository.run_id,
        source_file="scan_grid",
        dataset_id=test_repository.radar,
    )
    proc._router.persist(proc._pipeline_modules, result, meta)


def _write_grid_nc(path) -> None:
    ds = xr.Dataset(
        {"reflectivity": (("z", "y", "x"), np.zeros((3, 4, 4), dtype=np.float32))},
        coords={"z": [0, 1000, 2000], "y": range(4), "x": range(4)},
    )
    ds.to_netcdf(path)


class TestIngestDeclaresGridPath:
    def test_load_module_declares_grid_nc_path_output(self):
        from adapt.execution.nodes.ingest import LoadModule

        assert "grid_nc_path" in LoadModule.outputs


class TestProcessorRegistersGrid3D:
    def test_grid3d_registered_and_queryable(
        self, tmp_path, pipeline_config, pipeline_output_dirs, test_repository
    ):
        proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)

        nc_file = tmp_path / "scan_grid.nc"
        _write_grid_nc(nc_file)
        scan_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        _persist(proc, test_repository, {"grid_nc_path": str(nc_file)}, scan_time)

        artifacts = test_repository.query(
            product_type=ProductType.GRIDDED_NC, time_range=(scan_time, scan_time)
        )
        assert len(artifacts) == 1
        ds = test_repository.open_dataset(artifacts[0]["artifact_id"])
        assert "reflectivity" in ds.data_vars

    def test_missing_grid_file_raises(self, pipeline_config, pipeline_output_dirs, test_repository):
        proc = _make_proc(pipeline_config, pipeline_output_dirs, test_repository)
        scan_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

        # A declared path pointing at a non-existent file is corruption, not an
        # option: no fallbacks, fail loudly.
        with pytest.raises(FileNotFoundError, match="grid_nc_path"):
            _persist(proc, test_repository, {"grid_nc_path": "/nonexistent/grid.nc"}, scan_time)

        artifacts = test_repository.query(
            product_type=ProductType.GRIDDED_NC, time_range=(scan_time, scan_time)
        )
        assert artifacts == []
