# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for LoadModule (ingest node) with a faked RadarDataLoader.

The loader is the only third-party boundary (Py-ART); everything else —
scan-time handling, z-slicing, failure handling — is pure logic exercised on
a synthetic 3D grid. The node builds no paths: the 3D grid is returned
in-memory and persisted by the router as a gridded3d object.
"""

from datetime import UTC, datetime

import pytest

from adapt.contracts import check_grid_ds_2d
from adapt.execution.nodes import ingest as ingest_node
from adapt.execution.nodes.ingest import LoadModule
from tests.helpers.fake_grid import make_fake_grid_ds

pytestmark = pytest.mark.unit

_NEXRAD_NAME = "KLOT20240518_123456_V06"


class FakeLoader:
    """Stands in for RadarDataLoader; records calls, returns a synthetic grid."""

    instances = 0

    def __init__(self, config):
        FakeLoader.instances += 1
        self.calls = []
        self.result = make_fake_grid_ds(z_levels=(0, 1000, 2000), shape=(6, 6))

    def load_and_regrid(self, filepath):
        self.calls.append({"filepath": filepath})
        return self.result


@pytest.fixture
def load_module(monkeypatch):
    FakeLoader.instances = 0
    monkeypatch.setattr(ingest_node, "RadarDataLoader", FakeLoader)
    return LoadModule()


_SCAN_TIME = datetime(2024, 5, 18, 12, 34, 56, tzinfo=UTC)


def _context(config, **overrides):
    ctx = {
        "ingest_config": config,
        "nexrad_file": _NEXRAD_NAME,
        "scan_time": _SCAN_TIME,  # owned by the source boundary; processor-seeded
    }
    ctx.update(overrides)
    return ctx


def test_scan_time_is_never_output_or_rederived(load_module, ingest_module_config):
    # The source boundary owns scan_time; the node consumes it from the context
    # and must not re-emit it (one authoritative source per concept).
    result = load_module.run(_context(ingest_module_config))

    assert "scan_time" not in result


def test_missing_context_scan_time_raises(load_module, ingest_module_config):
    # Wall-clock substitution is forbidden (contracts/persistence.py): a scan
    # whose observation time cannot be determined must fail loudly, not write
    # present-day timestamps into the tracking tables. The filename is never
    # parsed as a substitute.
    with pytest.raises(ValueError, match="deadbeef"):
        load_module.run(_context(ingest_module_config, nexrad_file="deadbeef.nc", scan_time=None))


def test_2d_slice_satisfies_grid_contract(load_module, ingest_module_config):
    result = load_module.run(_context(ingest_module_config))

    check_grid_ds_2d(result["grid_ds_2d"])
    assert "z" not in result["grid_ds_2d"]["reflectivity"].dims
    assert "time" not in result["grid_ds_2d"]["reflectivity"].dims


def test_2d_slice_takes_nearest_z_level(load_module, ingest_module_config):
    result = load_module.run(_context(ingest_module_config))

    # config z_level is 2000 m; the synthetic grid has levels (0, 1000, 2000)
    expected = load_module._loader.result["reflectivity"].isel(time=0, z=2)
    assert (result["grid_ds_2d"]["reflectivity"].values == expected.values).all()


def test_outputs_are_in_memory_datasets_only(load_module, ingest_module_config):
    result = load_module.run(_context(ingest_module_config))

    assert set(result) == {"grid_ds", "grid_ds_2d"}
    assert result["grid_ds"] is load_module._loader.result


def test_declares_gridded3d_netcdf_artifact():
    from adapt.contracts import NetcdfArtifact
    from adapt.execution.nodes.ingest import LoadModule

    (spec,) = LoadModule.persistence
    assert isinstance(spec, NetcdfArtifact)
    assert spec.key == "grid_ds"
    assert spec.product_type == "gridded3d"
    assert "grid_nc_path" not in LoadModule.outputs


def test_loader_is_created_once_across_files(load_module, ingest_module_config):
    load_module.run(_context(ingest_module_config))
    load_module.run(_context(ingest_module_config, nexrad_file="KLOT20240518_124000_V06"))

    assert FakeLoader.instances == 1
    assert len(load_module._loader.calls) == 2


def test_loader_returning_none_raises(load_module, ingest_module_config, monkeypatch):
    monkeypatch.setattr(FakeLoader, "load_and_regrid", lambda *a, **kw: None)

    with pytest.raises(RuntimeError, match=_NEXRAD_NAME):
        load_module.run(_context(ingest_module_config))
