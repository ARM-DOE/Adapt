# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for LoadModule (ingest node) with a faked RadarDataLoader.

The loader is the only third-party boundary (Py-ART); everything else —
scan-time parsing, z-slicing, output paths, failure handling — is pure
logic exercised on a synthetic 3D grid.
"""

from datetime import datetime

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

    def load_and_regrid(self, filepath, save_netcdf=False, output_dir=None):
        self.calls.append({"filepath": filepath, "output_dir": output_dir})
        return self.result


@pytest.fixture
def load_module(monkeypatch):
    FakeLoader.instances = 0
    monkeypatch.setattr(ingest_node, "RadarDataLoader", FakeLoader)
    return LoadModule()


def _context(config, **overrides):
    ctx = {"ingest_config": config, "nexrad_file": _NEXRAD_NAME, "output_dirs": {}}
    ctx.update(overrides)
    return ctx


def test_scan_time_parsed_from_nexrad_filename(load_module, ingest_module_config):
    result = load_module.run(_context(ingest_module_config))

    assert result["scan_time"] == datetime(2024, 5, 18, 12, 34, 56)


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


def test_grid_nc_path_built_under_base_dir(load_module, ingest_module_config, tmp_path):
    result = load_module.run(_context(ingest_module_config, output_dirs={"base": tmp_path}))

    assert result["grid_nc_path"] is not None
    assert result["grid_nc_path"].endswith(f"{_NEXRAD_NAME}.nc")
    assert "20240518" in result["grid_nc_path"]


def test_grid_nc_path_none_without_base_dir(load_module, ingest_module_config):
    result = load_module.run(_context(ingest_module_config))

    assert "grid_nc_path" not in result


def test_loader_is_created_once_across_files(load_module, ingest_module_config):
    load_module.run(_context(ingest_module_config))
    load_module.run(_context(ingest_module_config, nexrad_file="KLOT20240518_124000_V06"))

    assert FakeLoader.instances == 1
    assert len(load_module._loader.calls) == 2


def test_loader_returning_none_raises(load_module, ingest_module_config, monkeypatch):
    monkeypatch.setattr(FakeLoader, "load_and_regrid", lambda *a, **kw: None)

    with pytest.raises(RuntimeError, match=_NEXRAD_NAME):
        load_module.run(_context(ingest_module_config))
