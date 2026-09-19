# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Source variable names are canonicalized ONCE, at the end of regrid().

reader.field_map renames source names to canonical names; reader.fields is
the explicit keep-list. Everything downstream of ingest sees canonical
names only, and the applied mapping is recorded as provenance.
"""

import json
import types

import numpy as np
import pytest
import xarray as xr

from adapt.modules.ingest.module import RadarDataLoader

pytestmark = pytest.mark.unit


def _fake_radar():
    # regrid() reads radar location AFTER canonicalization — the double
    # must carry the pyart metadata attributes.
    return types.SimpleNamespace(
        latitude={"data": [41.0]},
        longitude={"data": [-88.0]},
        altitude={"data": [200.0]},
    )


def _fake_grid(*var_names):
    ds = xr.Dataset(
        {name: (("y", "x"), np.zeros((2, 2))) for name in var_names},
        coords={"y": [0.0, 1.0], "x": [0.0, 1.0]},
    )
    return types.SimpleNamespace(to_xarray=lambda: ds)


def _loader(monkeypatch, make_ingest_config, grid, **reader_overrides):
    monkeypatch.setattr("pyart.map.grid_from_radars", lambda radar, **kw: grid)
    config = (
        make_ingest_config(reader=reader_overrides) if reader_overrides else make_ingest_config()
    )
    return RadarDataLoader(config)


def test_field_map_renames_source_variables_to_canonical(monkeypatch, make_ingest_config):
    loader = _loader(
        monkeypatch,
        make_ingest_config,
        _fake_grid("corrected_reflectivity"),
        field_map={"corrected_reflectivity": "reflectivity"},
    )
    ds = loader.regrid(_fake_radar())
    assert "reflectivity" in ds.data_vars
    assert "corrected_reflectivity" not in ds.data_vars


def test_fields_list_drops_unlisted_variables(monkeypatch, make_ingest_config):
    loader = _loader(
        monkeypatch,
        make_ingest_config,
        _fake_grid("reflectivity", "velocity", "ROI"),
        fields=["reflectivity"],
    )
    ds = loader.regrid(_fake_radar())
    assert set(ds.data_vars) == {"reflectivity"}


def test_missing_listed_field_raises_naming_available(monkeypatch, make_ingest_config):
    loader = _loader(
        monkeypatch,
        make_ingest_config,
        _fake_grid("reflectivity"),
        fields=["reflectivity", "differential_reflectivity"],
    )
    with pytest.raises(ValueError, match="differential_reflectivity"):
        loader.regrid(_fake_radar())


def test_source_field_provenance_stamped(monkeypatch, make_ingest_config):
    loader = _loader(
        monkeypatch,
        make_ingest_config,
        _fake_grid("corrected_reflectivity"),
        field_map={"corrected_reflectivity": "reflectivity"},
    )
    ds = loader.regrid(_fake_radar())
    assert json.loads(ds.attrs["source_fields_json"]) == {"reflectivity": "corrected_reflectivity"}


def test_map_entry_for_absent_source_variable_is_ignored(monkeypatch, make_ingest_config):
    # A map may cover fields a given file lacks; only the fields list
    # decides whether absence is an error.
    loader = _loader(
        monkeypatch,
        make_ingest_config,
        _fake_grid("reflectivity"),
        field_map={"corrected_reflectivity": "reflectivity"},
    )
    ds = loader.regrid(_fake_radar())
    assert set(ds.data_vars) == {"reflectivity"}
    assert json.loads(ds.attrs["source_fields_json"]) == {}


def test_empty_map_and_fields_change_nothing(monkeypatch, make_ingest_config):
    loader = _loader(monkeypatch, make_ingest_config, _fake_grid("reflectivity", "velocity", "ROI"))
    ds = loader.regrid(_fake_radar())
    assert set(ds.data_vars) == {"reflectivity", "velocity", "ROI"}
    assert ds.attrs["radar_latitude"] == 41.0
