# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""reader.field_map / reader.fields — the per-source canonicalization knobs."""

import pytest
from pydantic import ValidationError

from adapt.configuration.schemas.param import ParamConfig

pytestmark = pytest.mark.unit


def test_reader_field_map_defaults_are_identity():
    cfg = ParamConfig()
    assert cfg.reader.field_map == {}
    assert cfg.reader.fields == []


def test_reader_field_map_accepts_source_to_canonical_entries():
    cfg = ParamConfig(
        reader={
            "field_map": {"corrected_reflectivity": "reflectivity"},
            "fields": ["reflectivity"],
        }
    )
    assert cfg.reader.field_map == {"corrected_reflectivity": "reflectivity"}
    assert cfg.reader.fields == ["reflectivity"]


def test_reader_rejects_unknown_keys():
    with pytest.raises(ValidationError):
        ParamConfig(reader={"field_mapp": {"a": "b"}})
