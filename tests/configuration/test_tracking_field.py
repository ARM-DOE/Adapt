# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""global_.tracking_field — the role knob: which canonical field the core
detects, projects, and tracks on. A naming knob it is not (names are
canonicalized at ingest via reader.field_map)."""

import pytest
from pydantic import ValidationError

from adapt.configuration.schemas.param import ParamConfig

pytestmark = pytest.mark.unit


def test_tracking_field_default_and_override():
    cfg = ParamConfig()
    assert cfg.global_.tracking_field == "reflectivity"
    cfg2 = ParamConfig(**{"global": {"tracking_field": "pressure"}})
    assert cfg2.global_.tracking_field == "pressure"


def test_var_names_section_is_gone():
    with pytest.raises(ValidationError):
        ParamConfig(**{"global": {"var_names": {"reflectivity": "x"}}})
