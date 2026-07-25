# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Config validation errors read clearly: which field, what value, and why."""

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from adapt.configuration.schemas.errors import (
    ConfigError,
    format_validation_error,
    validated,
)
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config

pytestmark = pytest.mark.unit


class _Sample(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    count: int = Field(ge=0, le=10)


def _error(data: dict) -> ValidationError:
    with pytest.raises(ValidationError) as excinfo:
        _Sample.model_validate(data)
    return excinfo.value


def test_extra_forbidden_names_field_value_and_reason():
    msg = format_validation_error(_error({"name": "x", "count": 5, "bogus": 1}), "config.yaml")
    assert "config.yaml" in msg
    assert "bogus" in msg
    assert "unrecognized option" in msg


def test_missing_required_field_is_reported():
    msg = format_validation_error(_error({"count": 5}), "config.yaml")
    assert "name" in msg
    assert "required option is missing" in msg


def test_out_of_range_reports_value_and_bound():
    msg = format_validation_error(_error({"name": "x", "count": 99}), "config.yaml")
    assert "count" in msg
    assert "99" in msg
    assert "out of range" in msg


def test_validated_raises_configerror_not_validationerror():
    with pytest.raises(ConfigError) as excinfo:
        validated(_Sample, {"name": "x", "count": 5, "bogus": 1}, source="my_source")
    assert "my_source" in str(excinfo.value)
    assert not isinstance(excinfo.value, ValidationError)


def test_resolve_config_reports_stale_segmenter_option():
    """The reported case: a removed 'threshold' under segmenter must name the path."""
    with pytest.raises(ConfigError) as excinfo:
        resolve_config(ParamConfig(), {"segmenter": {"threshold": 30.0}}, {"radar": "KLOT"})
    message = str(excinfo.value)
    assert "segmenter.threshold" in message
    assert "30.0" in message
