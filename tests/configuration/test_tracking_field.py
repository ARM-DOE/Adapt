# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""global_.tracking_field — the role knob: which canonical field the core
detects, projects, and tracks on. A naming knob it is not (names are
canonicalized at ingest via reader.field_map)."""

import pytest
from pydantic import ValidationError

from adapt.configuration.schemas.errors import ConfigError
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig

pytestmark = pytest.mark.unit


def test_tracking_field_default_and_override():
    cfg = ParamConfig()
    assert cfg.global_.tracking_field == "reflectivity"
    cfg2 = ParamConfig(**{"global": {"tracking_field": "pressure"}})
    assert cfg2.global_.tracking_field == "pressure"


def test_var_names_section_is_gone():
    with pytest.raises(ValidationError):
        ParamConfig(**{"global": {"var_names": {"reflectivity": "x"}}})


def test_tracking_field_must_be_in_analyzer_whitelist():
    # A tracked field the analyzer never computes stats for would fail
    # mid-run in tracking; fail loudly at resolve time instead.
    user = UserConfig(base_dir="/tmp", radar="KHTX")
    with pytest.raises(ConfigError, match="tracking_field"):
        resolve_config(ParamConfig(**{"global": {"tracking_field": "pressure"}}), user, None)


def test_tracking_field_must_survive_reader_fields_selection():
    user = UserConfig(base_dir="/tmp", radar="KHTX")
    with pytest.raises(ConfigError, match="reader.fields"):
        resolve_config(ParamConfig(reader={"fields": ["velocity"]}), user, None)


def test_tracking_field_valid_when_whitelisted_and_selected():
    user = UserConfig(base_dir="/tmp", radar="KHTX")
    cfg = resolve_config(
        ParamConfig(
            reader={"fields": ["reflectivity", "velocity"]},
        ),
        user,
        None,
    )
    assert cfg.global_.tracking_field == "reflectivity"


def test_user_config_can_set_tracking_field():
    from adapt.configuration.schemas.user import UserGlobalConfig

    user = UserConfig(
        base_dir="/tmp", radar="KHTX", global_=UserGlobalConfig(tracking_field="velocity")
    )
    cfg = resolve_config(ParamConfig(), user, None)
    assert cfg.global_.tracking_field == "velocity"  # velocity is whitelisted


def test_reflectivity_var_alias_is_a_rename_not_a_role_change():
    # REFLECTIVITY_VAR="dbz" = "my file calls reflectivity dbz": ingest
    # renames dbz -> reflectivity; the core keeps tracking 'reflectivity'.
    user = UserConfig(base_dir="/tmp", radar="KHTX", reflectivity_var="dbz")
    cfg = resolve_config(ParamConfig(), user, None)
    assert cfg.reader.field_map == {"dbz": "reflectivity"}
    assert cfg.global_.tracking_field == "reflectivity"
