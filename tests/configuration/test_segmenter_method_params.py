# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Only the selected segmentation method's params are validated.

The generated config.yaml carries a param block for every method, but a run uses
exactly one. A stale or renamed field in a method that is NOT selected must not
block the pipeline — while a bad param in the SELECTED method still fails loudly.
"""

import pytest

from adapt.configuration.schemas.errors import ConfigError
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig

pytestmark = pytest.mark.unit


def _resolve(segmenter: dict):
    base = {"radar": "KLOT", "base_dir": "/tmp/adapt", "segmenter": segmenter}
    return resolve_config(ParamConfig(), UserConfig.model_validate(base))


def test_stale_field_in_unused_method_is_ignored():
    """method=yuter: a stale field under conv_strat_raut_params must not fail."""
    internal = _resolve(
        {"method": "conv_strat_yuter", "conv_strat_raut_params": {"always_core_thres": 42.0}}
    )
    assert internal.segmenter.method == "conv_strat_yuter"


def test_unknown_key_in_unused_method_is_ignored():
    """An outright bogus key in an unused method block is also ignored."""
    internal = _resolve(
        {"method": "feature_detection", "conv_strat_raut_params": {"totally_bogus": 1}}
    )
    assert internal.segmenter.method == "feature_detection"


def test_bad_param_in_selected_method_still_fails():
    """The selected method stays strict — a bogus key there is a clear error."""
    with pytest.raises(ConfigError, match="conv_strat_raut_params"):
        _resolve({"method": "conv_strat_raut", "conv_strat_raut_params": {"totally_bogus": 1}})


def test_valid_override_in_selected_method_applies():
    """A valid override for the selected method still takes effect."""
    internal = _resolve(
        {"method": "conv_strat_raut", "conv_strat_raut_params": {"conv_scale_km": 30.0}}
    )
    assert internal.segmenter.conv_strat_raut_params.conv_scale_km == 30.0
