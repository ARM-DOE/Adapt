# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""UserConfig must let every InternalConfig section be overridden.

Previously sections with no explicit UserConfig field (tracker, visualization,
output, logging, processor, reader) were silently dropped. Generated config.yaml
exposes them, so an edit there must reach the resolved InternalConfig.
"""

import pytest

pytestmark = pytest.mark.unit

from adapt.configuration.schemas.errors import ConfigError  # noqa: E402
from adapt.configuration.schemas.param import ParamConfig  # noqa: E402
from adapt.configuration.schemas.resolve import resolve_config  # noqa: E402
from adapt.configuration.schemas.user import UserConfig  # noqa: E402


def _resolve(user_dict):
    base = {"radar": "KLOT", "base_dir": "/tmp/adapt", **user_dict}
    return resolve_config(ParamConfig(), UserConfig.model_validate(base))


class TestPassthroughSections:
    def test_tracker_override_reaches_internal(self):
        internal = _resolve({"radar": "KLOT", "tracker": {"minimum_candidate_overlap": 0.5}})
        assert internal.tracker.minimum_candidate_overlap == 0.5

    def test_visualization_override_reaches_internal(self):
        internal = _resolve({"radar": "KLOT", "visualization": {"dpi": 123}})
        assert internal.visualization.dpi == 123

    def test_output_override_reaches_internal(self):
        internal = _resolve({"radar": "KLOT", "output": {"compression": "gzip"}})
        assert internal.output.compression == "gzip"

    def test_logging_override_reaches_internal(self):
        internal = _resolve({"radar": "KLOT", "logging": {"level": "DEBUG"}})
        assert internal.logging.level == "DEBUG"


class TestValidationStillStrict:
    def test_unknown_key_in_section_raises(self):
        with pytest.raises(ConfigError):
            _resolve({"radar": "KLOT", "tracker": {"bogus_param": 1}})
