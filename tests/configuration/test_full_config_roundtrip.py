# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""End-to-end: assemble → dump YAML → load → resolve_config.

The generated full config must round-trip: loading it back and resolving
reproduces the defaults, and editing any field (core section, passthrough
section, or a module_params value) overrides that default.
"""

import pytest

pytestmark = pytest.mark.unit

import yaml  # noqa: E402

from adapt.configuration.schemas import yaml_writer  # noqa: E402
from adapt.configuration.schemas.assemble import (  # noqa: E402
    assemble_default_config,
    assemble_descriptions,
)
from adapt.configuration.schemas.param import ParamConfig  # noqa: E402
from adapt.configuration.schemas.resolve import resolve_config  # noqa: E402
from adapt.configuration.schemas.user import UserConfig  # noqa: E402

_EXT = "adapt.execution.nodes.cell_volume_stats"


def _generate_and_load(edits: dict | None = None) -> dict:
    data = assemble_default_config(extensions=[_EXT])
    text = yaml_writer.dump(data, assemble_descriptions(extensions=[_EXT]))
    loaded = yaml.safe_load(text)
    loaded.setdefault("downloader", {})
    loaded["radar"] = "KLOT"
    loaded["base_dir"] = "/tmp/adapt"
    if edits:
        loaded.update(edits)
    return loaded


class TestRoundTrip:
    def test_defaults_resolve_unchanged(self):
        loaded = _generate_and_load()
        internal = resolve_config(ParamConfig(), UserConfig.model_validate(loaded))
        defaults = ParamConfig()
        assert (
            internal.segmenter.threshold_params.threshold
            == defaults.segmenter.threshold_params.threshold
        )
        assert (
            internal.tracker.minimum_candidate_overlap == defaults.tracker.minimum_candidate_overlap
        )
        assert (
            internal.analyzer.adjacency_min_touching_boundary_pixels
            == defaults.analyzer.adjacency_min_touching_boundary_pixels
        )
        assert internal.downloader.min_file_size == defaults.downloader.min_file_size

    def test_extension_params_present_in_generated_file(self):
        loaded = _generate_and_load()
        assert loaded["module_params"]["cell_volume_stats"]["gap_tolerance_m"] == 500.0
        assert "reflectivity_var" not in loaded["module_params"]["cell_volume_stats"]

    def test_core_section_edit_overrides(self):
        loaded = _generate_and_load(
            {"segmenter": {"method": "threshold", "threshold_params": {"threshold": 42.0}}}
        )
        internal = resolve_config(ParamConfig(), UserConfig.model_validate(loaded))
        assert internal.segmenter.threshold_params.threshold == 42.0

    def test_passthrough_section_edit_overrides(self):
        loaded = _generate_and_load({"tracker": {"max_speed_ms": 12.0}})
        internal = resolve_config(ParamConfig(), UserConfig.model_validate(loaded))
        assert internal.tracker.max_speed_ms == 12.0
