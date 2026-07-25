# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for resolve_module_configs — the registry-driven config engine.

Iterates registered modules, calls each one's build_config, and returns a dict
keyed by "<name>_config". Replaces the hardcoded materialize_module_configs.
"""

import pytest

pytestmark = pytest.mark.unit

from adapt.execution.pipeline_builder import _ensure_modules_registered  # noqa: E402


@pytest.fixture(autouse=True)
def _register_core_modules():
    """Ensure the five core modules are registered before each test."""
    _ensure_modules_registered()


class TestResolveModuleConfigs:
    def test_returns_all_core_module_config_keys(self, internal_config):
        from adapt.configuration.schemas.module_resolver import resolve_module_configs

        configs = resolve_module_configs(internal_config)
        for key in (
            "ingest_config",
            "detection_config",
            "projection_config",
            "analysis_config",
            "tracking_config",
        ):
            assert key in configs

    def test_detection_method_params_match_internal_config(self, internal_config):
        from adapt.configuration.schemas.module_resolver import resolve_module_configs

        configs = resolve_module_configs(internal_config)
        detection = configs["detection_config"]
        seg = internal_config.segmenter
        assert detection.method == seg.method
        assert detection.method_params == getattr(seg, f"{seg.method}_params").model_dump()

    def test_analysis_cross_reference_to_projector(self, internal_config):
        from adapt.configuration.schemas.module_resolver import resolve_module_configs

        configs = resolve_module_configs(internal_config)
        assert (
            configs["analysis_config"].max_projection_steps
            == internal_config.projector.max_projection_steps
        )

    def test_module_returning_none_contributes_no_key(self, internal_config):
        """A registered module whose build_config returns None adds no config key."""
        from adapt.configuration.schemas.module_resolver import resolve_module_configs
        from adapt.execution.module_registry import registry
        from adapt.modules.base import BaseModule

        class _NoConfig(BaseModule):
            name = "no_config_probe"

            def run(self, context):
                return {}

        registry.register(_NoConfig)
        try:
            configs = resolve_module_configs(internal_config)
            assert "no_config_probe_config" not in configs
        finally:
            registry.unregister("no_config_probe")
