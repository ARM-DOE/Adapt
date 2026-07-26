# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for BaseModule.build_config — the module-driven config hook.

A module owns the logic that slices the resolved InternalConfig into its own
frozen config. The default returns None (module needs no config).
"""

import pytest

pytestmark = pytest.mark.unit

from adapt.modules.base import BaseModule  # noqa: E402


class _NoConfigModule(BaseModule):
    name = "no_config"

    def run(self, context: dict) -> dict:
        return {}


class TestBaseModuleBuildConfig:
    def test_default_config_class_is_none(self):
        """Modules that need no config leave config_class as None."""
        assert _NoConfigModule.config_class is None

    def test_default_build_config_returns_none(self):
        """Default build_config returns None — no config produced."""
        assert _NoConfigModule.build_config(internal_config=object()) is None


class TestDetectModuleBuildConfig:
    def test_build_config_returns_detection_config(self, internal_config):
        """DetectModule.build_config returns a DetectionConfig instance."""
        from adapt.execution.nodes.detection import DetectModule
        from adapt.modules.detection.config import DetectionConfig

        cfg = DetectModule.build_config(internal_config)
        assert isinstance(cfg, DetectionConfig)

    def test_build_config_slices_segmenter_section(self, internal_config):
        """Module-specific fields come from the segmenter section."""
        from adapt.execution.nodes.detection import DetectModule

        cfg = DetectModule.build_config(internal_config)
        seg = internal_config.segmenter
        assert cfg.method == seg.method
        assert cfg.method_params == getattr(seg, f"{seg.method}_params").model_dump()
        assert cfg.h_maxima == seg.h_maxima

    def test_build_config_injects_global_fields(self, internal_config):
        """Global fields (z_level, var names) come from the global section."""
        from adapt.execution.nodes.detection import DetectModule

        cfg = DetectModule.build_config(internal_config)
        assert cfg.z_level == internal_config.global_.z_level
        assert cfg.reflectivity_var == internal_config.global_.var_names.reflectivity
        assert cfg.labels_var == internal_config.global_.var_names.cell_labels
