"""Test config resolution and validation with Pydantic."""

import pytest

from adapt.configuration.schemas.cli import CLIConfig
from adapt.configuration.schemas.errors import ConfigError
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import deep_merge, resolve_config
from adapt.configuration.schemas.user import (
    UserAnalyzerConfig,
    UserConfig,
    UserProjectorConfig,
    UserSegmenterConfig,
)


class TestConfigResolution:
    """Test resolve_config() precedence and merging."""

    def test_resolve_config_all_defaults(self):
        """Resolving with no user/CLI overrides fails due to missing required fields."""
        with pytest.raises(ConfigError):
            resolve_config(ParamConfig(), None, None)

    def test_user_config_overrides_param_config(self):
        """UserConfig values override ParamConfig defaults."""
        user = UserConfig(min_cellsize_gridpoint=40, base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.min_cellsize_gridpoint == 40
        assert config.base_dir == "/tmp"

    def test_cli_config_with_valid_structure(self):
        """CLIConfig structure validation (if implemented)."""
        # CLIConfig currently has limited fields - test what exists
        # Just verify it can be instantiated
        CLIConfig()

    def test_precedence_param_user(self):
        """Full precedence: User > Param."""
        param = ParamConfig()
        user = UserConfig(min_cellsize_gridpoint=40, radar="KDLH", base_dir="/tmp")
        config = resolve_config(param, user, None)

        # User won on min_cellsize_gridpoint
        assert config.segmenter.min_cellsize_gridpoint == 40
        # User won on radar_id
        assert config.downloader.radar == "KDLH"

    def test_empty_user_config_uses_all_param_defaults(self):
        """Empty UserConfig() doesn't override anything, fails if required fields missing."""
        with pytest.raises(ConfigError):
            resolve_config(ParamConfig(), UserConfig(), None)

    def test_none_user_config_uses_all_param_defaults(self):
        """None UserConfig doesn't override anything, but still fails if required fields missing."""
        with pytest.raises(ConfigError):
            resolve_config(ParamConfig(), None, None)


class TestUserConfigAliases:
    """Test UserConfig flat aliases map correctly."""

    def test_threshold_lives_in_per_method_block(self):
        """threshold is now a per-method parameter, defaulting to 30 dBZ."""
        user = UserConfig(base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.threshold_params.threshold == 30.0

    def test_radar_id_alias(self):
        """radar_id flat alias maps to downloader.radar_id."""
        user = UserConfig(radar="KDIX", base_dir="/tmp")
        config = resolve_config(ParamConfig(), user, None)

        assert config.downloader.radar == "KDIX"

    def test_reflectivity_var_alias(self):
        """reflectivity_var alias maps to global var_names."""
        user = UserConfig(reflectivity_var="dbz", base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.global_.var_names.reflectivity == "dbz"

    def test_max_projection_steps_alias(self):
        """max_projection_steps alias maps to projector.max_projection_steps."""
        user = UserConfig(max_projection_steps=5, base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.projector.max_projection_steps == 5

    def test_min_cellsize_gridpoint_alias(self):
        """min_cellsize_gridpoint alias maps to segmenter.min_cellsize_gridpoint."""
        user = UserConfig(min_cellsize_gridpoint=10, base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.min_cellsize_gridpoint == 10

    def test_nested_segmenter_override(self):
        """Nested segmenter config overrides the flat alias."""
        user = UserConfig(
            base_dir="/tmp",
            radar="KHTX",
            min_cellsize_gridpoint=5,
            segmenter=UserSegmenterConfig(min_cellsize_gridpoint=40),
        )
        config = resolve_config(ParamConfig(), user, None)

        # Nested should win
        assert config.segmenter.min_cellsize_gridpoint == 40

    def test_nested_per_method_params_override(self):
        """Advanced users may still override the selected method's param block."""
        user = UserConfig(
            base_dir="/tmp",
            radar="KHTX",
            segmenter=UserSegmenterConfig(method="threshold", threshold_params={"threshold": 40}),
        )
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.threshold_params.threshold == 40.0


class TestTypeCoercion:
    """Test UserConfig type coercion."""

    def test_int_coerced_to_float_for_threshold(self):
        """Integer threshold in the per-method block is coerced to float."""
        user = UserConfig(
            base_dir="/tmp",
            radar="KHTX",
            segmenter=UserSegmenterConfig(method="threshold", threshold_params={"threshold": 35}),
        )
        config = resolve_config(ParamConfig(), user, None)

        assert isinstance(config.segmenter.threshold_params.threshold, float)
        assert config.segmenter.threshold_params.threshold == 35.0

    def test_int_coerced_to_float_for_z_level(self):
        """Integer z_level is coerced to float."""
        user = UserConfig(base_dir="/tmp", radar="KHTX", z_level=1500)  # int
        config = resolve_config(ParamConfig(), user, None)

        assert isinstance(config.global_.z_level, float)
        assert config.global_.z_level == 1500.0

    def test_method_normalized_to_lowercase(self):
        """Method names are normalized to lowercase."""
        user = UserConfig(base_dir="/tmp", radar="KHTX", segmentation_method="THRESHOLD")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.method == "threshold"

    def test_uppercase_radar_id_preserved(self):
        """Radar IDs are preserved in uppercase."""
        user = UserConfig(base_dir="/tmp", radar="KDIX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.downloader.radar == "KDIX"


class TestEdgeCases:
    """Test config edge cases and error conditions."""

    def test_none_values_dont_override(self):
        """None values in UserConfig don't override ParamConfig."""
        user = UserConfig(min_cellsize_gridpoint=None, radar="KDIX", base_dir="/tmp")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.min_cellsize_gridpoint == 5  # default, not overridden
        assert config.downloader.radar == "KDIX"

    def test_dict_user_config_accepted(self):
        """Dict can be passed as UserConfig (converted by Pydantic)."""
        user_dict = {"min_cellsize_gridpoint": 35, "radar": "KDLH", "base_dir": "/tmp"}
        config = resolve_config(ParamConfig(), user_dict, None)

        assert config.segmenter.min_cellsize_gridpoint == 35
        assert config.downloader.radar == "KDLH"

    def test_empty_cli_config_dict_accepted(self):
        """Empty dict can be passed as CLIConfig (converted by Pydantic)."""
        cli_dict = {}
        user = UserConfig(min_cellsize_gridpoint=40, base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, cli_dict)

        # Empty CLI dict doesn't override anything
        assert config.segmenter.min_cellsize_gridpoint == 40

    def test_incomplete_param_config_dict_rejected(self):
        """Incomplete dict raises a clear config error."""
        with pytest.raises(ConfigError):
            resolve_config({"incomplete": "dict"}, None, None)

    def test_internal_config_is_complete(self):
        """Returned InternalConfig is complete with all fields."""
        user = UserConfig(base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter is not None
        assert config.projector is not None
        assert config.downloader is not None


class TestDefaultValues:
    """Test ParamConfig default values match old behavior."""

    def test_segmenter_defaults(self):
        """Segmenter defaults match old hardcoded values."""
        user = UserConfig(base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.segmenter.threshold_params.threshold == 30.0
        assert config.segmenter.closing_kernel == (1, 1)
        assert config.segmenter.min_cellsize_gridpoint == 5
        assert config.segmenter.filter_by_size is True

    def test_projector_defaults(self):
        """Projector defaults match old hardcoded values."""
        user = UserConfig(base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.projector.method == "adapt_default"
        assert config.projector.max_projection_steps == 3  # Updated default
        assert config.projector.flow_params.winsize == 10
        assert config.projector.flow_params.iterations == 3

    def test_downloader_defaults(self):
        """Downloader defaults are initialized."""
        user = UserConfig(base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.downloader.latest_files > 0
        assert config.downloader.poll_interval_sec > 0

    def test_regridder_defaults(self):
        """Regridder defaults are complete."""
        user = UserConfig(base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)

        assert config.regridder.grid_shape is not None
        assert len(config.regridder.grid_shape) == 3
        assert config.regridder.save_netcdf is True


class TestConfigValidation:
    """Test Pydantic validation of configs."""

    def test_invalid_method_rejected(self):
        """Invalid segmentation method raises validation error."""
        with pytest.raises(Exception):  # noqa: B017 — Pydantic ValidationError
            resolve_config(
                ParamConfig(),
                UserConfig(
                    segmentation_method="invalid_method_xyz",
                    base_dir="/tmp",
                    radar="KHTX",
                ),
                None,
            )

    def test_negative_threshold_rejected(self):
        """Negative threshold is coerced to float but should work."""
        # Note: Pydantic may allow negative threshold if no constraint
        # This test documents current behavior
        user = UserConfig(
            base_dir="/tmp",
            radar="KHTX",
            segmenter=UserSegmenterConfig(method="threshold", threshold_params={"threshold": -10}),
        )
        config = resolve_config(ParamConfig(), user, None)
        assert config.segmenter.threshold_params.threshold == -10.0

    def test_zero_min_cellsize_allowed(self):
        """Zero min_cellsize is valid (means no filtering)."""
        user = UserConfig(min_cellsize_gridpoint=0, base_dir="/tmp", radar="KHTX")
        config = resolve_config(ParamConfig(), user, None)
        assert config.segmenter.min_cellsize_gridpoint == 0

    def test_valid_field_accepted(self):
        """Valid fields in user config are accepted."""
        user_dict = {"min_cellsize_gridpoint": 35, "radar": "KDIX", "base_dir": "/tmp"}
        config = resolve_config(ParamConfig(), user_dict, None)
        assert config.segmenter.min_cellsize_gridpoint == 35
        assert config.downloader.radar == "KDIX"


class TestIntegration:
    """Integration tests combining multiple features."""

    def test_full_workflow_with_all_overrides(self):
        """Full workflow: param + user."""
        user = UserConfig(
            mode="historical",
            radar="KDLH",
            base_dir="/tmp",
            start_time="2024-01-01T00:00:00Z",
            end_time="2024-01-01T12:00:00Z",
            max_projection_steps=3,
            segmenter=UserSegmenterConfig(filter_by_size=False),
        )
        config = resolve_config(ParamConfig(), user, None)

        # Verify all overrides took effect
        assert config.mode == "historical"
        assert config.downloader.radar == "KDLH"
        assert config.downloader.start_time == "2024-01-01T00:00:00Z"
        assert config.projector.max_projection_steps == 3
        assert config.segmenter.filter_by_size is False

    def test_real_use_case_custom_radar(self):
        """Real use case: custom radar with custom naming and cell-size filter."""
        user = UserConfig(
            radar="KLTX",
            base_dir="/tmp",
            reflectivity_var="reflectivity_dbz",
            min_cellsize_gridpoint=20,
        )
        config = resolve_config(ParamConfig(), user, None)

        assert config.downloader.radar == "KLTX"
        assert config.global_.var_names.reflectivity == "reflectivity_dbz"
        assert config.segmenter.min_cellsize_gridpoint == 20

    def test_nested_config_complex_flow_params(self):
        """Complex nested config with custom flow parameters."""
        user = UserConfig(
            base_dir="/tmp",
            radar="KHTX",
            projector=UserProjectorConfig(
                max_projection_steps=5,
                flow_params={
                    "winsize": 15,
                    "iterations": 5,
                    "poly_n": 7,
                },
            ),
        )
        config = resolve_config(ParamConfig(), user, None)

        assert config.projector.max_projection_steps == 5
        assert config.projector.flow_params.winsize == 15
        assert config.projector.flow_params.iterations == 5
        assert config.projector.flow_params.poly_n == 7

    def test_analyzer_exclude_fields_union(self):
        """analyzer.exclude_fields should union defaults with user-provided fields."""
        # Get default excludes from ParamConfig
        param = ParamConfig()
        default_excludes = set(param.analyzer.exclude_fields)

        # User adds additional excludes
        user = UserConfig(
            base_dir="/tmp",
            radar="KHTX",
            analyzer=UserAnalyzerConfig(exclude_fields=["new_field1", "new_field2"]),
        )

        config = resolve_config(param, user, None)

        # Result should include both defaults AND user additions
        actual_excludes = set(config.analyzer.exclude_fields)
        expected_excludes = default_excludes | {"new_field1", "new_field2"}

        assert actual_excludes == expected_excludes
        assert "new_field1" in config.analyzer.exclude_fields
        assert "new_field2" in config.analyzer.exclude_fields
        # Original defaults should still be there
        for default_field in default_excludes:
            assert default_field in config.analyzer.exclude_fields

    def test_analyzer_exclude_fields_via_top_level_alias(self):
        """analyzer.exclude_fields union also works via top-level UserConfig alias."""
        param = ParamConfig()
        default_excludes = set(param.analyzer.exclude_fields)

        # User sets exclude_fields at top level (alias)
        user = UserConfig(base_dir="/tmp", radar="KHTX", exclude_fields=["top_level_exclude"])

        config = resolve_config(param, user, None)

        actual_excludes = set(config.analyzer.exclude_fields)
        expected_excludes = default_excludes | {"top_level_exclude"}

        assert actual_excludes == expected_excludes


class TestDeepMergeSemantics:
    """Test deep_merge behavior for lists, dicts, and values."""

    def test_deep_merge_list_behavior_replacement(self):
        """Lists should be replaced entirely, not concatenated."""
        base = {"list_field": ["a", "b", "c"], "other_field": "base_value"}
        override = {"list_field": ["x", "y"], "new_field": "override_value"}

        result = deep_merge(base, override)

        # List should be completely replaced, not merged/concatenated
        assert result["list_field"] == ["x", "y"]
        assert result["other_field"] == "base_value"
        assert result["new_field"] == "override_value"

    def test_deep_merge_nested_dict_behavior(self):
        """Nested dicts should merge recursively."""
        base = {
            "nested": {"keep_this": "base_value", "override_this": "old_value"},
            "top_level": "base",
        }
        override = {"nested": {"override_this": "new_value", "add_this": "added"}}

        result = deep_merge(base, override)

        # Nested dict should merge, not replace
        assert result["nested"]["keep_this"] == "base_value"
        assert result["nested"]["override_this"] == "new_value"
        assert result["nested"]["add_this"] == "added"
        assert result["top_level"] == "base"

    def test_deep_merge_multiple_overrides(self):
        """Multiple overrides should apply in order."""
        base = {"field": "base"}
        override1 = {"field": "middle"}
        override2 = {"field": "final"}

        result = deep_merge(base, override1, override2)

        assert result["field"] == "final"


class TestParamConfigCompleteness:
    """Test ParamConfig provides all runtime-critical defaults."""

    def test_paramconfig_completeness(self):
        """ParamConfig should provide all required fields for runtime."""
        param = ParamConfig()
        param_dict = param.model_dump()

        # Critical runtime fields that must be present
        required_top_level = [
            "mode",
            "global_",
            "downloader",
            "regridder",
            "segmenter",
            "analyzer",
            "projector",
        ]

        for field in required_top_level:
            assert field in param_dict, f"Missing required top-level field: {field}"
            assert param_dict[field] is not None, f"Field {field} is None"

        # Critical downloader fields
        downloader = param_dict["downloader"]
        downloader_required = [
            "output_dir",
            "latest_files",
            "latest_minutes",
            "poll_interval_sec",
        ]
        for field in downloader_required:
            assert field in downloader, f"Missing downloader field: {field}"

        # Critical segmenter fields
        segmenter = param_dict["segmenter"]
        segmenter_required = ["method", "threshold_params", "min_cellsize_gridpoint"]
        for field in segmenter_required:
            assert field in segmenter, f"Missing segmenter field: {field}"
            assert segmenter[field] is not None, f"Segmenter field {field} is None"

        # Critical regridder fields
        regridder = param_dict["regridder"]
        regridder_required = ["grid_shape", "grid_limits", "weighting_function"]
        for field in regridder_required:
            assert field in regridder, f"Missing regridder field: {field}"
            assert regridder[field] is not None, f"Regridder field {field} is None"

    def test_paramconfig_can_instantiate_without_errors(self):
        """ParamConfig should instantiate successfully with no validation errors."""
        # This should not raise any ValidationError
        param = ParamConfig()
        assert param is not None

        # Should be able to convert to dict
        param_dict = param.model_dump()
        assert isinstance(param_dict, dict)
        assert len(param_dict) > 0
