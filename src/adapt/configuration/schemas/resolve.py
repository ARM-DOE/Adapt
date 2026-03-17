"""Configuration resolution and merging logic.

This module provides the SINGLE AUTHORITATIVE ENTRYPOINT for configuration 
resolution: resolve_config(). It merges ParamConfig, UserConfig, and CLIConfig 
in the correct precedence order and returns a validated InternalConfig.

Precedence (highest to lowest):
1. CLIConfig (command-line overrides) 
2. UserConfig (user file overrides)
3. ParamConfig (expert defaults)

Merge Semantics:
- Nested dictionaries: recursively merged (child keys from higher-priority 
  configs override lower-priority configs)
- Lists: completely REPLACED (higher-priority list replaces entire 
  lower-priority list; no concatenation)
- Other values: replaced

Special Cases:
- analyzer.exclude_fields: UNIONED (default excludes + user excludes)
- projector.max_projection_steps: capped at 10 for safety
- mode inference: auto-set to 'historical' if start_time/end_time provided 
  but no explicit mode

Example precedence:
- ParamConfig: {"radar_variables": ["A", "B"], "threshold": 30}
- UserConfig: {"radar_variables": ["C"], "threshold": 40}
- Result: {"radar_variables": ["C"], "threshold": 40}  # List replaced
"""

from typing import Union, Optional, Any
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.user import UserConfig
from adapt.configuration.schemas.cli import CLIConfig
from adapt.configuration.schemas.internal import InternalConfig


def deep_merge(base: dict, *overrides: dict) -> dict:
    """Deep merge multiple dictionaries.
    
    Later dictionaries override earlier ones. Nested dictionaries are
    merged recursively; other values are replaced.
    
    Parameters
    ----------
    base : dict
        Base dictionary (lowest priority)
    *overrides : dict
        Override dictionaries (higher priority, left to right)
    
    Returns
    -------
    dict
        Merged dictionary
    
    Examples
    --------
    >>> base = {"a": 1, "b": {"c": 2, "d": 3}}
    >>> override = {"b": {"d": 4, "e": 5}, "f": 6}
    >>> deep_merge(base, override)
    {'a': 1, 'b': {'c': 2, 'd': 4, 'e': 5}, 'f': 6}
    """
    result = base.copy()
    
    for override in overrides:
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursive merge for nested dicts
                result[key] = deep_merge(result[key], value)
            else:
                # Replace value
                result[key] = value
    
    return result


def resolve_config(
    param_cfg: Union[dict, ParamConfig],
    user_cfg: Optional[Union[dict, UserConfig]] = None,
    cli_cfg: Optional[Union[dict, CLIConfig]] = None,
) -> InternalConfig:
    """Resolve final runtime configuration from param, user, and CLI configs.
    
    This is the SINGLE ENTRYPOINT for configuration resolution. It validates
    and merges configs in the correct precedence order, then returns an
    immutable InternalConfig for runtime use.
    
    Precedence (highest to lowest):
    1. CLIConfig (command-line overrides)
    2. UserConfig (user file overrides)
    3. ParamConfig (expert defaults)
    
    Parameters
    ----------
    param_cfg : dict or ParamConfig
        Expert configuration with complete defaults. Required.
    user_cfg : dict or UserConfig, optional
        User configuration with overrides. If None or empty, uses only param defaults.
    cli_cfg : dict or CLIConfig, optional
        Command-line overrides. If None or empty, no CLI overrides applied.
    
    Returns
    -------
    InternalConfig
        Fully validated, immutable runtime configuration
    
    Raises
    ------
    ValidationError
        If any config fails Pydantic validation
    
    Examples
    --------
    >>> from adapt.configuration.schemas import resolve_config, ParamConfig, UserConfig
    >>> 
    >>> # Load expert defaults
    >>> param = ParamConfig()
    >>> 
    >>> # User wants higher threshold
    >>> user = UserConfig(threshold=35, radar="KHTX")
    >>> 
    >>> # Resolve to internal config
    >>> config = resolve_config(param, user)
    >>> config.segmenter.threshold
    35.0
    >>> config.downloader.radar
    'KHTX'
    """
    # Validate/convert inputs to Pydantic models
    if not isinstance(param_cfg, ParamConfig):
        param = ParamConfig.model_validate(param_cfg)
    else:
        param = param_cfg
    
    if user_cfg is None or (isinstance(user_cfg, dict) and not user_cfg):
        user = UserConfig()
    elif not isinstance(user_cfg, UserConfig):
        user = UserConfig.model_validate(user_cfg)
    else:
        user = user_cfg
    
    if cli_cfg is None or (isinstance(cli_cfg, dict) and not cli_cfg):
        cli = CLIConfig()
    elif not isinstance(cli_cfg, CLIConfig):
        cli = CLIConfig.model_validate(cli_cfg)
    else:
        cli = cli_cfg
    
    # Convert to dicts for merging
    param_dict = param.model_dump(by_alias=True)  # Use 'global' not 'global_'
    user_overrides = user.to_internal_overrides()
    cli_overrides = cli.to_internal_overrides()
    
    # Deep merge: param < user < cli
    merged = deep_merge(param_dict, user_overrides, cli_overrides)
    
    # Special handling for analyzer.exclude_fields: AMEND instead of REPLACE
    # We want to keep the internal defaults and add user-specified ones.
    default_excludes = set(param.analyzer.exclude_fields)
    user_excludes = set()
    
    # Check top-level UserConfig alias
    if user.exclude_fields:
        user_excludes.update(user.exclude_fields)
    
    # Check nested UserConfig section
    if user.analyzer and user.analyzer.exclude_fields:
        user_excludes.update(user.analyzer.exclude_fields)
    
    # Check CLI overrides (if we ever add analyzer to CLIConfig)
    if hasattr(cli, 'analyzer') and cli.analyzer and cli.analyzer.exclude_fields:
        user_excludes.update(cli.analyzer.exclude_fields)
    
    if user_excludes:
        merged.setdefault("analyzer", {})["exclude_fields"] = list(default_excludes | user_excludes)

    # Schema responsibility: Infer historical mode from times if mode not explicitly set
    # This handles cases where times are provided but mode is not.
    # We check user.mode and cli.mode directly to see if they were explicitly set.
    if user.mode is None and cli.mode is None:
        downloader_dict = merged.get("downloader", {})
        if downloader_dict.get("start_time") or downloader_dict.get("end_time"):
            merged["mode"] = "historical"
    
    # Cap max_projection_steps at 10
    if "projector" in merged and "max_projection_steps" in merged["projector"]:
        merged["projector"]["max_projection_steps"] = min(
            merged["projector"]["max_projection_steps"], 10
        )
    
    # Add mode to downloader config for validation
    if "downloader" in merged and "mode" not in merged["downloader"]:
        merged["downloader"]["mode"] = merged.get("mode", "realtime")
    
    # Validate and freeze as InternalConfig
    internal = InternalConfig.model_validate(merged)
    
    # Conditional validation: Historical mode requires start_time and end_time
    if internal.downloader.mode == "historical":
        if not internal.downloader.start_time:
            raise ValueError(
                "start_time is required when mode='historical'. "
                "Please provide start_time in user config or CLI arguments."
            )
        if not internal.downloader.end_time:
            raise ValueError(
                "end_time is required when mode='historical'. "
                "Please provide end_time in user config or CLI arguments."
            )
    
    return internal
