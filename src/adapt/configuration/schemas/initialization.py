# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Complete runtime initialization for Adapt pipeline.

This module handles ALL initialization responsibilities:
- Configuration resolution (CLI > User > Param)
- Run-id continuation (config reloaded from the store registry)
- Returns fully ready InternalConfig for orchestrator

The store layout is created only by ``adapt init``; nothing here scaffolds
directories, cleans outputs, or persists config files — the resolved config
travels in the registry's run record.

Author: Bhupendra Raut
"""

import importlib.util
import re
from datetime import datetime
from pathlib import Path

from adapt.configuration.schemas.cli import CLIConfig
from adapt.configuration.schemas.errors import validated
from adapt.configuration.schemas.internal import InternalConfig
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.user import UserConfig
from adapt.persistence.errors import StoreError
from adapt.persistence.store_registry import StoreRegistry

_RUN_ID_PATTERN = re.compile(
    r"^\d{4}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}-\d{4}-[A-Z0-9]{4}$"
)


def _split_csv(value: str | None) -> list[str] | None:
    """Parse a comma-separated CLI value into a list, or None if unset/empty."""
    if not value:
        return None
    items = [p.strip() for p in value.split(",") if p.strip()]
    return items or None


_CONFIG_HEADER = """\
# Adapt Pipeline Configuration — full generated config
# Generated: {timestamp}
#
# Every parameter of every module in the pipeline is listed below, set to its
# default. Edit any value to override the default for this run; delete what you
# don't need (omitted keys keep their defaults). CLI flags (e.g. --radar, --mode)
# take precedence over this file.
#
# Set the radar under `downloader: {{ radar: KLOT }}` (or pass --radar), then run:
#   adapt run-nexrad config.yaml --radar KLOT
"""


def write_default_config(path: Path, extensions: list[str] | None = None) -> None:
    """Write a full default config.yaml to ``path``.

    The config is assembled dynamically from the registered pipeline modules:
    ParamConfig defaults (core/global sections) plus every extension module's
    own params under ``module_params``. Public — called by both
    ``init_runtime_config`` (auto-bootstrap) and ``adapt config``.
    """
    from adapt.configuration.schemas import yaml_writer
    from adapt.configuration.schemas.assemble import (
        assemble_default_config,
        assemble_descriptions,
    )

    data = assemble_default_config(extensions)
    # Default the output directory to the config's own folder so `adapt run-nexrad
    # config.yaml` works without --base-dir; the user can edit or override it.
    data = {"base_dir": str(path.parent.resolve()), **data}
    descriptions = assemble_descriptions(extensions)
    header = _CONFIG_HEADER.format(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml_writer.dump(data, descriptions, header=header), encoding="utf-8")


# Internal alias used within this module
_write_default_config = write_default_config


def _load_user_config_dict(config_path: str) -> dict:
    """Load user config dict from a Python (.py) or YAML (.yaml/.yml) file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError as err:
            raise ImportError(
                "PyYAML is required for YAML config files: pip install pyyaml"
            ) from err
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data or {}

    # Python file (legacy / advanced users)
    spec = importlib.util.spec_from_file_location("config_module", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load config module from {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find CONFIG dict
    for name in dir(module):
        if name.startswith("CONFIG"):
            obj = getattr(module, name)
            if isinstance(obj, dict):
                return obj

    raise ValueError(f"No CONFIG dict found in {path}")


def _generate_run_id(radar: str) -> str:
    """Generate a run ID in YYYYMONDD-HHMM-RADAR format (e.g. 2026MAR23-0206-KBOX)."""
    now_local = datetime.now()
    month = now_local.strftime("%b").upper()
    return f"{now_local:%Y}{month}{now_local:%d}-{now_local:%H%M}-{radar.upper()}"


def _load_run_config(base_dir: str, run_id: str) -> InternalConfig | None:
    """Reload the exact config of an existing run from the store registry."""
    registry = StoreRegistry.get_instance(base_dir)
    try:
        run = registry.get_run(run_id)
    except StoreError:
        return None
    return InternalConfig.model_validate_json(run["config_json"])


def init_runtime_config(args) -> InternalConfig:
    """Complete runtime initialization - single entry point for Adapt.

    Handles ALL initialization responsibilities:
    1. Configuration resolution (CLI > User > Param)
    2. Run-id continuation (config reloaded from the store registry)
    3. Returns fully ready InternalConfig for orchestrator

    This is the ONLY function user scripts should call from schemas.
    Everything else is internal implementation.

    Parameters
    ----------
    args : argparse.Namespace
        Command line arguments with config path and all overrides

    Returns
    -------
    InternalConfig
        Fully validated, ready-to-use runtime configuration with all CLI
        overrides applied and the run ID included. The orchestrator records
        the config in the store registry when the run begins.

    Examples
    --------
    >>> args = parser.parse_args()
    >>> config = init_runtime_config(args)
    >>> orchestrator = PipelineOrchestrator(config)
    """
    # 0. Continuation fast-path: existing run_id reuses saved runtime config.
    user_provided_run_id = getattr(args, "run_id", None)
    normalized_run_id = None
    if user_provided_run_id:
        if _RUN_ID_PATTERN.fullmatch(user_provided_run_id) is None:
            raise ValueError(
                "Invalid run_id: must match YYYYMONDD-HHMM-RADAR "
                f"(e.g. 2026MAR23-0206-KBOX), got '{user_provided_run_id}'"
            )
        normalized_run_id = user_provided_run_id.upper()

        base_dir_arg = getattr(args, "base_dir", None)
        if not base_dir_arg:
            raise ValueError("--base-dir is required when --run-id is provided")

        saved = _load_run_config(base_dir_arg, normalized_run_id)
        if saved is not None:
            print(f"Continuing existing run ID: {normalized_run_id}")
            print(
                "Ignoring user config file and CLI config overrides; "
                "reusing the run's recorded config from the store registry."
            )
            return saved

    # 1. Load and resolve configuration from all sources
    config_path = getattr(args, "config", None)
    base_dir_arg = getattr(args, "base_dir", None)

    # When no --base-dir and no config file are provided, default to CWD.
    # If config.yaml exists there, load it; if not, write a default one.
    if not base_dir_arg and not config_path:
        cwd = Path.cwd()
        cwd_config = cwd / "config.yaml"
        if cwd_config.exists():
            config_path = str(cwd_config)
        else:
            _write_default_config(cwd_config)
            print(f"[adapt] No config found — wrote defaults to {cwd_config}")
        if not base_dir_arg:
            # Inject CWD as base_dir so the CLI args dict picks it up below
            args = type(args)(**{**vars(args), "base_dir": str(cwd)})

    # Load components
    param_cfg = ParamConfig()
    if config_path:
        user_cfg_dict = _load_user_config_dict(config_path)
        user_cfg = validated(UserConfig, user_cfg_dict, source=str(config_path))
    else:
        user_cfg = UserConfig()  # type: ignore[call-arg]  # all fields optional

    # Create CLI config from args
    cli_args = {
        k: v
        for k, v in {
            "radar": getattr(args, "radar", None),
            "mode": getattr(args, "mode", None),
            "start_time": getattr(args, "start_time", None),
            "end_time": getattr(args, "end_time", None),
            "base_dir": getattr(args, "base_dir", None),
            "log_level": "DEBUG" if getattr(args, "verbose", False) else None,
            "run_id": getattr(args, "run_id", None),
            "only_modules": _split_csv(getattr(args, "only_modules", None)),
            "exclude_modules": _split_csv(getattr(args, "exclude_modules", None)),
        }.items()
        if v is not None
    }
    cli_cfg = validated(CLIConfig, cli_args, source="command-line flags")

    # Resolve to final internal config
    internal_config_dict = resolve_config(param_cfg, user_cfg, cli_cfg).model_dump()

    # 2. Generate or use the provided run ID (no implicit reuse: a new
    #    invocation is a new run unless --run-id names an existing one).
    if normalized_run_id:
        run_id = normalized_run_id
        print(f"Using user-provided run ID (new run): {run_id}")
    else:
        run_id = _generate_run_id(internal_config_dict["downloader"]["radar"])
    internal_config_dict["run_id"] = run_id

    return InternalConfig.model_validate(internal_config_dict)


# Only this function is exposed - everything else is internal
__all__ = ["init_runtime_config"]
