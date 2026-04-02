"""Complete runtime initialization for Adapt pipeline.

This module handles ALL initialization responsibilities:
- Configuration resolution (CLI > User > Param)  
- Output directory setup
- Cleanup handling (--rerun)
- Configuration persistence with run ID
- Returns fully ready InternalConfig for orchestrator

Author: Bhupendra Raut
"""

import importlib.util
import shutil
import json
import re
from pathlib import Path
from typing import Dict
from datetime import datetime, timezone

from adapt.configuration.schemas.resolve import resolve_config
from adapt.configuration.schemas.param import ParamConfig
from adapt.configuration.schemas.user import UserConfig 
from adapt.configuration.schemas.cli import CLIConfig
from adapt.configuration.schemas.internal import InternalConfig
from adapt.persistence import DataRepository
from adapt.persistence.registry import RepositoryRegistry

_RUN_ID_PATTERN = re.compile(
    r"^\d{4}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}-\d{4}-[A-Z0-9]{4}$"
)


def _load_user_config_dict(config_path: str) -> dict:
    """Load user config dict from a Python (.py) or YAML (.yaml/.yml) file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    if path.suffix in ('.yaml', '.yml'):
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for YAML config files: pip install pyyaml"
            )
        with open(path) as f:
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
        if name.startswith('CONFIG'):
            obj = getattr(module, name)
            if isinstance(obj, dict):
                return obj

    raise ValueError(f"No CONFIG dict found in {path}")


def _setup_output_directories(base_dir: str) -> Dict[str, Path]:
    """Setup output directory structure.
    
    Creates the standard Adapt directory layout under base_dir.
    Uses the local directories module for path management.
    """
    from adapt.configuration.schemas.directories import setup_output_directories
    return setup_output_directories(base_dir)


def _handle_rerun_cleanup(base_dir: str, rerun: bool) -> None:
    """Handle --rerun directory cleanup if requested."""
    if not rerun:
        return
        
    base_dir_path = Path(base_dir)
    if base_dir_path.exists():
        print(f"Cleaning output directory: {base_dir_path}")
        shutil.rmtree(base_dir_path)
        print("Output directory cleaned")


def _persist_runtime_config(config: InternalConfig, run_id: str, output_dirs: Dict[str, Path]) -> None:
    """Persist final runtime configuration to output directory with run ID.
    
    Saves the complete resolved configuration for reproducibility and debugging.
    """
    config_output_dir = Path(output_dirs["base"])
    config_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save config with run ID in filename  
    config_file = config_output_dir / f"runtime_config_{run_id}.json"
    
    # Add run_id to config dict for persistence
    config_dict = config.model_dump()
    config_dict["run_id"] = run_id
    config_dict["created_at"] = datetime.now(timezone.utc).isoformat()
    
    with open(config_file, 'w') as f:
        json.dump(config_dict, f, indent=2, default=str)


_CONFIG_SKIP_KEYS = frozenset({'run_id', 'created_at', 'output_dirs'})


def _config_fingerprint(d: dict) -> dict:
    """Return a copy of config dict with transient keys removed for comparison."""
    return {k: v for k, v in d.items() if k not in _CONFIG_SKIP_KEYS}


def _find_matching_run_id(new_config_dict: dict) -> str | None:
    """Check existing runtime_config_*.json files; return run_id if config matches.

    Compares the resolved config (minus run_id, created_at, output_dirs) against
    every saved runtime_config in the output directory.  Returns the run_id of the
    most-recently-saved matching config, or None if no match is found.
    """
    base_dir = Path(new_config_dict.get("base_dir", ""))
    if not base_dir.exists():
        return None

    candidates = sorted(base_dir.glob("runtime_config_*.json"), reverse=True)
    target = _config_fingerprint(new_config_dict)

    for cfg_file in candidates:
        try:
            with open(cfg_file) as f:
                saved = json.load(f)
            if _config_fingerprint(saved) == target:
                return saved.get("run_id")
        except Exception:
            continue
    return None


def _run_id_exists(base_dir: str, run_id: str) -> bool:
    """Check if run_id exists in repository registry."""
    runs = RepositoryRegistry.get_instance(base_dir).list_runs()
    if runs.empty:
        return False
    return bool((runs["run_id"] == run_id).any())


def _load_saved_runtime_config(base_dir: str, run_id: str) -> InternalConfig:
    """Load saved runtime config JSON for an existing run."""
    cfg_path = Path(base_dir) / f"runtime_config_{run_id}.json"
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"Saved runtime config not found for run_id '{run_id}': {cfg_path}"
        )

    with open(cfg_path) as f:
        cfg_dict = json.load(f)

    # Non-schema metadata persisted for audit only.
    cfg_dict.pop("created_at", None)
    return InternalConfig.model_validate(cfg_dict)


def init_runtime_config(args) -> InternalConfig:
    """Complete runtime initialization - single entry point for Adapt.
    
    Handles ALL initialization responsibilities:
    1. Configuration resolution (CLI > User > Param)
    2. Cleanup handling (--rerun)  
    3. Output directory setup
    4. Configuration persistence with run ID
    5. Returns fully ready InternalConfig for orchestrator
    
    This is the ONLY function user scripts should call from schemas.
    Everything else is internal implementation.
    
    Parameters
    ----------
    args : argparse.Namespace
        Command line arguments with config path and all overrides
        
    Returns
    -------
    InternalConfig
        Fully validated, ready-to-use runtime configuration with:
        - All directories created and paths set
        - All CLI overrides applied
        - Run ID generated and included
        - Configuration persisted to output directory
        
    Examples
    --------
    >>> args = parser.parse_args()
    >>> config = init_runtime_config(args) 
    >>> orchestrator = PipelineOrchestrator(config)
    """
    # 0. Continuation fast-path: existing run_id reuses saved runtime config.
    user_provided_run_id = getattr(args, 'run_id', None)
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

        if _run_id_exists(base_dir_arg, normalized_run_id):
            print(f"Continuing existing run ID: {normalized_run_id}")
            print("Ignoring user config file and CLI config overrides; reusing saved runtime config for this run.")
            return _load_saved_runtime_config(base_dir_arg, normalized_run_id)

    # 1. Load and resolve configuration from all sources
    config_path = getattr(args, 'config', None)

    # Load components
    param_cfg = ParamConfig()
    if config_path:
        user_cfg_dict = _load_user_config_dict(config_path)
        user_cfg = UserConfig.model_validate(user_cfg_dict)
    else:
        user_cfg = UserConfig()  # use param defaults only
    
    # Create CLI config from args  
    cli_args = {
        k: v
        for k, v in {
            "radar": getattr(args, 'radar', None),
            "mode": getattr(args, 'mode', None), 
            "start_time": getattr(args, 'start_time', None),
            "end_time": getattr(args, 'end_time', None),
            "base_dir": getattr(args, 'base_dir', None),
            "log_level": "DEBUG" if getattr(args, 'verbose', False) else None,
            "run_id": getattr(args, 'run_id', None),
        }.items()
        if v is not None
    }
    cli_cfg = CLIConfig.model_validate(cli_args)
    
    # Resolve to final internal config
    internal_config_dict = resolve_config(param_cfg, user_cfg, cli_cfg).model_dump()
    
    # 2. Handle --rerun cleanup BEFORE directory setup
    rerun = getattr(args, 'rerun', False)
    _handle_rerun_cleanup(internal_config_dict["base_dir"], rerun)
    
    # 3. Setup output directories  
    output_dirs = _setup_output_directories(internal_config_dict["base_dir"])
    
    # Add output_dirs to config for orchestrator use
    internal_config_dict["output_dirs"] = {k: str(v) for k, v in output_dirs.items()}
    
    # 4. Generate or use provided run ID
    if normalized_run_id:
        run_id = normalized_run_id
        print(f"Using user-provided run ID (new run): {run_id}")
    else:
        run_id = _find_matching_run_id(internal_config_dict)
        if run_id:
            print(f"Reusing existing run ID (config unchanged): {run_id}")
        else:
            run_id = DataRepository.generate_run_id(internal_config_dict["downloader"]["radar"])
    internal_config_dict["run_id"] = run_id
    
    config = InternalConfig.model_validate(internal_config_dict)
    
    # 5. Persist configuration for reproducibility
    _persist_runtime_config(config, run_id, output_dirs)

    return config


# Only this function is exposed - everything else is internal
__all__ = ['init_runtime_config']
