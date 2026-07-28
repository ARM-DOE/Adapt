"""Tests for init_runtime_config run-id behavior."""

import json
import re
from argparse import Namespace

import pytest

from adapt.configuration.schemas.initialization import init_runtime_config
from adapt.persistence.registry import RepositoryRegistry

_USE_TMP_BASE = object()


def _args(tmp_path, run_id=None, config=None, base_dir=_USE_TMP_BASE, radar="KBOX"):
    resolved_base_dir = str(tmp_path) if base_dir is _USE_TMP_BASE else base_dir
    return Namespace(
        config=config,
        radar=radar,
        mode="historical",
        start_time="2026-03-23T02:00:00Z",
        end_time="2026-03-23T03:00:00Z",
        base_dir=resolved_base_dir,
        verbose=False,
        run_id=run_id,
        rerun=False,
    )


def test_init_runtime_config_accepts_valid_user_run_id(tmp_path, capsys):
    """Valid user-provided run_id is accepted as a new run when not found."""
    run_id = "2026MAR23-0206-KBOX"
    config = init_runtime_config(_args(tmp_path, run_id=run_id))
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert f"Using user-provided run ID (new run): {run_id}" in out


def test_init_runtime_config_rejects_invalid_user_run_id(tmp_path):
    """Invalid run_id format raises a ValueError."""
    with pytest.raises(ValueError, match="Invalid run_id: must match YYYYMONDD-HHMM-RADAR"):
        init_runtime_config(_args(tmp_path, run_id="bad-run-id"))


def test_init_runtime_config_continues_existing_run_id(tmp_path, capsys):
    """Existing user-provided run_id is treated as continuation."""
    run_id = "2026MAR23-0206-KBOX"
    baseline = init_runtime_config(_args(tmp_path, run_id=None, radar="KBOX"))
    saved_cfg = baseline.model_dump()
    saved_cfg["run_id"] = run_id
    saved_cfg["created_at"] = "2026-03-23T02:06:00+00:00"
    with open(tmp_path / f"runtime_config_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(saved_cfg, f)

    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar("KBOX")
    registry.register_run(run_id=run_id, radar="KBOX", mode="historical")

    config = init_runtime_config(_args(tmp_path, run_id=run_id))
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert f"Continuing existing run ID: {run_id}" in out


def test_init_runtime_config_requires_base_dir_with_run_id(tmp_path):
    """--base-dir is required when --run-id is provided."""
    with pytest.raises(ValueError, match="--base-dir is required when --run-id is provided"):
        init_runtime_config(_args(tmp_path, run_id="2026MAR23-0206-KBOX", base_dir=None))


def test_init_runtime_config_existing_run_ignores_config_and_cli(tmp_path, capsys):
    """Existing run_id must load saved runtime config and ignore incoming config/CLI overrides."""
    run_id = "2026MAR23-0206-KBOX"

    # Create a saved runtime config for the run.
    baseline = init_runtime_config(_args(tmp_path, run_id=None, radar="KBOX"))
    saved_cfg = baseline.model_dump()
    saved_cfg["run_id"] = run_id
    saved_cfg["downloader"]["radar"] = "KBOX"
    saved_cfg["segmenter"]["method"] = "threshold"
    saved_cfg["segmenter"]["threshold_params"]["threshold"] = 37.0
    saved_cfg["created_at"] = "2026-03-23T02:06:00+00:00"
    with open(tmp_path / f"runtime_config_{run_id}.json", "w", encoding="utf-8") as f:
        json.dump(saved_cfg, f)

    # Register existing run in repository registry.
    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar("KBOX")
    registry.register_run(run_id=run_id, radar="KBOX", mode="historical")

    # Provide conflicting config file and CLI radar override - should be ignored.
    conflict_cfg = tmp_path / "conflict_config.py"
    conflict_cfg.write_text(
        "CONFIG = {\n"
        "    'radar': 'KTLX',\n"
        "    'base_dir': '/tmp/should_not_be_used',\n"
        "    'threshold': 99,\n"
        "}\n",
        encoding="utf-8",
    )
    config = init_runtime_config(
        _args(
            tmp_path,
            run_id=run_id,
            config=str(conflict_cfg),
            radar="KTLX",
        )
    )
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert config.downloader.radar == "KBOX"
    assert config.segmenter.threshold_params.threshold == 37.0
    assert "Ignoring user config file and CLI config overrides" in out


def test_init_runtime_config_existing_run_missing_saved_runtime_config(tmp_path):
    """Existing run_id without runtime_config_<run_id>.json should fail loudly."""
    run_id = "2026MAR23-0206-KBOX"
    registry = RepositoryRegistry.get_instance(tmp_path)
    registry.register_radar("KBOX")
    registry.register_run(run_id=run_id, radar="KBOX", mode="historical")

    with pytest.raises(FileNotFoundError, match="Saved runtime config not found"):
        init_runtime_config(_args(tmp_path, run_id=run_id))


def test_init_runtime_config_auto_generates_formatted_run_id(tmp_path):
    """Auto-generated run_id follows YYYYMONDD-HHMM-RADAR format."""
    config = init_runtime_config(_args(tmp_path, run_id=None))
    assert re.match(
        r"^\d{4}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}-\d{4}-KBOX$",
        config.run_id,
    )
