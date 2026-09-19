"""Tests for init_runtime_config run-id behavior (continuation via the store registry)."""

from argparse import Namespace

import pytest

from adapt.configuration.schemas.initialization import init_runtime_config
from adapt.persistence.store import init_store
from adapt.persistence.store_registry import RunStart, StoreRegistry

_USE_TMP_BASE = object()


def _args(base_dir, run_id=None, config=None, radar="KBOX"):
    return Namespace(
        config=config,
        radar=radar,
        mode="historical",
        start_time="2026-03-23T02:00:00Z",
        end_time="2026-03-23T03:00:00Z",
        base_dir=str(base_dir) if base_dir is not None else None,
        verbose=False,
        run_id=run_id,
    )


@pytest.fixture
def store_root(tmp_path):
    return init_store(tmp_path / "store")


def _record_run(store_root, run_id, config_json: str) -> None:
    registry = StoreRegistry.get_instance(store_root)
    registry.register_collection("KBOX", source_kind="nexrad")
    registry.begin_run(
        RunStart(
            run_id=run_id,
            collection_id="KBOX",
            config_hash="h",
            config_json=config_json,
            pipeline_version="0",
            environment_json="{}",
        )
    )


def test_init_runtime_config_accepts_valid_user_run_id(store_root, capsys):
    """Valid user-provided run_id is accepted as a new run when not found."""
    run_id = "2026MAR23-0206-KBOX"
    config = init_runtime_config(_args(store_root, run_id=run_id))
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert f"Using user-provided run ID (new run): {run_id}" in out


def test_init_runtime_config_rejects_invalid_user_run_id(store_root):
    """Invalid run_id format raises a ValueError."""
    with pytest.raises(ValueError, match="Invalid run_id: must match YYYYMONDD-HHMM-RADAR"):
        init_runtime_config(_args(store_root, run_id="bad-run-id"))


def test_init_runtime_config_continues_existing_run_id(store_root, capsys):
    """An existing run_id reloads that run's exact config from the registry."""
    run_id = "2026MAR23-0206-KBOX"
    baseline = init_runtime_config(_args(store_root, run_id=None, radar="KBOX"))
    saved = baseline.model_copy(update={"run_id": run_id})
    _record_run(store_root, run_id, saved.model_dump_json())

    config = init_runtime_config(_args(store_root, run_id=run_id))
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert f"Continuing existing run ID: {run_id}" in out


def test_init_runtime_config_requires_base_dir_with_run_id(store_root):
    """--base-dir is required when --run-id is provided."""
    with pytest.raises(ValueError, match="--base-dir is required when --run-id is provided"):
        init_runtime_config(_args(None, run_id="2026MAR23-0206-KBOX"))


def test_init_runtime_config_existing_run_ignores_config_and_cli(store_root, capsys):
    """An existing run_id loads the registry's config and ignores config/CLI overrides."""
    run_id = "2026MAR23-0206-KBOX"

    baseline = init_runtime_config(_args(store_root, run_id=None, radar="KBOX"))
    saved_dict = baseline.model_dump()
    saved_dict["run_id"] = run_id
    saved_dict["downloader"]["radar"] = "KBOX"
    saved_dict["segmenter"]["method"] = "threshold"
    saved_dict["segmenter"]["threshold_params"]["threshold"] = 37.0
    from adapt.configuration.schemas.internal import InternalConfig

    _record_run(store_root, run_id, InternalConfig.model_validate(saved_dict).model_dump_json())

    # Provide conflicting config file and CLI radar override - should be ignored.
    conflict_cfg = store_root.parent / "conflict_config.py"
    conflict_cfg.write_text(
        "CONFIG = {\n"
        "    'radar': 'KTLX',\n"
        "    'base_dir': '/tmp/should_not_be_used',\n"
        "    'threshold': 99,\n"
        "}\n",
        encoding="utf-8",
    )
    config = init_runtime_config(
        _args(store_root, run_id=run_id, config=str(conflict_cfg), radar="KTLX")
    )
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert config.downloader.radar == "KBOX"
    assert config.segmenter.threshold_params.threshold == 37.0
    assert "Ignoring user config file and CLI config overrides" in out


def test_init_runtime_config_unknown_run_id_starts_new_run(store_root, capsys):
    """A run_id absent from the registry is a NEW run — nothing to silently reuse."""
    run_id = "2026MAR23-0206-KBOX"
    StoreRegistry.get_instance(store_root)  # initialized store, no runs

    config = init_runtime_config(_args(store_root, run_id=run_id))
    out = capsys.readouterr().out

    assert config.run_id == run_id
    assert "new run" in out
