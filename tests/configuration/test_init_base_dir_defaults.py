# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for init_runtime_config base_dir defaulting behaviour.

When --base-dir is not provided and no config file is given:
  1. CWD is used as base_dir.
  2. If config.yaml exists in CWD it is loaded as the user config.
  3. If config.yaml does not exist in CWD a default one is written there.

These tests cover the crash reported when running:
  adapt run-nexrad --radar KLOT
without --base-dir or a config file, which produced a Pydantic ValidationError
instead of a sensible default.
"""

import argparse

import pytest

pytestmark = pytest.mark.unit


def _minimal_args(**kwargs) -> argparse.Namespace:
    defaults = {
        "config": None,
        "radar": "KLOT",
        "mode": None,
        "start_time": None,
        "end_time": None,
        "base_dir": None,
        "verbose": False,
        "run_id": None,
        "rerun": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestInitBaseDirDefaults:
    def test_missing_base_dir_uses_cwd(self, tmp_path, monkeypatch):
        """When --base-dir is absent, init_runtime_config uses CWD as base_dir."""
        monkeypatch.chdir(tmp_path)
        from adapt.configuration.schemas.initialization import init_runtime_config

        config = init_runtime_config(_minimal_args())

        assert config.base_dir == str(tmp_path)

    def test_missing_base_dir_writes_default_config_yaml(self, tmp_path, monkeypatch):
        """When config.yaml is absent from CWD, a default one is written."""
        monkeypatch.chdir(tmp_path)
        from adapt.configuration.schemas.initialization import init_runtime_config

        init_runtime_config(_minimal_args())

        assert (tmp_path / "config.yaml").exists()

    def test_missing_base_dir_uses_existing_config_yaml(self, tmp_path, monkeypatch):
        """When config.yaml exists in CWD it is loaded as user config."""
        monkeypatch.chdir(tmp_path)
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text(
            "min_cellsize_gridpoint: 12\nradar: KLOT\nbase_dir: " + str(tmp_path) + "\n"
        )

        from adapt.configuration.schemas.initialization import init_runtime_config

        config = init_runtime_config(_minimal_args())

        assert config.segmenter.min_cellsize_gridpoint == 12

    def test_downloader_output_dir_matches_base_dir(self, tmp_path, monkeypatch):
        """downloader.output_dir must equal base_dir — the two errors from the crash."""
        monkeypatch.chdir(tmp_path)
        from adapt.configuration.schemas.initialization import init_runtime_config

        config = init_runtime_config(_minimal_args())

        assert config.downloader.output_dir == str(tmp_path)

    def test_explicit_base_dir_still_respected(self, tmp_path, monkeypatch):
        """Providing --base-dir still overrides CWD default."""
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(tmp_path)
        from adapt.configuration.schemas.initialization import init_runtime_config

        config = init_runtime_config(_minimal_args(base_dir=str(other)))

        assert config.base_dir == str(other)
