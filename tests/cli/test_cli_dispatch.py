# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for CLI dispatch, PID-file handling, and config-command branches."""

import argparse
import os

import pytest

from adapt import cli

pytestmark = pytest.mark.unit


@pytest.fixture
def pid_file(tmp_path, monkeypatch):
    path = tmp_path / "pipeline.pid"
    monkeypatch.setattr(cli, "_PID_FILE", path)
    return path


class TestSingleInstance:
    def test_no_pid_file_passes(self, pid_file):
        cli._check_single_instance()

    def test_live_pid_exits_with_error(self, pid_file, capsys):
        pid_file.write_text(str(os.getpid()))  # our own PID is always alive

        with pytest.raises(SystemExit) as exc:
            cli._check_single_instance()

        assert exc.value.code == 1
        assert "already running" in capsys.readouterr().out

    def test_stale_pid_is_ignored(self, pid_file, monkeypatch):
        pid_file.write_text("12345")

        def gone(pid, sig):
            raise ProcessLookupError

        monkeypatch.setattr(cli.os, "kill", gone)
        cli._check_single_instance()

    def test_malformed_pid_file_is_ignored(self, pid_file):
        pid_file.write_text("not-a-pid")

        cli._check_single_instance()

    def test_write_then_remove_pid(self, pid_file):
        cli._write_pid()
        assert pid_file.read_text() == str(os.getpid())

        cli._remove_pid()
        assert not pid_file.exists()

    def test_remove_missing_pid_is_noop(self, pid_file):
        cli._remove_pid()


class TestMainDispatch:
    def test_no_command_exits_with_usage_error(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["adapt"])

        with pytest.raises(SystemExit) as exc:
            cli.main()

        assert exc.value.code == 2

    def test_version_prints_and_exits(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["adapt", "--version"])

        with pytest.raises(SystemExit) as exc:
            cli.main()

        assert exc.value.code == 0
        assert "adapt" in capsys.readouterr().out

    def test_config_command_writes_template(self, monkeypatch, tmp_path):
        out = tmp_path / "config.yaml"
        monkeypatch.setattr("sys.argv", ["adapt", "config", str(out)])

        cli.main()

        assert out.exists()
        assert "radar" in out.read_text()


class TestConfigCommand:
    def _args(self, output, **kw):
        return argparse.Namespace(output=str(output), pipeline="nexrad", extensions=None, **kw)

    def test_directory_output_appends_filename(self, tmp_path):
        cli._config_cmd(self._args(tmp_path))

        assert (tmp_path / "config.yaml").exists()

    def test_existing_file_aborts_when_not_confirmed(self, tmp_path, monkeypatch, capsys):
        out = tmp_path / "config.yaml"
        out.write_text("keep me")
        monkeypatch.setattr("builtins.input", lambda *_: "n")

        cli._config_cmd(self._args(out))

        assert out.read_text() == "keep me"
        assert "Aborted" in capsys.readouterr().out

    def test_existing_file_overwritten_when_confirmed(self, tmp_path, monkeypatch):
        out = tmp_path / "config.yaml"
        out.write_text("old")
        monkeypatch.setattr("builtins.input", lambda *_: "y")

        cli._config_cmd(self._args(out))

        assert out.read_text() != "old"

    def test_unknown_pipeline_raises(self, tmp_path):
        args = argparse.Namespace(output=str(tmp_path / "c.yaml"), pipeline="goes", extensions=None)

        with pytest.raises(ValueError, match="goes"):
            cli._config_cmd(args)

    def test_extensions_are_included_in_template(self, tmp_path):
        out = tmp_path / "config.yaml"
        args = argparse.Namespace(
            output=str(out),
            pipeline="nexrad",
            extensions="adapt.execution.nodes.cell_volume_stats",
        )

        cli._config_cmd(args)

        assert "cell_volume_stats" in out.read_text()


class TestPostprocessCommand:
    def test_missing_module_raises(self):
        args = argparse.Namespace(
            repository=".", module=None, input_dir=None, config=None, verbose=False
        )

        with pytest.raises(ValueError, match="module"):
            cli._postprocess_cmd(args)

    def test_open_repository_without_runs_raises(self, tmp_path):
        from adapt.persistence.registry import RepositoryRegistry

        RepositoryRegistry._instance = None
        try:
            with pytest.raises(ValueError, match="No runs found"):
                cli._open_repository(str(tmp_path), None)
        finally:
            RepositoryRegistry._instance = None
