# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""`adapt init` subcommand: creates a store, fails loudly when one exists."""

import pytest

from adapt import cli

pytestmark = pytest.mark.unit


class TestInitCommand:
    def test_init_creates_store_and_reports_root(self, tmp_path, monkeypatch, capsys):
        root = tmp_path / "repo"
        monkeypatch.setattr("sys.argv", ["adapt", "init", str(root)])

        cli.main()

        assert {p.name for p in root.iterdir()} == {"registry.db", "logs", "collections"}
        assert str(root) in capsys.readouterr().out

    def test_init_twice_exits_nonzero_with_message(self, tmp_path, monkeypatch, capsys):
        root = tmp_path / "repo"
        monkeypatch.setattr("sys.argv", ["adapt", "init", str(root)])
        cli.main()

        with pytest.raises(SystemExit) as exc:
            cli.main()

        assert exc.value.code == 1
        assert "already initialized" in capsys.readouterr().err
