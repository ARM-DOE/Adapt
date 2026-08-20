# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""AppContext — the session facts the dashboard shell shares with its tabs."""

import pandas as pd
import pytest

import adapt.consumers.live._context as context_mod
from adapt.consumers.live._context import AppContext

pytestmark = pytest.mark.unit


def _ctx(repo="/repo", radar="klot", run_sel="", **overrides):
    kwargs = {
        "get_repo": lambda: repo,
        "get_radar": lambda: radar,
        "get_run_sel": lambda: run_sel,
        "get_cfg": dict,
        "report_scan_time": lambda dt: None,
    }
    kwargs.update(overrides)
    return AppContext(**kwargs)


def test_repo_and_radar_normalized():
    ctx = _ctx(repo="  /repo ", radar=" klot ")
    assert ctx.repo() == "/repo"
    assert ctx.radar() == "KLOT"


def test_run_id_parses_run_selector_label():
    ctx = _ctx(run_sel="2026JUL04-1454-KLOT  (95 scans)")
    assert ctx.run_id() == "2026JUL04-1454-KLOT"


def test_run_id_none_when_blank():
    assert _ctx(run_sel="  ").run_id() is None


def test_scan_index_is_catalog_driven(tmp_path):
    """Discovery reads the catalog through the client — no directory walk,
    no filename parsing, no fall-through to other runs."""
    radar_dir = tmp_path / "KLOT"
    radar_dir.mkdir()
    (radar_dir / "catalog.db").touch()

    calls = []

    class FakeClient:
        def artifacts(self, product_type=None, radar=None, run_id=None):
            calls.append({"product_type": product_type, "radar": radar, "run_id": run_id})
            return pd.DataFrame(
                {
                    "scan_id": ["sid-a", "sid-b"],
                    "scan_time": ["2026-07-04T23:00:00Z", "2026-07-05T01:00:00Z"],
                    "file_path": ["analysis/a.nc", "analysis/b.nc"],
                }
            )

    run = "2026JUL04-1454-KLOT"
    ctx = _ctx(repo=str(tmp_path), radar="KLOT", run_sel=f"{run} …")
    ctx._client = FakeClient()
    ctx._client_repo = ctx.repo()

    index = ctx.scan_index()

    assert calls == [{"product_type": "segmentation2d", "radar": "KLOT", "run_id": run}]
    assert index == [
        ("sid-a", "2026-07-04T23:00:00Z", radar_dir / "analysis/a.nc"),
        ("sid-b", "2026-07-05T01:00:00Z", radar_dir / "analysis/b.nc"),
    ]
    assert ctx.nc_files() == [radar_dir / "analysis/a.nc", radar_dir / "analysis/b.nc"]


def test_nc_files_empty_when_radar_has_no_catalog(tmp_path):
    ctx = _ctx(repo=str(tmp_path), radar="KLOT")
    assert ctx.nc_files() == []


def test_client_cached_per_repo_path(monkeypatch):
    created, closed = [], []

    class FakeClient:
        def __init__(self, repo):
            created.append(repo)

        def close(self):
            closed.append(True)

    monkeypatch.setattr(context_mod, "RepositoryClient", FakeClient)
    repo_holder = ["/repo1"]
    ctx = _ctx(get_repo=lambda: repo_holder[0])

    c1 = ctx.client()
    assert ctx.client() is c1  # cached — same repo, no new client
    assert created == ["/repo1"]

    repo_holder[0] = "/repo2"
    c2 = ctx.client()
    assert c2 is not c1
    assert created == ["/repo1", "/repo2"]
    assert closed == [True]  # stale client was closed, not leaked
