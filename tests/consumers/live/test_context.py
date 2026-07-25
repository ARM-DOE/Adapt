# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""AppContext — the session facts the dashboard shell shares with its tabs."""

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


def test_nc_files_sorted_and_run_filtered_with_legacy_fallthrough(tmp_path):
    analysis = tmp_path / "KLOT" / "analysis"
    d1, d2 = analysis / "20260704", analysis / "20260705"
    d1.mkdir(parents=True)
    d2.mkdir()
    run = "2026JUL04-1454-KLOT"
    other = "2026JUL02-2319-KLOT"
    b = d2 / f"KLOT20260705_010000_V06_{run}_analysis.nc"
    a = d1 / f"KLOT20260704_230000_V06_{run}_analysis.nc"
    c = d1 / f"KLOT20260704_235500_V06_{other}_analysis.nc"
    for p in (a, b, c):
        p.touch()

    ctx = _ctx(repo=str(tmp_path), radar="KLOT", run_sel=f"{run} …")
    assert ctx.nc_files() == [a, b]  # chronological, other run excluded

    # No filename matches the selected run → legacy fallback to the full list
    ctx_legacy = _ctx(repo=str(tmp_path), radar="KLOT", run_sel="UNKNOWN-RUN …")
    assert ctx_legacy.nc_files() == [a, c, b]


def test_nc_files_empty_when_analysis_dir_missing(tmp_path):
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
