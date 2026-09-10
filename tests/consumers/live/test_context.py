# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""AppContext — the session facts the dashboard shell shares with its tabs."""

from datetime import UTC, datetime

import pytest

import adapt.consumers.live._context as context_mod
from adapt.api.domain import ScanRef
from adapt.consumers.live._context import AppContext

pytestmark = pytest.mark.unit


def _ctx(repo="/repo", collection="klot", run_sel="", **overrides):
    kwargs = {
        "get_repo": lambda: repo,
        "get_collection": lambda: collection,
        "get_run_sel": lambda: run_sel,
        "get_cfg": dict,
        "report_scan_time": lambda dt: None,
    }
    kwargs.update(overrides)
    return AppContext(**kwargs)


def test_repo_and_collection_normalized():
    ctx = _ctx(repo="  /repo ", collection=" klot ")
    assert ctx.repo() == "/repo"
    assert ctx.collection() == "KLOT"


def test_run_id_parses_run_selector_label():
    ctx = _ctx(run_sel="2026JUL04-1454-KLOT  (95 scans)")
    assert ctx.run_id() == "2026JUL04-1454-KLOT"


def test_run_id_none_when_blank():
    assert _ctx(run_sel="  ").run_id() is None


class _FakeRun:
    def __init__(self, run_id):
        self.run_id = run_id


def test_timeline_uses_selected_run():
    """The timeline goes through the client's complete-only scan timeline —
    no directory walk, no filename parsing."""
    calls = []
    refs = [
        ScanRef("2026JUL04-1454-KLOT", "sid-a", datetime(2026, 7, 4, 23, tzinfo=UTC)),
        ScanRef("2026JUL04-1454-KLOT", "sid-b", datetime(2026, 7, 5, 1, tzinfo=UTC)),
    ]

    class FakeClient:
        def scan_timeline(self, collection, run_id):
            calls.append({"collection": collection, "run_id": run_id})
            return refs

    run = "2026JUL04-1454-KLOT"
    ctx = _ctx(collection="KLOT", run_sel=f"{run} …")
    ctx._client = FakeClient()
    ctx._client_repo = ctx.repo()

    assert ctx.timeline() == refs
    assert calls == [{"collection": "KLOT", "run_id": run}]


def test_timeline_defaults_to_latest_run():
    """Without a Run selection the collection's newest run is used — the
    registry's documented ordering, not a data fallback."""
    calls = []

    class FakeClient:
        def latest_run(self, collection):
            calls.append(("latest_run", collection))
            return _FakeRun("run-new")

        def scan_timeline(self, collection, run_id):
            calls.append(("timeline", collection, run_id))
            return []

    ctx = _ctx(collection="KLOT", run_sel="")
    ctx._client = FakeClient()
    ctx._client_repo = ctx.repo()

    assert ctx.timeline() == []
    assert calls == [("latest_run", "KLOT"), ("timeline", "KLOT", "run-new")]


def test_timeline_empty_when_no_runs():
    class FakeClient:
        def latest_run(self, collection):
            return None

    ctx = _ctx(collection="KLOT")
    ctx._client = FakeClient()
    ctx._client_repo = ctx.repo()

    assert ctx.timeline() == []


def test_open_raster_delegates_with_ref_identity():
    calls = []
    ref = ScanRef("run-1", "sid-a", datetime(2026, 7, 4, 23, tzinfo=UTC))

    class FakeClient:
        def open_scan_raster(self, collection, run_id, scan_id, product="segmentation2d"):
            calls.append((collection, run_id, scan_id, product))
            return "raster"

    ctx = _ctx(collection="KLOT")
    ctx._client = FakeClient()
    ctx._client_repo = ctx.repo()

    assert ctx.open_raster(ref) == "raster"
    assert calls == [("KLOT", "run-1", "sid-a", "segmentation2d")]


def test_client_cached_per_repo_path(monkeypatch):
    created, closed = [], []

    class FakeClient:
        def __init__(self, repo):
            created.append(repo)

        def close(self):
            closed.append(True)

    monkeypatch.setattr(context_mod, "StoreClient", FakeClient)
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
