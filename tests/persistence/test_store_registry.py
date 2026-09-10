# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreRegistry: collection registration and the single run lifecycle."""

import pytest

from adapt.persistence.store import StoreError, init_store
from adapt.persistence.store_registry import RunStart, StoreRegistry

pytestmark = pytest.mark.unit


@pytest.fixture
def registry(tmp_path):
    root = init_store(tmp_path / "store")
    reg = StoreRegistry(root)
    yield reg
    reg.close()


def _start(run_id: str = "run-1", collection_id: str = "KILX") -> RunStart:
    return RunStart(
        run_id=run_id,
        collection_id=collection_id,
        config_hash="cfg-hash",
        config_json="{}",
        pipeline_version="1.2.3",
        environment_json="{}",
    )


class TestCollections:
    def test_register_and_list(self, registry):
        registry.register_collection("KILX", source_kind="nexrad", lat=40.15, lon=-89.34)

        rows = registry.list_collections()

        assert [r["collection_id"] for r in rows] == ["KILX"]
        assert rows[0]["source_kind"] == "nexrad"
        assert rows[0]["location_lat"] == pytest.approx(40.15)

    def test_register_twice_is_idempotent(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")
        registry.register_collection("KILX", source_kind="nexrad")

        assert len(registry.list_collections()) == 1


class TestRunLifecycle:
    def test_begin_run_marks_running(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")

        registry.begin_run(_start())

        run = registry.get_run("run-1")
        assert run["status"] == "running"
        assert run["collection_id"] == "KILX"
        assert run["config_hash"] == "cfg-hash"
        assert run["pipeline_version"] == "1.2.3"
        assert run["started_at"]
        assert run["ended_at"] is None

    def test_begin_run_unknown_collection_raises(self, registry):
        with pytest.raises(StoreError, match="KILX"):
            registry.begin_run(_start())

    def test_begin_run_duplicate_run_raises(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")
        registry.begin_run(_start())

        with pytest.raises(StoreError, match="run-1"):
            registry.begin_run(_start())

    @pytest.mark.parametrize("status", ["completed", "cancelled", "failed"])
    def test_finalize_to_each_terminal_state(self, registry, status):
        registry.register_collection("KILX", source_kind="nexrad")
        registry.begin_run(_start())

        registry.finalize_run("run-1", status)

        run = registry.get_run("run-1")
        assert run["status"] == status
        assert run["ended_at"]

    def test_double_finalize_raises(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")
        registry.begin_run(_start())
        registry.finalize_run("run-1", "completed")

        with pytest.raises(StoreError, match="run-1"):
            registry.finalize_run("run-1", "failed")

    def test_finalize_unknown_run_raises(self, registry):
        with pytest.raises(StoreError, match="nope"):
            registry.finalize_run("nope", "completed")

    def test_finalize_nonterminal_status_raises(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")
        registry.begin_run(_start())

        with pytest.raises(ValueError, match="running"):
            registry.finalize_run("run-1", "running")


class TestCollectionLocation:
    def test_location_set_once_then_kept(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")

        registry.ensure_collection_location("KILX", lat=40.15, lon=-89.34)
        registry.ensure_collection_location("KILX", lat=0.0, lon=0.0)  # ignored

        row = registry.get_collection("KILX")
        assert row["location_lat"] == pytest.approx(40.15)
        assert row["location_lon"] == pytest.approx(-89.34)

    def test_unknown_collection_raises(self, registry):
        with pytest.raises(StoreError, match="KILX"):
            registry.ensure_collection_location("KILX", lat=1.0, lon=2.0)


class TestLatestRun:
    def test_latest_run_returns_newest(self, registry):
        registry.register_collection("KILX", source_kind="nexrad")
        registry.begin_run(_start("run-1"))
        registry.begin_run(_start("run-2"))

        assert registry.latest_run()["run_id"] == "run-2"

    def test_latest_run_none_when_empty(self, registry):
        assert registry.latest_run() is None


class TestInstanceCache:
    def test_get_instance_is_cached_per_root(self, tmp_path):
        root = init_store(tmp_path / "store")

        first = StoreRegistry.get_instance(root)
        second = StoreRegistry.get_instance(root)

        assert first is second
        StoreRegistry.close_all()

    def test_uninitialized_root_raises_naming_adapt_init(self, tmp_path):
        with pytest.raises(StoreError, match="adapt init"):
            StoreRegistry(tmp_path)
