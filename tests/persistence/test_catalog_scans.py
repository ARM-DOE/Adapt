# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Catalog scans: registration, product links, and completeness semantics."""

from datetime import UTC, datetime, timedelta

import pytest

from adapt.contracts.persistence import ScanRecord
from adapt.persistence.collection_catalog import ArtifactRecord
from adapt.persistence.store import Store, StoreError, init_store

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)


def _catalog_artifact(catalog, artifact_id: str) -> str:
    catalog.register_artifact(
        ArtifactRecord(
            artifact_id=artifact_id,
            artifact_type="gridded3d",
            producer="test",
            run_id="run-1",
            scan_id="abc123",
            observation_time=None,
            original_filename=None,
            source_uri=None,
            object_name=f"{artifact_id}.nc",
            checksum_sha256="0" * 64,
            size_bytes=1,
        )
    )
    return artifact_id


@pytest.fixture
def catalog(tmp_path):
    root = init_store(tmp_path / "store")
    store = Store.open(root)
    yield store.collection("KILX").catalog
    store.close()


def _record(scan_id: str = "abc123", run_id: str = "run-1", offset_s: int = 0) -> ScanRecord:
    return ScanRecord(
        run_id=run_id,
        scan_id=scan_id,
        scan_time=SCAN_TIME + timedelta(seconds=offset_s),
        source_file_name="KILX20260813_173158_V06",
    )


class TestRegisterScan:
    def test_registered_scan_is_pending_with_canonical_time(self, catalog):
        catalog.register_scan(_record())

        scan = catalog.get_scan("run-1", "abc123")
        assert scan is not None
        assert scan["status"] == "pending"
        assert scan["scan_time"] == "2026-08-13T17:31:58Z"
        assert scan["scan_date"] == "20260813"
        assert scan["source_file_name"] == "KILX20260813_173158_V06"

    def test_two_scans_sharing_a_second_in_one_run_raise(self, catalog):
        catalog.register_scan(_record(scan_id="aaa"))

        with pytest.raises(StoreError, match="scan_time"):
            catalog.register_scan(_record(scan_id="bbb"))


class TestCompleteness:
    def test_scan_completes_only_when_all_required_products_linked(self, catalog):
        catalog.register_scan(_record())
        art = {"gridded3d": "a1", "segmentation2d": "a2"}

        for product, artifact_id in art.items():
            _catalog_artifact(catalog, artifact_id)
            catalog.link_scan_product("run-1", "abc123", product, artifact_id=artifact_id)
            assert not catalog.scan_is_complete("run-1", "abc123")

        catalog.link_scan_product("run-1", "abc123", "cell_stats", table_name="cell_stats")
        assert catalog.scan_is_complete("run-1", "abc123")

    def test_auxiliary_links_never_complete_a_scan(self, catalog):
        catalog.register_scan(_record())

        catalog.link_scan_product("run-1", "abc123", "tracking", table_name="cells_by_scan")
        catalog.link_scan_product("run-1", "abc123", "volume_stats", table_name="cell_volume_stats")

        assert not catalog.scan_is_complete("run-1", "abc123")

    def test_incomplete_scans_excluded_from_complete_scans(self, catalog):
        catalog.register_scan(_record(scan_id="s1", offset_s=0))
        catalog.register_scan(_record(scan_id="s2", offset_s=300))
        for product in ("gridded3d", "segmentation2d"):
            _catalog_artifact(catalog, f"{product}-a")
            catalog.link_scan_product("run-1", "s1", product, artifact_id=f"{product}-a")
        catalog.link_scan_product("run-1", "s1", "cell_stats", table_name="cell_stats")

        complete = catalog.complete_scans("run-1")

        assert [s["scan_id"] for s in complete] == ["s1"]

    def test_complete_scans_ordered_by_scan_time(self, catalog):
        for scan_id, offset in (("late", 600), ("early", 0)):
            catalog.register_scan(_record(scan_id=scan_id, offset_s=offset))
            for product in ("gridded3d", "segmentation2d"):
                _catalog_artifact(catalog, f"{scan_id}-{product}")
                catalog.link_scan_product(
                    "run-1", scan_id, product, artifact_id=f"{scan_id}-{product}"
                )
            catalog.link_scan_product("run-1", scan_id, "cell_stats", table_name="cell_stats")

        assert [s["scan_id"] for s in catalog.complete_scans("run-1")] == ["early", "late"]

    def test_same_scan_in_two_runs_has_independent_status(self, catalog):
        catalog.register_scan(_record(run_id="run-1"))
        catalog.register_scan(_record(run_id="run-2"))
        for product in ("gridded3d", "segmentation2d"):
            _catalog_artifact(catalog, f"{product}-a")
            catalog.link_scan_product("run-1", "abc123", product, artifact_id=f"{product}-a")
        catalog.link_scan_product("run-1", "abc123", "cell_stats", table_name="cell_stats")

        assert catalog.scan_is_complete("run-1", "abc123")
        assert not catalog.scan_is_complete("run-2", "abc123")


class TestFailureAndListing:
    def test_mark_scan_failed(self, catalog):
        catalog.register_scan(_record())

        catalog.mark_scan_failed("run-1", "abc123")

        assert catalog.get_scan("run-1", "abc123")["status"] == "failed"

    def test_list_artifacts_filters_and_orders_by_time(self, catalog):
        catalog.register_scan(_record())
        for artifact_id, obs in (
            ("late", "2026-08-13T17:40:00Z"),
            ("early", "2026-08-13T17:31:58Z"),
        ):
            catalog.register_artifact(
                ArtifactRecord(
                    artifact_id=artifact_id,
                    artifact_type="segmentation2d",
                    producer="processor",
                    run_id="run-1",
                    scan_id="abc123",
                    observation_time=obs,
                    original_filename=None,
                    source_uri=None,
                    object_name=f"{artifact_id}.nc",
                    checksum_sha256="0" * 64,
                    size_bytes=1,
                )
            )
        _catalog_artifact(catalog, "other-type")

        rows = catalog.list_artifacts(run_id="run-1", artifact_type="segmentation2d")

        assert [r["artifact_id"] for r in rows] == ["early", "late"]


class TestLinkValidation:
    def test_link_to_unregistered_scan_raises(self, catalog):
        with pytest.raises(StoreError, match="ghost"):
            catalog.link_scan_product("run-1", "ghost", "gridded3d", artifact_id="a1")

    def test_link_to_uncataloged_artifact_raises(self, catalog):
        catalog.register_scan(_record())

        with pytest.raises(StoreError, match="not cataloged"):
            catalog.link_scan_product("run-1", "abc123", "gridded3d", artifact_id="phantom")

    def test_link_requires_exactly_one_target(self, catalog):
        catalog.register_scan(_record())

        with pytest.raises(ValueError, match="exactly one"):
            catalog.link_scan_product("run-1", "abc123", "gridded")
        with pytest.raises(ValueError, match="exactly one"):
            catalog.link_scan_product(
                "run-1", "abc123", "gridded3d", artifact_id="a1", table_name="t"
            )

    def test_relink_replaces_own_product(self, catalog):
        catalog.register_scan(_record())
        catalog.link_scan_product(
            "run-1", "abc123", "gridded3d", artifact_id=_catalog_artifact(catalog, "old")
        )

        catalog.link_scan_product(
            "run-1", "abc123", "gridded3d", artifact_id=_catalog_artifact(catalog, "new")
        )

        products = catalog.scan_products("run-1", "abc123")
        assert products["gridded3d"]["artifact_id"] == "new"
