# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Direct tests for RadarCatalog: items, progress, scans, and lineage.

Each test exercises one catalog behaviour against a fresh on-disk SQLite
catalog in tmp_path — no pipeline, no radar data.
"""

import json
from datetime import UTC, datetime

import pytest

from adapt.contracts import ScanRecord
from adapt.persistence.catalog import RadarCatalog
from adapt.utils.time import to_scan_iso

pytestmark = pytest.mark.unit

_RUN = "run001"
_T0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
_T1 = datetime(2024, 6, 1, 12, 5, 0, tzinfo=UTC)


@pytest.fixture
def catalog(tmp_path):
    radar_dir = tmp_path / "KTST"
    radar_dir.mkdir()
    cat = RadarCatalog(radar_dir)
    yield cat
    cat.close()


def _register(catalog, item_id="item-1", item_type="analysis2d", scan_time=_T0, **kw):
    catalog.register_item(
        item_id=item_id,
        run_id=_RUN,
        item_type=item_type,
        scan_time=scan_time.isoformat(),
        file_path=f"analysis/{item_id}.parquet",
        **kw,
    )


class TestItems:
    def test_register_and_get_item_round_trip(self, catalog):
        _register(catalog, metadata={"threshold": 30}, parent_ids=["raw-1"])

        item = catalog.get_item("item-1")

        assert item["run_id"] == _RUN
        assert item["item_type"] == "analysis2d"
        assert json.loads(item["parent_ids"]) == ["raw-1"]
        assert json.loads(item["metadata"]) == {"threshold": 30}

    def test_get_unknown_item_returns_none(self, catalog):
        assert catalog.get_item("nope") is None

    def test_update_item_status_records_error(self, catalog):
        _register(catalog)

        catalog.update_item_status("item-1", "failed", error_message="disk full")

        item = catalog.get_item("item-1")
        assert item["status"] == "failed"
        assert item["error_message"] == "disk full"

    def test_query_items_filters_by_type_run_and_status(self, catalog):
        _register(catalog, item_id="a", item_type="analysis2d")
        _register(catalog, item_id="b", item_type="gridded3d")
        catalog.update_item_status("b", "failed")

        df = catalog.query_items(item_type="analysis2d", run_id=_RUN, status="complete")

        assert df["item_id"].tolist() == ["a"]

    def test_query_items_respects_limit(self, catalog):
        _register(catalog, item_id="a", scan_time=_T0)
        _register(catalog, item_id="b", scan_time=_T1)

        df = catalog.query_items(limit=1)

        assert len(df) == 1
        assert df.iloc[0]["item_id"] == "b"  # newest first

    def test_get_latest_item_returns_newest_complete(self, catalog):
        _register(catalog, item_id="old", scan_time=_T0)
        _register(catalog, item_id="new", scan_time=_T1)

        latest = catalog.get_latest_item("analysis2d", run_id=_RUN)

        assert latest["item_id"] == "new"

    def test_get_latest_item_without_run_filter(self, catalog):
        _register(catalog, item_id="only")

        assert catalog.get_latest_item("analysis2d")["item_id"] == "only"

    def test_get_latest_item_none_when_no_match(self, catalog):
        assert catalog.get_latest_item("gridded3d") is None


class TestProgress:
    def test_get_progress_none_before_any_update(self, catalog):
        assert catalog.get_progress(_RUN) is None

    def test_update_progress_inserts_then_updates(self, catalog):
        catalog.update_progress(_RUN, latest_analyzed_time=_T0.isoformat())
        catalog.update_progress(_RUN, num_items_complete=5)

        progress = catalog.get_progress(_RUN)

        assert progress["latest_analyzed_time"] == _T0.isoformat()
        assert progress["num_items_complete"] == 5
        assert progress["last_updated"] is not None

    def test_update_progress_with_no_fields_is_noop(self, catalog):
        catalog.update_progress(_RUN)

        assert catalog.get_progress(_RUN) is None


class TestScans:
    """Scan registry: identity-keyed rows, times as metadata (lean, store-aligned)."""

    @staticmethod
    def _record(scan_id="sid-aaaa", scan_time=_T0, source="KTST_20240601_120000_V06", **kw):
        return ScanRecord(
            run_id=_RUN,
            scan_id=scan_id,
            scan_time=scan_time,
            source_file_name=source,
            **kw,
        )

    def test_register_scan_stores_identity_and_canonical_time(self, catalog):
        catalog.register_scan(self._record())

        scan = catalog.get_scan(_RUN, "sid-aaaa")

        assert scan["run_id"] == _RUN
        assert scan["scan_id"] == "sid-aaaa"
        assert scan["scan_time"] == to_scan_iso(_T0)
        assert scan["scan_date"] == "20240601"
        assert scan["source_file_name"] == "KTST_20240601_120000_V06"
        assert scan["processing_status"] == "complete"

    def test_register_scan_upserts_on_run_and_scan_id(self, catalog):
        catalog.register_scan(self._record())
        catalog.register_scan(self._record(source="KTST_renamed_V06"))

        df = catalog.list_scans(run_id=_RUN)

        assert len(df) == 1
        assert df["source_file_name"].tolist() == ["KTST_renamed_V06"]

    def test_get_scan_unknown_returns_none(self, catalog):
        assert catalog.get_scan(_RUN, "sid-none") is None

    def test_same_scan_in_two_runs_is_independent(self, catalog):
        catalog.register_scan(self._record())
        catalog.register_scan(
            ScanRecord(
                run_id="run002",
                scan_id="sid-aaaa",
                scan_time=_T0,
                source_file_name="KTST_20240601_120000_V06",
            )
        )

        assert len(catalog.list_scans()) == 2
        assert catalog.get_scan("run002", "sid-aaaa")["run_id"] == "run002"

    def test_coverage_times_are_optional_per_source_metadata(self, catalog):
        catalog.register_scan(self._record())
        catalog.register_scan(
            self._record(scan_id="sid-bbbb", scan_time=_T1, start_time=_T1, end_time=_T1)
        )

        bare = catalog.get_scan(_RUN, "sid-aaaa")
        rich = catalog.get_scan(_RUN, "sid-bbbb")

        assert bare["start_time"] is None and bare["end_time"] is None
        assert rich["start_time"] is not None and rich["end_time"] is not None

    def test_list_scans_filters_by_time_window_and_run(self, catalog):
        catalog.register_scan(self._record())
        catalog.register_scan(self._record(scan_id="sid-bbbb", scan_time=_T1))

        df = catalog.list_scans(start_time=_T1, run_id=_RUN)

        assert df["scan_time"].tolist() == [to_scan_iso(_T1)]
        assert df["scan_id"].tolist() == ["sid-bbbb"]

    def test_list_scans_ordered_by_scan_time(self, catalog):
        catalog.register_scan(self._record(scan_id="sid-bbbb", scan_time=_T1))
        catalog.register_scan(self._record())

        df = catalog.list_scans(run_id=_RUN)

        assert df["scan_id"].tolist() == ["sid-aaaa", "sid-bbbb"]

    def test_two_different_scans_at_one_time_raise(self, catalog):
        # A second scan_id at the same (run, scan_time) is a data problem
        # (e.g. duplicate download) — registration surfaces it loudly instead
        # of letting time-ordered reads silently pick one.
        import sqlite3

        catalog.register_scan(self._record())
        with pytest.raises(sqlite3.IntegrityError):
            catalog.register_scan(self._record(scan_id="sid-other"))


def test_pre_identity_catalog_raises_recreate_at_open(tmp_path):
    # Opening a catalog created before scan identity must fail with recreate
    # guidance — BEFORE the schema script trips an ugly OperationalError on
    # the scan_id index it cannot create against the old items table.
    import sqlite3

    radar_dir = tmp_path / "KOLD"
    radar_dir.mkdir()
    conn = sqlite3.connect(str(radar_dir / "catalog.db"))
    conn.execute(
        "CREATE TABLE items (item_id TEXT PRIMARY KEY, run_id TEXT, item_type TEXT, "
        "scan_time TEXT, file_path TEXT)"  # pre-identity: no scan_id column
    )
    conn.commit()
    conn.close()

    with pytest.raises(RuntimeError, match="Recreate catalog.db"):
        RadarCatalog(radar_dir)
