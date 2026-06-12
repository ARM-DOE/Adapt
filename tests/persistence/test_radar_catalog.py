# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Direct tests for RadarCatalog: items, progress, scans, and lineage.

Each test exercises one catalog behaviour against a fresh on-disk SQLite
catalog in tmp_path — no pipeline, no radar data.
"""

import json
from datetime import UTC, datetime

import pytest

from adapt.persistence.catalog import RadarCatalog

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
    def test_register_scan_is_idempotent(self, catalog):
        first = catalog.register_scan(_T0, _RUN, nexrad_file_name="KTST_f1")
        second = catalog.register_scan(_T0, _RUN)

        assert first == second

    def test_get_scan_returns_registered_record(self, catalog):
        catalog.register_scan(_T0, _RUN)

        scan = catalog.get_scan(_T0)

        assert scan["run_id"] == _RUN
        assert scan["processing_status"] == "pending"

    def test_get_scan_unknown_time_returns_none(self, catalog):
        assert catalog.get_scan(_T1) is None

    def test_get_scan_by_id(self, catalog):
        scan_id = catalog.register_scan(_T0, _RUN)

        assert catalog.get_scan_by_id(scan_id)["scan_id"] == scan_id
        assert catalog.get_scan_by_id("nope") is None

    def test_link_item_marks_scan_partial(self, catalog):
        catalog.register_scan(_T0, _RUN)
        _register(catalog, item_id="seg-1", item_type="segmentation2d")

        catalog.link_item_to_scan(_T0, "segmentation2d", "seg-1", num_cells=3)

        scan = catalog.get_scan(_T0)
        assert scan["segmentation2d_item_id"] == "seg-1"
        assert scan["num_cells"] == 3
        assert scan["processing_status"] == "partial"

    def test_linking_all_core_items_marks_scan_complete(self, catalog):
        catalog.register_scan(_T0, _RUN)
        for item_type in ("gridded3d", "segmentation2d", "analysis2d"):
            _register(catalog, item_id=f"{item_type}-1", item_type=item_type)
            catalog.link_item_to_scan(_T0, item_type, f"{item_type}-1")

        assert catalog.get_scan(_T0)["processing_status"] == "complete"

    def test_link_unknown_item_type_is_ignored(self, catalog):
        catalog.register_scan(_T0, _RUN)

        catalog.link_item_to_scan(_T0, "bogus_type", "x-1")

        assert catalog.get_scan(_T0)["processing_status"] == "pending"

    def test_list_scans_filters_by_time_window_and_run(self, catalog):
        catalog.register_scan(_T0, _RUN)
        catalog.register_scan(_T1, _RUN)

        df = catalog.list_scans(start_time=_T1, run_id=_RUN)

        assert df["scan_time"].tolist() == [_T1.isoformat()]

    def test_list_scans_filters_by_status(self, catalog):
        catalog.register_scan(_T0, _RUN)

        assert catalog.list_scans(status="complete").empty
        assert len(catalog.list_scans(status="pending")) == 1

    def test_get_latest_scan_returns_newest_complete(self, catalog):
        for t in (_T0, _T1):
            catalog.register_scan(t, _RUN)
            for item_type in ("gridded3d", "segmentation2d", "analysis2d"):
                item_id = f"{item_type}-{t:%H%M}"
                _register(catalog, item_id=item_id, item_type=item_type, scan_time=t)
                catalog.link_item_to_scan(t, item_type, item_id)

        latest = catalog.get_latest_scan(run_id=_RUN)

        assert latest["scan_time"] == _T1.isoformat()
        assert catalog.get_latest_scan()["scan_time"] == _T1.isoformat()

    def test_get_latest_scan_none_when_nothing_complete(self, catalog):
        catalog.register_scan(_T0, _RUN)

        assert catalog.get_latest_scan() is None
