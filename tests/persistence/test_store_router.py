# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreOutputRouter: spec dispatch onto the store (objects + products + links)."""

import sqlite3
from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd
import pytest
import xarray as xr

from adapt.contracts.persistence import (
    NetcdfArtifact,
    PersistenceMeta,
    ProductTableWrite,
    ScanRecord,
    TrackTablesWrite,
)
from adapt.persistence.errors import StoreError
from adapt.persistence.output_router import StoreOutputRouter
from adapt.persistence.store import Store, init_store

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)
RUN = "run-1"
SID = "abc123"

NC_SPEC = NetcdfArtifact(
    key="analysis_ds", product_type="segmentation2d", producer="processor", description="test"
)
TABLE_SPEC = ProductTableWrite(
    key="cell_stats", table="cell_stats", primary_key=("run_id", "scan_id", "cell_label")
)
TRACK_SPEC = TrackTablesWrite(
    tracked_key="tracked_cells",
    events_key="cell_events",
    stats_key="cell_stats",
    adjacency_key="cell_adjacency",
)


def _module(name: str, *specs) -> SimpleNamespace:
    return SimpleNamespace(name=name, persistence=tuple(specs))


def _meta(scan_id: str | None = SID, scan_time: datetime | None = SCAN_TIME) -> PersistenceMeta:
    return PersistenceMeta(
        scan_time=scan_time,
        scan_id=scan_id,
        run_id=RUN,
        source_file="KILX20260813_173158_V06",
        collection_id="KILX",
    )


def _dataset() -> xr.Dataset:
    return xr.Dataset({"reflectivity": ("x", [1.0, 2.0])})


def _tracking_result() -> dict:
    return {
        "tracked_cells": pd.DataFrame(
            {"cell_label": [1], "cell_uid": ["u1"], "area": [10.0], "max_reflectivity": [45.0]}
        ),
        "cell_events": pd.DataFrame(),
        "cell_stats": pd.DataFrame({"cell_label": [1], "cell_area_sqkm": [10.0]}),
        "cell_adjacency": pd.DataFrame(
            columns=["cell_label_a", "cell_label_b", "touching_boundary_pixels"]
        ),
    }


@pytest.fixture
def store(tmp_path):
    root = init_store(tmp_path / "store")
    st = Store.open(root)
    yield st
    st.close()


@pytest.fixture
def collection(store):
    return store.collection("KILX")


@pytest.fixture
def raw_artifact_id(collection):
    """The raw Level-II object every per-scan product descends from."""
    from adapt.persistence.objects import ArtifactMeta, ObjectStore

    objects = ObjectStore(collection.objects_dir, collection.catalog)
    handle = objects.begin(suffix=".raw")
    handle.staging_path.write_bytes(b"level2")
    record = objects.commit(
        handle,
        ArtifactMeta(
            artifact_type="raw_volume",
            producer="acquisition",
            run_id=RUN,
            scan_id=SID,
            observation_time=SCAN_TIME,
            original_filename="KILX20260813_173158_V06",
            source_uri="s3://nexrad/KILX20260813_173158_V06",
        ),
    )
    collection.catalog.register_scan(
        ScanRecord(
            run_id=RUN,
            scan_id=SID,
            scan_time=SCAN_TIME,
            source_file_name="KILX20260813_173158_V06",
        )
    )
    return record.artifact_id


@pytest.fixture
def router(collection):
    return StoreOutputRouter(collection)


def _products_rows(collection, sql):
    conn = sqlite3.connect(collection.products_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()


class TestNetcdfArtifact:
    def test_writes_object_with_catalog_row_lineage_and_link(
        self, router, collection, raw_artifact_id
    ):
        ds = _dataset()

        router.persist([_module("tracking", NC_SPEC)], {"analysis_ds": ds}, _meta())

        products = collection.catalog.scan_products(RUN, SID)
        artifact_id = products["segmentation2d"]["artifact_id"]
        row = collection.catalog.get_artifact(artifact_id)
        assert row["artifact_type"] == "segmentation2d"
        assert row["producer"] == "processor"
        assert row["observation_time"] == "2026-08-13T17:31:58Z"
        object_path = collection.objects_dir / row["object_name"]
        assert object_path.exists()
        assert collection.catalog.parents_of(artifact_id) == [raw_artifact_id]

        reopened = xr.open_dataset(object_path)
        try:
            assert reopened.attrs["scan_id"] == SID
            assert reopened.attrs["scan_time"] == "2026-08-13T17:31:58Z"
            assert reopened.attrs["radar"] == "KILX"
        finally:
            reopened.close()

    def test_without_raw_artifact_raises(self, router, collection):
        collection.catalog.register_scan(
            ScanRecord(run_id=RUN, scan_id=SID, scan_time=SCAN_TIME, source_file_name="f")
        )

        with pytest.raises(StoreError, match="raw_volume"):
            router.persist([_module("tracking", NC_SPEC)], {"analysis_ds": _dataset()}, _meta())

    def test_absent_key_skips(self, router, raw_artifact_id):
        router.persist([_module("tracking", NC_SPEC)], {}, _meta())

    def test_missing_scan_id_raises(self, router):
        with pytest.raises(ValueError, match="scan_id"):
            router.persist(
                [_module("tracking", NC_SPEC)], {"analysis_ds": _dataset()}, _meta(scan_id=None)
            )


class TestProductTableWrite:
    def test_rows_written_and_table_linked(self, router, collection, raw_artifact_id):
        df = pd.DataFrame({"run_id": [RUN], "cell_label": [1], "area": [10.5]})

        router.persist([_module("analysis", TABLE_SPEC)], {"cell_stats": df}, _meta())

        rows = _products_rows(collection, "SELECT * FROM cell_stats")
        assert len(rows) == 1
        assert rows[0]["scan_id"] == SID
        link = collection.catalog.scan_products(RUN, SID)["cell_stats"]
        assert link["kind"] == "table"
        assert link["table_name"] == "cell_stats"

    def test_run_level_write_makes_no_scan_link(self, router, collection):
        spec = ProductTableWrite(
            key="xs", table="xlma_stat_scan", primary_key=("run_id", "scan_id", "cell_uid")
        )
        df = pd.DataFrame({"run_id": [RUN], "scan_id": [SID], "cell_uid": ["u1"], "n": [3]})

        router.persist(
            [_module("xlma_stat", spec)], {"xs": df}, _meta(scan_id=None, scan_time=None)
        )

        rows = _products_rows(collection, "SELECT * FROM xlma_stat_scan")
        assert len(rows) == 1
        assert collection.catalog.scan_products(RUN, SID) == {}

    def test_empty_frame_skips(self, router, collection, raw_artifact_id):
        router.persist([_module("analysis", TABLE_SPEC)], {"cell_stats": pd.DataFrame()}, _meta())

        assert "cell_stats" not in collection.catalog.scan_products(RUN, SID)


class TestTrackTablesWrite:
    def test_cells_written_to_products_with_aux_link(self, router, collection, raw_artifact_id):
        router.persist([_module("tracking", TRACK_SPEC)], _tracking_result(), _meta())

        rows = _products_rows(collection, "SELECT * FROM cells_by_scan")
        assert len(rows) == 1
        assert rows[0]["scan_id"] == SID
        assert rows[0]["cell_uid"] == "u1"
        link = collection.catalog.scan_products(RUN, SID)["tracking"]
        assert link["kind"] == "table"
        assert not collection.catalog.scan_is_complete(RUN, SID)

    def test_missing_scan_id_raises(self, router):
        with pytest.raises(ValueError, match="scan_id"):
            router.persist(
                [_module("tracking", TRACK_SPEC)], _tracking_result(), _meta(scan_id=None)
            )


class TestCompletenessThroughRouter:
    def test_required_products_complete_the_scan(self, router, collection, raw_artifact_id):
        grid_spec = NetcdfArtifact(
            key="grid_ds", product_type="gridded3d", producer="ingest", description="grid"
        )
        result = {
            "grid_ds": _dataset(),
            "analysis_ds": _dataset(),
            "cell_stats": pd.DataFrame({"run_id": [RUN], "cell_label": [1], "area": [1.0]}),
        }

        router.persist(
            [
                _module("ingest", grid_spec),
                _module("tracking", NC_SPEC),
                _module("analysis", TABLE_SPEC),
            ],
            result,
            _meta(),
        )

        assert collection.catalog.scan_is_complete(RUN, SID)
