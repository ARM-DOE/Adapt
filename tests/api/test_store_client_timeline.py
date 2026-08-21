# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreClient scan timeline, live-follow, in-memory rasters, and bundles."""

import pytest

from adapt.api.domain import ScanRaster, ScanRef
from adapt.api.store_client import StoreClient
from adapt.persistence.errors import StoreError
from tests.api.synthetic_store import COLLECTION, RUN_1, build_synthetic_store

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    return build_synthetic_store(tmp_path_factory.mktemp("storetl"))


@pytest.fixture
def client(built):
    c = StoreClient(built.root)
    yield c
    c.close()


class TestTimeline:
    def test_ordered_complete_refs(self, client, built):
        timeline = client.scan_timeline(COLLECTION, RUN_1)

        assert [type(ref) for ref in timeline] == [ScanRef, ScanRef]
        assert [ref.scan_id for ref in timeline] == built.scan_ids[:2]
        assert timeline[0].scan_time < timeline[1].scan_time
        assert all(ref.run_id == RUN_1 for ref in timeline)

    def test_scans_since_watermark(self, client, built):
        timeline = client.scan_timeline(COLLECTION, RUN_1)

        assert client.scans_since(COLLECTION, RUN_1, after=None) == timeline
        assert client.scans_since(COLLECTION, RUN_1, after=timeline[0]) == [timeline[1]]
        assert client.scans_since(COLLECTION, RUN_1, after=timeline[-1]) == []


class TestRaster:
    def test_open_scan_raster_is_fully_in_memory(self, client, built):
        (ref, _) = client.scan_timeline(COLLECTION, RUN_1)[:2]

        with client.open_scan_raster(COLLECTION, RUN_1, ref.scan_id) as raster:
            assert isinstance(raster, ScanRaster)
            assert raster.ref == ref
            assert raster.dataset.attrs["scan_id"] == ref.scan_id
            # Fully loaded: deleting the object file must not break reads.
            row = (
                client._handle(COLLECTION)
                .catalog()
                .execute(
                    "SELECT object_name FROM artifacts WHERE scan_id = ? "
                    "AND artifact_type = 'segmentation2d' AND run_id = ?",
                    (ref.scan_id, RUN_1),
                )
                .fetchone()
            )
            object_path = built.root / "collections" / COLLECTION / "objects" / row["object_name"]
            moved = object_path.with_suffix(".hidden")
            object_path.rename(moved)
            try:
                assert float(raster.dataset["reflectivity"].max()) > 0
            finally:
                moved.rename(object_path)

    def test_gridded3d_product(self, client, built):
        ref = client.scan_timeline(COLLECTION, RUN_1)[0]

        with client.open_scan_raster(COLLECTION, RUN_1, ref.scan_id, product="gridded3d") as r:
            assert "reflectivity" in r.dataset.data_vars
            assert r.product == "gridded3d"

    def test_missing_product_raises(self, client, built):
        pending = built.scan_ids[2]  # acquired, never processed

        with pytest.raises(StoreError, match="segmentation2d"):
            client.open_scan_raster(COLLECTION, RUN_1, pending)


class TestBundle:
    def test_bundle_carries_scan_segmentation_cells_tracks(self, client, built):
        bundle = client.scan_bundle(RUN_1, built.scan_ids[1], COLLECTION)

        assert bundle.scan.scan_id == built.scan_ids[1]
        assert bundle.scan.run_id == RUN_1
        assert bundle.segmentation is not None
        assert bundle.segmentation.attrs["scan_id"] == built.scan_ids[1]
        assert bundle.cells is not None
        assert bundle.cells["cell_uid"].tolist() == [built.uid]
        assert [t.cell_uid for t in bundle.tracks] == [built.uid]
        assert bundle.tracks[0].n_scans == 2
        bundle.segmentation.close()

    def test_incomplete_scan_raises(self, client, built):
        with pytest.raises(StoreError, match="not complete"):
            client.scan_bundle(RUN_1, built.scan_ids[2], COLLECTION)

    def test_unknown_scan_raises(self, client):
        with pytest.raises(StoreError, match="ghost"):
            client.scan_bundle(RUN_1, "ghost", COLLECTION)
