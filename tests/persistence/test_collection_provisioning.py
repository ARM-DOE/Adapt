# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Collection provisioning: catalog.db + products.db + objects/, idempotent."""

import sqlite3
from datetime import UTC, datetime

import pandas as pd
import pytest

from adapt.contracts.persistence import PersistenceMeta, ProductTableWrite
from adapt.persistence.products import SchemaLedger, TableWriter
from adapt.persistence.store import Store, StoreError, init_store

pytestmark = pytest.mark.unit


def _tables(db_path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
    finally:
        conn.close()
    return {r[0] for r in rows}


@pytest.fixture
def store(tmp_path):
    root = init_store(tmp_path / "store")
    st = Store.open(root)
    yield st
    st.close()


class TestCollectionProvisioning:
    def test_creates_exact_collection_layout(self, store):
        store.collection("KILX")
        store.close()

        cdir = store.root / "collections" / "KILX"
        assert {p.name for p in cdir.iterdir()} == {
            "catalog.db",
            "products.db",
            "decisions.db",
            "objects",
        }
        assert (cdir / "objects").is_dir()

    def test_catalog_has_discovery_tables_only(self, store):
        store.collection("KILX")
        store.close()

        assert _tables(store.root / "collections" / "KILX" / "catalog.db") == {
            "artifacts",
            "artifact_lineage",
            "scans",
            "scan_products",
            "table_schemas",
        }

    def test_products_has_static_tables_only(self, store):
        store.collection("KILX")
        store.close()

        assert _tables(store.root / "collections" / "KILX" / "products.db") == {
            "table_schemas",
            "annotations",
        }

    def test_decisions_has_the_decision_tables_only(self, store):
        store.collection("KILX")
        store.close()

        assert _tables(store.root / "collections" / "KILX" / "decisions.db") == {
            "tracking_scans",
            "tracking_cells",
            "tracking_pairs",
            "tracking_lineage",
            "tracking_identity",
            "tracking_latent",
            "segmentation_frames",
            "segmentation_seeds",
            "segmentation_components",
        }

    def test_second_open_reuses_layout(self, store):
        first = store.collection("KILX")
        second = store.collection("KILX")

        assert second is first
        store.close()
        cdir = store.root / "collections" / "KILX"
        assert {p.name for p in cdir.iterdir()} == {
            "catalog.db",
            "products.db",
            "decisions.db",
            "objects",
        }

    def test_collection_exposes_paths_and_catalog(self, store):
        coll = store.collection("KILX")

        assert coll.collection_id == "KILX"
        assert coll.objects_dir == store.root / "collections" / "KILX" / "objects"
        assert coll.products_path == store.root / "collections" / "KILX" / "products.db"
        assert coll.decisions_path == store.root / "collections" / "KILX" / "decisions.db"
        assert coll.catalog is not None


def _freeze_one_table(store) -> None:
    coll = store.collection("KILX")
    writer = TableWriter(
        SchemaLedger(coll.products_path, coll.catalog),
        ProductTableWrite(key="s", table="cell_stats", primary_key=("run_id", "cell_label")),
        owner_module="analysis",
    )
    meta = PersistenceMeta(
        scan_time=datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC),
        scan_id=None,
        run_id="run-1",
        source_file="",
        collection_id="KILX",
    )
    writer.write(pd.DataFrame({"run_id": ["run-1"], "cell_label": [1], "area": [1.5]}), meta)


class TestValidateCollection:
    def test_agreeing_snapshots_validate(self, store):
        _freeze_one_table(store)

        store.validate_collection("KILX")
        store.close()
        reopened = Store.open(store.root)
        reopened.collection("KILX")
        reopened.close()

    def test_tampered_snapshot_raises_naming_table_and_fingerprints(self, store):
        _freeze_one_table(store)
        store.close()
        products = store.root / "collections" / "KILX" / "products.db"
        conn = sqlite3.connect(products)
        conn.execute("UPDATE table_schemas SET columns_json = '[]'")
        conn.commit()
        conn.close()

        reopened = Store.open(store.root)
        try:
            with pytest.raises(StoreError, match="cell_stats") as exc:
                reopened.collection("KILX")
        finally:
            reopened.close()
        message = str(exc.value)
        assert "catalog" in message and "products" in message

    def test_snapshot_missing_on_one_side_raises(self, store):
        _freeze_one_table(store)
        store.close()
        catalog_db = store.root / "collections" / "KILX" / "catalog.db"
        conn = sqlite3.connect(catalog_db)
        conn.execute("DELETE FROM table_schemas")
        conn.commit()
        conn.close()

        reopened = Store.open(store.root)
        try:
            with pytest.raises(StoreError, match="cell_stats"):
                reopened.validate_collection("KILX")
        finally:
            reopened.close()
