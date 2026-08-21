# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""products.db frozen first-frame schemas: freeze, dual snapshots, enforcement."""

import json
import sqlite3
from datetime import UTC, datetime

import pandas as pd
import pytest

from adapt.contracts.persistence import PersistenceMeta, ProductTableWrite
from adapt.persistence.errors import StoreError
from adapt.persistence.products import SchemaLedger, TableWriter
from adapt.persistence.store import Store, init_store

pytestmark = pytest.mark.unit

SCAN_TIME = datetime(2026, 8, 13, 17, 31, 58, tzinfo=UTC)

SPEC = ProductTableWrite(
    key="stats",
    table="cell_stats",
    primary_key=("run_id", "scan_id", "cell_label"),
    index_columns=("cell_label",),
)


def _scan_meta(scan_id: str = "abc123") -> PersistenceMeta:
    return PersistenceMeta(
        scan_time=SCAN_TIME,
        scan_id=scan_id,
        run_id="run-1",
        source_file="KILX20260813_173158_V06",
        collection_id="KILX",
    )


def _run_meta() -> PersistenceMeta:
    return PersistenceMeta(
        scan_time=None, scan_id=None, run_id="run-1", source_file="", collection_id="KILX"
    )


def _frame(**overrides) -> pd.DataFrame:
    data = {"run_id": ["run-1", "run-1"], "cell_label": [1, 2], "area": [10.5, 20.5]}
    data.update(overrides)
    return pd.DataFrame(data)


@pytest.fixture
def collection(tmp_path):
    root = init_store(tmp_path / "store")
    store = Store.open(root)
    yield store.collection("KILX")
    store.close()


@pytest.fixture
def ledger(collection):
    return SchemaLedger(collection.products_path, collection.catalog)


@pytest.fixture
def writer(ledger):
    return TableWriter(ledger, SPEC, owner_module="analysis")


def _rows(collection, sql):
    conn = sqlite3.connect(collection.products_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql).fetchall()]
    finally:
        conn.close()


def _snapshot(db_path, table_name):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM table_schemas WHERE table_name = ?", (table_name,)
        ).fetchone()
    finally:
        conn.close()
    return dict(row) if row else None


class TestFirstFrameFreeze:
    def test_empty_frame_does_not_freeze(self, writer, collection):
        writer.write(pd.DataFrame(), _scan_meta())

        assert _snapshot(collection.products_path, "cell_stats") is None
        assert _rows(collection, "SELECT name FROM sqlite_master WHERE name = 'cell_stats'") == []

    def test_first_frame_creates_table_with_declared_key_and_payload(self, writer, collection):
        writer.write(_frame(), _scan_meta())

        rows = _rows(collection, "SELECT * FROM cell_stats ORDER BY cell_label")
        assert len(rows) == 2
        assert rows[0]["scan_id"] == "abc123"
        assert rows[0]["scan_time"] == "2026-08-13T17:31:58Z"
        assert rows[0]["area"] == pytest.approx(10.5)

        conn = sqlite3.connect(collection.products_path)
        try:
            info = conn.execute("PRAGMA table_info(cell_stats)").fetchall()
            index_cols = {r[1] for r in conn.execute("PRAGMA index_list(cell_stats)")}
        finally:
            conn.close()
        pk_cols = {r[1] for r in info if r[5] > 0}
        assert pk_cols == {"run_id", "scan_id", "cell_label"}
        assert any("cell_label" in name for name in index_cols)

    def test_identical_snapshots_in_products_and_catalog(self, writer, collection):
        writer.write(_frame(), _scan_meta())

        products_snap = _snapshot(collection.products_path, "cell_stats")
        catalog_snap = _snapshot(collection.catalog.db_path, "cell_stats")

        assert products_snap is not None
        assert products_snap == catalog_snap
        assert products_snap["owner_module"] == "analysis"
        assert products_snap["granularity"] == "scan"
        assert json.loads(products_snap["primary_key"]) == ["run_id", "scan_id", "cell_label"]
        frozen_names = {c["name"] for c in json.loads(products_snap["columns_json"])}
        assert {"run_id", "scan_id", "scan_time", "scan_time_unix", "cell_label", "area"} == (
            frozen_names
        )

    def test_reserved_table_names_rejected(self, ledger):
        for reserved in ("table_schemas", "annotations"):
            spec = ProductTableWrite(key="x", table=reserved, primary_key=("run_id",))
            with pytest.raises(StoreError, match=reserved):
                TableWriter(ledger, spec, owner_module="rogue")

    def test_tracking_tables_cannot_be_claimed(self, ledger):
        for core in ("cells_by_scan", "cell_events", "cell_tracks"):
            spec = ProductTableWrite(key="x", table=core, primary_key=("run_id",))
            with pytest.raises(StoreError, match="tracking"):
                TableWriter(ledger, spec, owner_module="rogue")


class TestEnforcement:
    def test_omitted_optional_column_becomes_null(self, writer, collection):
        writer.write(_frame(), _scan_meta())

        writer.write(_frame(area=None).drop(columns=["area"]), _scan_meta("def456"))

        rows = _rows(collection, "SELECT * FROM cell_stats WHERE scan_id = 'def456'")
        assert len(rows) == 2
        assert all(r["area"] is None for r in rows)

    def test_new_column_rejected(self, writer):
        writer.write(_frame(), _scan_meta())

        with pytest.raises(StoreError, match="novel"):
            writer.write(_frame(novel=[1, 2]), _scan_meta("def456"))

    def test_incompatible_dtype_rejected(self, writer):
        writer.write(_frame(), _scan_meta())

        with pytest.raises(StoreError, match="area"):
            writer.write(_frame(area=["high", "low"]), _scan_meta("def456"))

    def test_int_into_real_column_allowed(self, writer, collection):
        writer.write(_frame(), _scan_meta())

        writer.write(_frame(area=[1, 2]), _scan_meta("def456"))

        rows = _rows(collection, "SELECT area FROM cell_stats WHERE scan_id = 'def456'")
        assert sorted(r["area"] for r in rows) == [1, 2]

    def test_nan_promoted_float_into_int_column_allowed(self, writer, collection):
        # A cell with no match promotes an int column to float64 (NaN). The
        # values are still integers; freezing must not make later scans fail.
        writer.write(_frame(pixel_count=[40, 50]), _scan_meta())

        writer.write(_frame(pixel_count=[60.0, float("nan")]), _scan_meta("def456"))

        rows = _rows(collection, "SELECT pixel_count FROM cell_stats WHERE scan_id = 'def456'")
        values = sorted(r["pixel_count"] for r in rows if r["pixel_count"] is not None)
        assert values == [60]
        assert sum(r["pixel_count"] is None for r in rows) == 1

    def test_duplicate_primary_key_within_frame_rejected(self, writer):
        with pytest.raises(StoreError, match="duplicate"):
            writer.write(_frame(cell_label=[7, 7]), _scan_meta())

    def test_upsert_replaces_on_declared_key(self, writer, collection):
        writer.write(_frame(), _scan_meta())

        writer.write(_frame(area=[99.0, 98.0]), _scan_meta())

        rows = _rows(collection, "SELECT * FROM cell_stats ORDER BY cell_label")
        assert len(rows) == 2
        assert rows[0]["area"] == pytest.approx(99.0)

    def test_write_to_another_modules_table_rejected(self, ledger, writer):
        writer.write(_frame(), _scan_meta())
        intruder = TableWriter(ledger, SPEC, owner_module="tracking")

        with pytest.raises(StoreError, match="analysis"):
            intruder.write(_frame(), _scan_meta("def456"))


class TestIdentityStamping:
    def test_run_id_stamped_when_frame_omits_it(self, writer, collection):
        frame = pd.DataFrame({"cell_label": [1], "area": [1.5]})

        writer.write(frame, _scan_meta())

        rows = _rows(collection, "SELECT run_id FROM cell_stats")
        assert rows == [{"run_id": "run-1"}]

    def test_mismatched_run_id_column_raises(self, writer):
        with pytest.raises(StoreError, match="run_id"):
            writer.write(_frame(run_id=["other-run", "other-run"]), _scan_meta())

    def test_scan_granular_frame_may_not_carry_identity_columns(self, writer):
        with pytest.raises(StoreError, match="scan_id"):
            writer.write(_frame(scan_id=["x", "y"]), _scan_meta())

    def test_scan_granular_per_scan_write_requires_scan_time(self, writer):
        meta = PersistenceMeta(
            scan_time=None, scan_id="abc123", run_id="run-1", source_file="", collection_id="KILX"
        )

        with pytest.raises(StoreError, match="scan_time"):
            writer.write(_frame(), meta)

    def test_run_level_write_to_scan_table_requires_scan_id_column(self, ledger):
        spec = ProductTableWrite(
            key="x", table="xlma_stat_scan", primary_key=("run_id", "scan_id", "cell_uid")
        )
        writer = TableWriter(ledger, spec, owner_module="xlma_stat")
        frame = pd.DataFrame({"run_id": ["run-1"], "cell_uid": ["c1"], "flashes": [3]})

        with pytest.raises(StoreError, match="scan_id"):
            writer.write(frame, _run_meta())

        frame["scan_id"] = ["abc123"]
        writer.write(frame, _run_meta())

    def test_time_granular_table_requires_valid_time(self, ledger, collection):
        spec = ProductTableWrite(
            key="x", table="xlma_stat_minutes", primary_key=("run_id", "valid_time", "cell_uid")
        )
        writer = TableWriter(ledger, spec, owner_module="xlma_stat")

        with pytest.raises(StoreError, match="valid_time"):
            writer.write(pd.DataFrame({"run_id": ["run-1"], "cell_uid": ["c1"]}), _run_meta())

        writer.write(
            pd.DataFrame(
                {"run_id": ["run-1"], "cell_uid": ["c1"], "valid_time": ["2026-08-13T17:32:00Z"]}
            ),
            _run_meta(),
        )
        assert _snapshot(collection.products_path, "xlma_stat_minutes")["granularity"] == "time"
