# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Tests for the generic module output writer.

A module declares a SqliteTable spec; the writer creates and upserts into a
per-module SQLite table from a returned DataFrame. Columns are inferred from
the DataFrame; a JSON schema snapshot is registered for API discovery.
"""

import json
import sqlite3
from datetime import UTC, datetime

import pandas as pd
import pytest

pytestmark = pytest.mark.unit

from adapt.contracts import SqliteTable  # noqa: E402
from adapt.persistence.module_output import ModuleOutputWriter  # noqa: E402


def _spec() -> SqliteTable:
    return SqliteTable(
        key="rows",
        table="analysis_probe",
        primary_key=("run_id", "scan_time", "cell_uid"),
        index_columns=("scan_time", "cell_uid"),
    )


def _df(value: float = 1.5) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"run_id": "R1", "scan_time": "2024-01-01T00:00:00", "cell_uid": "a", "value": value},
            {"run_id": "R1", "scan_time": "2024-01-01T00:00:00", "cell_uid": "b", "value": value},
        ]
    )


class TestSqliteTableSpec:
    def test_holds_table_pk_index(self):
        spec = _spec()
        assert spec.table == "analysis_probe"
        assert spec.primary_key == ("run_id", "scan_time", "cell_uid")
        assert spec.index_columns == ("scan_time", "cell_uid")


class TestDatetimeScanTimeSerialization:
    """A datetime scan_time must serialize to the canonical cells_by_scan join key."""

    def _dt_df(self, scan_time) -> pd.DataFrame:
        return pd.DataFrame(
            [{"run_id": "R1", "scan_time": scan_time, "cell_uid": "a", "value": 1.0}]
        )

    def test_naive_datetime_scan_time_writes_canonical_string(self, tmp_path):
        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, _spec()).write(self._dt_df(datetime(2024, 1, 1, 12, 0, 0)))

        conn = sqlite3.connect(str(db))
        try:
            stored = conn.execute("SELECT scan_time FROM analysis_probe").fetchone()[0]
        finally:
            conn.close()
        assert stored == "2024-01-01T12:00:00Z"  # matches track_store._to_iso

    def test_aware_datetime_scan_time_same_string(self, tmp_path):
        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, _spec()).write(
            self._dt_df(datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC))
        )
        conn = sqlite3.connect(str(db))
        try:
            stored = conn.execute("SELECT scan_time FROM analysis_probe").fetchone()[0]
        finally:
            conn.close()
        assert stored == "2024-01-01T12:00:00Z"

    def test_scan_time_unix_auto_added_and_consistent(self, tmp_path):
        """Every table with scan_time also gets a machine-readable scan_time_unix."""
        from adapt.utils.time import to_scan_unix

        db = tmp_path / "catalog.db"
        dt = datetime(2024, 1, 1, 12, 0, 0)
        ModuleOutputWriter(db, _spec()).write(self._dt_df(dt))

        conn = sqlite3.connect(str(db))
        try:
            iso, unix = conn.execute(
                "SELECT scan_time, scan_time_unix FROM analysis_probe LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        assert iso == "2024-01-01T12:00:00Z"
        assert unix == to_scan_unix(dt)  # same instant, machine-readable

    def test_scan_time_unix_not_added_when_no_scan_time(self, tmp_path):
        """Tables without a scan_time column are untouched."""
        db = tmp_path / "catalog.db"
        spec = SqliteTable(key="rows", table="no_time", primary_key=("run_id",))
        ModuleOutputWriter(db, spec).write(pd.DataFrame([{"run_id": "R1", "v": 1.0}]))

        conn = sqlite3.connect(str(db))
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(no_time)")}
        finally:
            conn.close()
        assert "scan_time_unix" not in cols


class TestModuleOutputWriterWrite:
    def test_creates_table_and_writes_rows(self, tmp_path):
        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, _spec()).write(_df(value=2.5))

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute(
                "SELECT run_id, scan_time, cell_uid, value FROM analysis_probe ORDER BY cell_uid"
            ).fetchall()
        finally:
            conn.close()

        assert len(rows) == 2
        assert rows[0] == ("R1", "2024-01-01T00:00:00", "a", 2.5)
        assert rows[1] == ("R1", "2024-01-01T00:00:00", "b", 2.5)

    def test_value_column_is_real_type(self, tmp_path):
        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, _spec()).write(_df())

        conn = sqlite3.connect(str(db))
        try:
            cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(analysis_probe)")}
        finally:
            conn.close()
        assert cols["value"] == "REAL"
        assert cols["run_id"] == "TEXT"

    def test_upsert_on_primary_key_updates_not_duplicates(self, tmp_path):
        db = tmp_path / "catalog.db"
        writer = ModuleOutputWriter(db, _spec())
        writer.write(_df(value=1.0))
        writer.write(_df(value=9.0))  # same PKs, new value

        conn = sqlite3.connect(str(db))
        try:
            rows = conn.execute("SELECT value FROM analysis_probe ORDER BY cell_uid").fetchall()
        finally:
            conn.close()
        assert len(rows) == 2  # not 4
        assert [r[0] for r in rows] == [9.0, 9.0]  # latest value wins

    def test_empty_dataframe_creates_no_table(self, tmp_path):
        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, _spec()).write(pd.DataFrame())

        conn = sqlite3.connect(str(db))
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='analysis_probe'"
            ).fetchall()
        finally:
            conn.close()
        assert tables == []

    def test_none_dataframe_is_noop(self, tmp_path):
        db = tmp_path / "catalog.db"
        # Should not raise
        ModuleOutputWriter(db, _spec()).write(None)


class TestSchemaRegistration:
    def test_schema_snapshot_registered(self, tmp_path):
        db = tmp_path / "catalog.db"
        ModuleOutputWriter(db, _spec()).write(_df())

        conn = sqlite3.connect(str(db))
        try:
            row = conn.execute(
                "SELECT primary_key, index_columns, columns_json "
                "FROM module_schemas WHERE table_name = ?",
                ("analysis_probe",),
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        primary_key, index_columns, columns_json = row
        assert primary_key == "run_id,scan_time,cell_uid"
        assert index_columns == "scan_time,cell_uid"
        cols = {c["name"]: c["type"] for c in json.loads(columns_json)}
        assert cols["value"] == "REAL"
        assert cols["run_id"] == "TEXT"


class TestSchemaEvolution:
    def test_new_column_added_on_later_write(self, tmp_path):
        db = tmp_path / "catalog.db"
        spec = _spec()
        writer = ModuleOutputWriter(db, spec)

        # First write: no 'extra' column
        writer.write(_df(value=1.0))

        # Second write: same rows plus a new 'extra' column
        df2 = _df(value=1.0)
        df2["extra"] = [7.0, 8.0]
        writer.write(df2)

        conn = sqlite3.connect(str(db))
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(analysis_probe)")}
            extra_vals = conn.execute(
                "SELECT extra FROM analysis_probe ORDER BY cell_uid"
            ).fetchall()
        finally:
            conn.close()

        assert "extra" in cols
        assert [r[0] for r in extra_vals] == [7.0, 8.0]
