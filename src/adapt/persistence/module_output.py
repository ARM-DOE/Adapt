# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Generic per-module output table writer.

A module declares a ``SqliteTable`` spec (table name, primary key, index
columns — see ``adapt.contracts.persistence``). The ``OutputRouter`` passes the
module's returned DataFrame to a ``ModuleOutputWriter``, which creates the
SQLite table, infers columns from the DataFrame, upserts on the primary key,
and registers a JSON schema snapshot in ``module_schemas`` for API discovery.

No module-specific code lives here — the writer is fully driven by the spec and
the DataFrame. Mirrors the SQLite patterns used by TrackStore.
"""

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.api import types as pdt

from adapt.contracts import SqliteTable
from adapt.persistence.tables import is_core_table
from adapt.utils.time import to_scan_iso, to_scan_unix


class ModuleOutputWriter:
    """Creates and writes a module's output table from a DataFrame.

    Columns and types are inferred from the DataFrame. Rows are upserted on the
    spec's primary key. A JSON schema snapshot is registered in ``module_schemas``
    for API discovery. No module-specific logic lives here.
    """

    def __init__(self, db_path: str | Path, spec: SqliteTable) -> None:
        if is_core_table(spec.table):
            raise ValueError(
                f"'{spec.table}' is a core table; module writers may only create "
                "extension tables, never mutate core pipeline tables."
            )
        self._db_path = Path(db_path)
        self._spec = spec

    def write(self, df: pd.DataFrame) -> None:
        """Create the table if needed and upsert the DataFrame's rows."""
        if df is None or df.empty:
            return
        df = self._add_scan_time_unix(df)
        conn = self._connect()
        try:
            self._ensure_table(conn, df)
            self._ensure_columns(conn, df)
            self._upsert(conn, df)
            self._register_schema(conn, df)
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        finally:
            conn.close()

    @staticmethod
    def _add_scan_time_unix(df: pd.DataFrame) -> pd.DataFrame:
        """Unify time across all derived tables: every table with a ``scan_time``
        column also gets a machine-readable ``scan_time_unix`` (epoch seconds) for
        the identical instant. Module authors never add it — both representations
        always come from the same value via the single time source.
        """
        if "scan_time" not in df.columns or "scan_time_unix" in df.columns:
            return df
        df = df.copy()
        df["scan_time_unix"] = df["scan_time"].map(to_scan_unix)
        return df

    # ── SQLite helpers ────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self._db_path), check_same_thread=False, isolation_level="DEFERRED"
        )
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_table(self, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
        col_defs = ", ".join(f"{name} {_sqlite_type(df[name])}" for name in df.columns)
        pk = ", ".join(self._spec.primary_key)
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._spec.table} ({col_defs}, PRIMARY KEY ({pk}))"
        )

    def _ensure_columns(self, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
        existing = {r[1] for r in conn.execute(f"PRAGMA table_info({self._spec.table})")}
        for col in df.columns:
            if col not in existing:
                conn.execute(
                    f"ALTER TABLE {self._spec.table} ADD COLUMN {col} {_sqlite_type(df[col])}"
                )

    def _upsert(self, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
        cols = list(df.columns)
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(cols)
        pk = set(self._spec.primary_key)
        update_set = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in pk)
        conflict = ", ".join(self._spec.primary_key)
        sql = (
            f"INSERT INTO {self._spec.table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict}) DO UPDATE SET {update_set}"
        )
        rows = [tuple(_to_sqlite(v) for v in row) for row in df.itertuples(index=False)]
        conn.executemany(sql, rows)

    def _register_schema(self, conn: sqlite3.Connection, df: pd.DataFrame) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS module_schemas ("
            "table_name TEXT PRIMARY KEY, primary_key TEXT, index_columns TEXT, "
            "columns_json TEXT, updated_at TEXT)"
        )
        columns = [{"name": c, "type": _sqlite_type(df[c])} for c in df.columns]
        conn.execute(
            "INSERT OR REPLACE INTO module_schemas "
            "(table_name, primary_key, index_columns, columns_json, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                self._spec.table,
                ",".join(self._spec.primary_key),
                ",".join(self._spec.index_columns),
                json.dumps(columns),
                datetime.now(UTC).isoformat(),
            ),
        )


def _sqlite_type(series: pd.Series) -> str:
    """Map a pandas Series dtype to a SQLite column type."""
    if pdt.is_bool_dtype(series) or pdt.is_integer_dtype(series):
        return "INTEGER"
    if pdt.is_float_dtype(series):
        return "REAL"
    return "TEXT"


def _to_sqlite(value):
    """Coerce a pandas/numpy scalar to a SQLite-storable Python value.

    Datetimes (python, pandas Timestamp, numpy datetime64) are canonicalized to
    the shared scan-time join key so derived module tables join to cells_by_scan.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    # datetime-like before .item(): python datetime + pandas Timestamp (a subclass)
    if isinstance(value, (datetime, np.datetime64)):
        return to_scan_iso(value)
    if pd.api.types.is_bool(value):
        return int(value)
    if hasattr(value, "item"):
        return value.item()
    return value
