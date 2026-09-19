# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Module-owned product tables with frozen first-frame schemas.

``SchemaLedger`` freezes a table's schema on its first non-empty frame —
declared primary key + payload columns — and writes identical snapshots to
products.db and catalog.db. Frozen types are deterministic: core identity
columns are contracted by name (``_CORE_COLUMN_TYPES``), every other numeric
column freezes as REAL, so the schema depends on the column set — never on
the values in whichever scan arrived first. ``TableWriter`` stamps scan identity from
``PersistenceMeta``, enforces the frozen schema (no new columns, no
incompatible dtypes, no duplicate keys, no cross-module writes), and upserts
on the declared key. There is no ALTER TABLE on the new path — ever.

Granularity derives from the declared primary key: ``valid_time`` in the key
marks a time-granular product (interpolations, forecasts; the row's own time
is part of its identity), otherwise ``scan_id`` marks scan granularity,
otherwise the table is run-granular. Identity columns of scan-granular
per-scan writes are stamped by the writer — modules never pass them.
"""

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.api import types as pdt

from adapt.contracts.persistence import PersistenceMeta, ProductTableWrite
from adapt.persistence.collection_catalog import Catalog
from adapt.persistence.errors import StoreError
from adapt.utils.time import to_scan_iso, to_scan_unix

__all__ = ["SchemaLedger", "TableDeclaration", "TableWriter", "table_granularity"]

_RESERVED_TABLES = frozenset({"table_schemas", "annotations"})
# TrackStore owns these via bespoke DDL; no module may claim them through the
# generic writer even before the first tracking write freezes them.
_TRACKSTORE_TABLES = frozenset({"cells_by_scan", "cell_events", "cell_tracks"})

# Core identity columns carry contracted types, fixed by NAME — never inferred
# from a frame's dtype. Everything else is a source-dependent extra whose type
# follows the deterministic rule in _canonical_sqlite_type, so the frozen
# schema is a pure function of the column set: a NaN in whichever scan happens
# to arrive first must never decide the schema.
_CORE_COLUMN_TYPES: dict[str, str] = {
    "run_id": "TEXT",
    "scan_id": "TEXT",
    "scan_time": "TEXT",
    "scan_time_unix": "INTEGER",  # to_scan_unix returns whole epoch seconds
    "valid_time": "TEXT",
    "cell_uid": "TEXT",
    "cell_label": "INTEGER",
}


def table_granularity(primary_key: Sequence[str]) -> str:
    """Derive a table's granularity from its declared primary key."""
    if "valid_time" in primary_key:
        return "time"
    if "scan_id" in primary_key:
        return "scan"
    return "run"


@dataclass(frozen=True)
class TableDeclaration:
    """One frozen table schema: ownership, keys, and column types."""

    table: str
    owner_module: str
    granularity: str
    primary_key: tuple[str, ...]
    index_columns: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]  # (name, sqlite type) in frozen order


class SchemaLedger:
    """Frozen first-frame schema registry with dual snapshots."""

    def __init__(self, products_path: str | Path, catalog: Catalog) -> None:
        self.products_path = Path(products_path)
        self._catalog = catalog

    def frozen(self, table: str) -> TableDeclaration | None:
        """The frozen declaration for ``table``, or None before first freeze."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM table_schemas WHERE table_name = ?", (table,)
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return TableDeclaration(
            table=row["table_name"],
            owner_module=row["owner_module"],
            granularity=row["granularity"],
            primary_key=tuple(json.loads(row["primary_key"])),
            index_columns=tuple(json.loads(row["index_columns"])),
            columns=tuple((c["name"], c["type"]) for c in json.loads(row["columns_json"])),
        )

    def freeze(self, decl: TableDeclaration) -> None:
        """Create the table + indexes and write identical snapshots to both dbs."""
        if decl.table in _RESERVED_TABLES:
            raise StoreError(f"'{decl.table}' is a reserved table name; modules cannot own it")
        snapshot = _snapshot_row(decl)
        conn = self._connect()
        try:
            col_defs = ", ".join(f"{name} {sqlite_type}" for name, sqlite_type in decl.columns)
            pk = ", ".join(decl.primary_key)
            conn.execute(f"CREATE TABLE {decl.table} ({col_defs}, PRIMARY KEY ({pk}))")
            for col in decl.index_columns:
                conn.execute(f"CREATE INDEX idx_{decl.table}_{col} ON {decl.table}({col})")
            try:
                conn.execute(
                    "INSERT INTO table_schemas (table_name, owner_module, granularity, "
                    " primary_key, index_columns, columns_json, frozen_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    tuple(snapshot.values()),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"Table '{decl.table}' is already frozen") from exc
            conn.commit()
        finally:
            conn.close()
        self._catalog.register_table_schema(snapshot)

    def register_external(self, decl: TableDeclaration) -> None:
        """Snapshot a table created by a bespoke writer (e.g. TrackStore DDL).

        The table must already exist in products.db; only the dual snapshots
        are written here.
        """
        if decl.table in _RESERVED_TABLES:
            raise StoreError(f"'{decl.table}' is a reserved table name; modules cannot own it")
        snapshot = _snapshot_row(decl)
        conn = self._connect()
        try:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (decl.table,)
            ).fetchone()
            if exists is None:
                raise StoreError(
                    f"Cannot register '{decl.table}': the table does not exist in products.db"
                )
            try:
                conn.execute(
                    "INSERT INTO table_schemas (table_name, owner_module, granularity, "
                    " primary_key, index_columns, columns_json, frozen_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    tuple(snapshot.values()),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"Table '{decl.table}' is already frozen") from exc
            conn.commit()
        finally:
            conn.close()
        self._catalog.register_table_schema(snapshot)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.products_path), isolation_level="DEFERRED")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


class TableWriter:
    """Writes one module's product table; freezes on the first non-empty frame."""

    def __init__(self, ledger: SchemaLedger, spec: ProductTableWrite, owner_module: str) -> None:
        if spec.table in _RESERVED_TABLES:
            raise StoreError(f"'{spec.table}' is a reserved table name; modules cannot own it")
        if spec.table in _TRACKSTORE_TABLES and owner_module != "tracking":
            raise StoreError(
                f"'{spec.table}' is a tracking core table; module '{owner_module}' "
                "may not write to it"
            )
        self._ledger = ledger
        self._spec = spec
        self._owner = owner_module
        self._granularity = table_granularity(spec.primary_key)

    def write(self, df: pd.DataFrame, meta: PersistenceMeta) -> None:
        """Stamp identity, enforce the frozen schema, and upsert on the declared key."""
        if df is None or df.empty:
            return
        df = self._stamp(df, meta)

        missing_keys = [c for c in self._spec.primary_key if c not in df.columns]
        if missing_keys:
            raise StoreError(
                f"{self._spec.table}: frame is missing primary-key column(s) "
                f"{', '.join(missing_keys)}"
            )
        if df.duplicated(subset=list(self._spec.primary_key)).any():
            raise StoreError(
                f"{self._spec.table}: duplicate primary key rows within one frame — "
                "refusing to persist (rows would silently overwrite each other)"
            )

        frozen = self._ledger.frozen(self._spec.table)
        if frozen is None:
            frozen = TableDeclaration(
                table=self._spec.table,
                owner_module=self._owner,
                granularity=self._granularity,
                primary_key=tuple(self._spec.primary_key),
                index_columns=tuple(self._spec.index_columns),
                columns=tuple((c, _canonical_sqlite_type(c, df[c])) for c in df.columns),
            )
            self._ledger.freeze(frozen)
        else:
            if frozen.owner_module != self._owner:
                raise StoreError(
                    f"{self._spec.table}: owned by module '{frozen.owner_module}'; "
                    f"'{self._owner}' may not write to it"
                )
            self._validate_frame(df, frozen)
        self._upsert(df)

    # -- identity stamping -------------------------------------------------------

    def _stamp(self, df: pd.DataFrame, meta: PersistenceMeta) -> pd.DataFrame:
        df = self._ensure_run_id(df, meta.run_id)
        if self._granularity == "scan":
            return self._stamp_scan_granular(df, meta)
        if self._granularity == "time" and "valid_time" not in df.columns:
            raise StoreError(
                f"{self._spec.table}: time-granular table requires a valid_time column "
                "(the time each row describes) in the frame"
            )
        return _with_scan_time_unix(df)

    def _ensure_run_id(self, df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Stamp the runtime-owned run identity, or verify a module-carried column."""
        if "run_id" not in df.columns:
            df = df.copy()
            df["run_id"] = run_id
            return df
        foreign = df.loc[df["run_id"] != run_id, "run_id"].unique()
        if len(foreign):
            raise StoreError(
                f"{self._spec.table}: frame carries run_id value(s) "
                f"{', '.join(map(str, foreign))} that do not match the persist's "
                f"run '{run_id}'"
            )
        return df

    def _stamp_scan_granular(self, df: pd.DataFrame, meta: PersistenceMeta) -> pd.DataFrame:
        if meta.scan_id is None:
            # Run-level persist over a scan-keyed table: every row carries its
            # own scan identity (e.g. postprocess re-keying of many scans).
            if "scan_id" not in df.columns:
                raise StoreError(
                    f"{self._spec.table}: primary key includes scan_id but this is a "
                    "run-level persist and the frame carries no scan_id column"
                )
            return _with_scan_time_unix(df)

        carried = [c for c in ("scan_id", "scan_time", "scan_time_unix") if c in df.columns]
        if carried:
            raise StoreError(
                f"{self._spec.table}: frame carries source-owned identity column(s) "
                f"{', '.join(carried)} — the writer stamps them from PersistenceMeta"
            )
        if meta.scan_time is None:
            raise StoreError(
                f"{self._spec.table}: per-scan write is missing scan_time — "
                "wall-clock substitution is forbidden"
            )
        df = df.copy()
        df["scan_id"] = meta.scan_id
        df["scan_time"] = to_scan_iso(meta.scan_time)
        df["scan_time_unix"] = to_scan_unix(meta.scan_time)
        return df

    # -- enforcement + upsert ------------------------------------------------------

    def _validate_frame(self, df: pd.DataFrame, frozen: TableDeclaration) -> None:
        frozen_types = dict(frozen.columns)
        for col in df.columns:
            if col not in frozen_types:
                raise StoreError(
                    f"{self._spec.table}: column '{col}' is not in the frozen schema — "
                    "new columns are rejected after first-frame freeze"
                )
            actual = _sqlite_type(df[col])
            expected = frozen_types[col]
            # INTEGER and REAL interchange losslessly under SQLite affinity.
            # New freezes canonicalize measures to REAL, but pandas promotes
            # int columns to float64 whenever a NaN appears, so int frames must
            # keep writing into REAL columns, NaN-promoted frames into the
            # INTEGER core columns (cell_label), and legacy stores frozen
            # before canonicalization keep accepting both directions.
            if actual != expected and not ({actual, expected} == {"INTEGER", "REAL"}):
                raise StoreError(
                    f"{self._spec.table}: column '{col}' has incompatible dtype "
                    f"{actual}; frozen as {expected}"
                )

    def _upsert(self, df: pd.DataFrame) -> None:
        cols = list(df.columns)
        placeholders = ", ".join("?" for _ in cols)
        pk = set(self._spec.primary_key)
        update_cols = [c for c in cols if c not in pk]
        conflict = ", ".join(self._spec.primary_key)
        if update_cols:
            action = "DO UPDATE SET " + ", ".join(f"{c}=excluded.{c}" for c in update_cols)
        else:
            action = "DO NOTHING"
        sql = (
            f"INSERT INTO {self._spec.table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict}) {action}"
        )
        rows = [tuple(_to_sqlite(v) for v in row) for row in df.itertuples(index=False)]
        conn = self._ledger._connect()
        try:
            conn.executemany(sql, rows)
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        finally:
            conn.close()


def _snapshot_row(decl: TableDeclaration) -> dict:
    return {
        "table_name": decl.table,
        "owner_module": decl.owner_module,
        "granularity": decl.granularity,
        "primary_key": json.dumps(list(decl.primary_key)),
        "index_columns": json.dumps(list(decl.index_columns)),
        "columns_json": json.dumps([{"name": n, "type": t} for n, t in decl.columns]),
        "frozen_at": datetime.now(UTC).isoformat(),
    }


def _with_scan_time_unix(df: pd.DataFrame) -> pd.DataFrame:
    """Every frame with a scan_time column also gets the machine-readable twin."""
    if "scan_time" not in df.columns or "scan_time_unix" in df.columns:
        return df
    df = df.copy()
    df["scan_time_unix"] = df["scan_time"].map(to_scan_unix)
    return df


def _sqlite_type(series: pd.Series) -> str:
    """Map a pandas Series dtype to a SQLite column type (frame validation)."""
    if pdt.is_bool_dtype(series) or pdt.is_integer_dtype(series):
        return "INTEGER"
    if pdt.is_float_dtype(series):
        return "REAL"
    return "TEXT"


def _canonical_sqlite_type(name: str, series: pd.Series) -> str:
    """The FROZEN type of a column: contracted by name for core identity
    columns, deterministic by kind for everything else.

    Numeric measures always freeze as REAL — never as the first frame's luck
    of INTEGER-vs-NaN-promoted-float. Bools stay INTEGER: they never
    NaN-promote (a missing bool becomes object dtype, a genuine data bug that
    must fail validation loudly).
    """
    if name in _CORE_COLUMN_TYPES:
        return _CORE_COLUMN_TYPES[name]
    if pdt.is_bool_dtype(series):
        return "INTEGER"
    if pdt.is_integer_dtype(series) or pdt.is_float_dtype(series):
        return "REAL"
    return "TEXT"


def _to_sqlite(value):
    """Coerce a pandas/numpy scalar to a SQLite-storable Python value.

    Datetimes (python, pandas Timestamp, numpy datetime64) are canonicalized to
    the shared scan-time string so product tables order consistently.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (datetime, np.datetime64)):
        return to_scan_iso(value)
    if pd.api.types.is_bool(value):
        return int(value)
    if hasattr(value, "item"):
        return value.item()
    return value
