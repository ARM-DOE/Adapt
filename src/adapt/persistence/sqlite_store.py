# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Shared SQLite store base for Adapt persistence.

Owns the thread-safe WAL connection and schema bootstrap from the canonical SQL
files in ``adapt/configuration/schemas/``. Subclasses (``RadarCatalog``,
``RepositoryRegistry``) supply their database path and schema filename and add
their table-specific methods.

A missing schema file raises immediately — there is no inline fallback.
"""

import sqlite3
import threading
from pathlib import Path

__all__ = ["SqliteStore"]

_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "configuration" / "schemas"


class SqliteStore:
    """Thread-safe SQLite connection and schema initialisation.

    Provides the shared WAL connection, locking, and schema bootstrap. The
    database path and schema file are chosen by the subclass.
    """

    def __init__(
        self, db_path: str | Path, schema_filename: str, *, checkpoint: bool = False
    ) -> None:
        self.db_path = Path(db_path)
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._init_schema(schema_filename, checkpoint=checkpoint)

    def _get_connection(self) -> sqlite3.Connection:
        """Return the shared thread-safe connection, opening it on first use."""
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path), check_same_thread=False, isolation_level="DEFERRED"
            )
            self._conn.row_factory = sqlite3.Row
            # Enable WAL mode for concurrent access
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _init_schema(self, schema_filename: str, *, checkpoint: bool) -> None:
        """Create tables from the canonical SQL file. Raise if it is missing."""
        schema_path = _SCHEMA_DIR / schema_filename
        if not schema_path.exists():
            raise FileNotFoundError(f"SQL schema not found: {schema_path}")

        schema_sql = schema_path.read_text(encoding="utf-8")
        conn = self._get_connection()
        with self._lock:
            conn.executescript(schema_sql)
            conn.commit()
            if checkpoint:
                # Checkpoint WAL so readonly readers (immutable=1) see the schema.
                conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    def close(self) -> None:
        """Close the connection if open."""
        if self._conn:
            with self._lock:
                self._conn.close()
                self._conn = None
