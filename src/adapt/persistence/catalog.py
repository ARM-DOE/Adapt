# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Radar-level catalog manager for Adapt repository.

Manages catalog.db at {root_dir}/{radar}/catalog.db.
Tracks all data items, processing progress, and schemas for a specific radar.

The RadarCatalog is specific to one radar and provides:
- Item registration and querying
- Progress tracking
- Schema definitions for Parquet tables
- Lineage tracking via parent_ids

Thread-safe for concurrent writer/reader access via SQLite WAL mode.
"""

import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from adapt.contracts.persistence import ScanRecord
from adapt.persistence.sqlite_store import SqliteStore
from adapt.utils.time import to_scan_iso

__all__ = ["RadarCatalog"]

logger = logging.getLogger(__name__)


class RadarCatalog(SqliteStore):
    """Radar-level catalog manager.

    Manages catalog.db at {radar_dir}/catalog.db.
    Tracks all items, progress, and schemas for one radar.

    Thread-safe via SQLite WAL mode and internal locking.

    Examples
    --------
    >>> catalog = RadarCatalog("/data/radar_output/KHTX")
    >>> catalog.register_item(
    ...     item_id="abc123",
    ...     run_id="run001",
    ...     item_type="analysis2d",
    ...     scan_time="2026-02-15T12:00:00Z",
    ...     file_path="analysis/20260215/cells.parquet"
    ... )
    >>> items = catalog.query_items(item_type="analysis2d", limit=10)
    """

    def __init__(self, radar_dir: str | Path):
        """Initialize radar catalog.

        Parameters
        ----------
        radar_dir : str or Path
            Radar data directory (e.g., /data/KHTX)
        """
        self.radar_dir = Path(radar_dir).resolve()
        self.radar = self.radar_dir.name
        # BEFORE the schema script runs: the script only CREATEs missing
        # tables/indexes — against a pre-identity catalog it would trip an
        # opaque OperationalError (index on the missing scan_id column) before
        # any friendly guidance could fire.
        self._assert_scan_identity_schema(self.radar_dir / "catalog.db")
        super().__init__(self.radar_dir / "catalog.db", "radar_catalog_schema.sql", checkpoint=True)
        logger.info(f"RadarCatalog initialized for {self.radar} at {self.db_path}")

    @staticmethod
    def _assert_scan_identity_schema(db_path: Path) -> None:
        """Fail fast on pre-identity catalogs, before touching their schema.

        This codebase does not migrate old catalogs: recreate them instead.
        A missing file (fresh repository) is fine — the schema script creates
        everything identity-shaped from scratch.
        """
        if not db_path.exists():
            return
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            tables = {
                r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if "items" not in tables:
                return  # empty/new database file
            items_cols = {r[1] for r in conn.execute("PRAGMA table_info(items)").fetchall()}
            scans_cols = {r[1] for r in conn.execute("PRAGMA table_info(scans)").fetchall()}
        finally:
            conn.close()
        if "scan_id" not in items_cols or (
            tables >= {"scans"} and "source_file_name" not in scans_cols
        ):
            raise RuntimeError(
                f"{db_path} predates scan identity (items.scan_id / "
                "scans.source_file_name missing). "
                "Recreate catalog.db (delete it and rerun the pipeline)."
            )

    # =========================================================================
    # Item Management
    # =========================================================================

    def register_item(
        self,
        item_id: str,
        run_id: str,
        item_type: str,
        scan_time: str | None,
        file_path: str,
        scan_id: str | None = None,
        processing_stage: str = "complete",
        status: str = "complete",
        parent_ids: list[str] | None = None,
        metadata: dict | None = None,
        file_size_bytes: int | None = None,
        file_hash: str | None = None,
    ) -> None:
        """Register a data item in the catalog.

        Parameters
        ----------
        item_id : str
            Unique item identifier
        run_id : str
            Run that produced this item
        item_type : str
            Type of item (e.g., 'analysis2d', 'gridded3d')
        scan_time : str
            ISO8601 scan timestamp
        file_path : str
            Relative path from radar directory
        processing_stage : str
            Stage: acquisition, gridding, segmentation, analysis
        status : str
            Status: complete, failed, processing
        parent_ids : list of str, optional
            Parent item IDs for lineage
        metadata : dict, optional
            Additional metadata
        file_size_bytes : int, optional
            File size
        file_hash : str, optional
            File hash (SHA256)
        """
        now = datetime.now(UTC).isoformat()
        parent_ids_json = json.dumps(parent_ids) if parent_ids else None
        metadata_json = json.dumps(metadata) if metadata else None

        conn = self._get_connection()
        with self._lock:
            conn.execute(
                """
                INSERT OR REPLACE INTO items
                (item_id, run_id, item_type, scan_id, scan_time, file_path, parent_ids,
                 processing_stage, status, metadata, file_size_bytes, file_hash,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    item_id,
                    run_id,
                    item_type,
                    scan_id,
                    scan_time,
                    file_path,
                    parent_ids_json,
                    processing_stage,
                    status,
                    metadata_json,
                    file_size_bytes,
                    file_hash,
                    now,
                    now,
                ),
            )
            conn.commit()

        logger.debug(f"Item registered: {item_id} ({item_type})")

    def update_item_status(
        self, item_id: str, status: str, error_message: str | None = None
    ) -> None:
        """Update item status.

        Parameters
        ----------
        item_id : str
            Item identifier
        status : str
            New status
        error_message : str, optional
            Error message if status=failed
        """
        now = datetime.now(UTC).isoformat()

        conn = self._get_connection()
        with self._lock:
            conn.execute(
                """
                UPDATE items
                SET status = ?, error_message = ?, updated_at = ?
                WHERE item_id = ?
            """,
                (status, error_message, now, item_id),
            )
            conn.commit()

    def query_items(
        self,
        item_type: str | None = None,
        run_id: str | None = None,
        status: str | None = None,
        limit: int | None = None,
        order_by: str = "scan_time DESC",
    ) -> pd.DataFrame:
        """Query items with optional filters.

        Parameters
        ----------
        item_type : str, optional
            Filter by item type
        run_id : str, optional
            Filter by run ID
        status : str, optional
            Filter by status
        limit : int, optional
            Maximum results
        order_by : str
            Sort order (default: newest first)

        Returns
        -------
        DataFrame
            Matching items
        """
        query = "SELECT * FROM items WHERE 1=1"
        params = []

        if item_type:
            query += " AND item_type = ?"
            params.append(item_type)
        if run_id:
            query += " AND run_id = ?"
            params.append(run_id)
        if status:
            query += " AND status = ?"
            params.append(status)

        query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        conn = self._get_connection()
        with self._lock:
            return pd.read_sql_query(query, conn, params=params)

    def get_latest_item(self, item_type: str, run_id: str | None = None) -> dict | None:
        """Get the most recent item of a type.

        Parameters
        ----------
        item_type : str
            Item type to query
        run_id : str, optional
            Filter by run ID

        Returns
        -------
        dict or None
            Item metadata dictionary
        """
        conn = self._get_connection()
        with self._lock:
            if run_id:
                row = conn.execute(
                    """
                    SELECT * FROM items
                    WHERE item_type = ? AND run_id = ? AND status = 'complete'
                    ORDER BY scan_time DESC
                    LIMIT 1
                """,
                    (item_type, run_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM items
                    WHERE item_type = ? AND status = 'complete'
                    ORDER BY scan_time DESC
                    LIMIT 1
                """,
                    (item_type,),
                ).fetchone()

        return dict(row) if row else None

    def get_item(self, item_id: str) -> dict | None:
        """Get a single item record by ID. Returns None if not found."""
        conn = self._get_connection()
        with self._lock:
            row = conn.execute("SELECT * FROM items WHERE item_id = ?", (item_id,)).fetchone()
        return dict(row) if row else None

    # =========================================================================
    # Progress Tracking
    # =========================================================================

    def update_progress(self, run_id: str, **kwargs) -> None:
        """Update processing progress for a run.

        Parameters
        ----------
        run_id : str
            Run identifier
        **kwargs
            Progress fields to update (latest_downloaded_time, etc.)
        """
        now = datetime.now(UTC).isoformat()

        # Build update query dynamically
        fields = list(kwargs.keys())
        if not fields:
            return

        set_clause = ", ".join(f"{field} = ?" for field in fields)
        values = list(kwargs.values()) + [now, run_id]

        conn = self._get_connection()
        with self._lock:
            # Try update first
            cursor = conn.execute(
                f"""
                UPDATE progress
                SET {set_clause}, last_updated = ?
                WHERE run_id = ?
            """,
                values,
            )

            # If no rows updated, insert
            if cursor.rowcount == 0:
                conn.execute(
                    """
                    INSERT INTO progress (run_id, last_updated)
                    VALUES (?, ?)
                """,
                    (run_id, now),
                )
                # Retry update
                conn.execute(
                    f"""
                    UPDATE progress
                    SET {set_clause}, last_updated = ?
                    WHERE run_id = ?
                """,
                    values,
                )

            conn.commit()

    def get_progress(self, run_id: str) -> dict | None:
        """Get progress status for a run.

        Parameters
        ----------
        run_id : str
            Run identifier

        Returns
        -------
        dict or None
            Progress metadata
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute("SELECT * FROM progress WHERE run_id = ?", (run_id,)).fetchone()

        return dict(row) if row else None

    # =========================================================================
    # Schema Management
    # =========================================================================

    def register_schema(
        self, item_type: str, columns: list[dict[str, str]], schema_version: int = 1
    ) -> None:
        """Register or update schema for an item type.

        Parameters
        ----------
        item_type : str
            Item type name
        columns : list of dict
            Column definitions: [{"name": "refl", "dtype": "float32"}, ...]
        schema_version : int
            Schema version number
        """
        now = datetime.now(UTC).isoformat()
        columns_json = json.dumps(columns)

        conn = self._get_connection()
        with self._lock:
            conn.execute(
                """
                INSERT OR REPLACE INTO schemas
                (item_type, columns_json, schema_version, updated_at)
                VALUES (?, ?, ?, ?)
            """,
                (item_type, columns_json, schema_version, now),
            )
            conn.commit()

        logger.debug(f"Schema registered for {item_type} (v{schema_version})")

    def get_schema(self, item_type: str) -> list[dict] | None:
        """Get schema for an item type.

        Parameters
        ----------
        item_type : str
            Item type name

        Returns
        -------
        list of dict or None
            Column definitions
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT columns_json FROM schemas WHERE item_type = ?", (item_type,)
            ).fetchone()

        if row:
            return json.loads(row["columns_json"])
        return None

    # =========================================================================
    # Scan Management
    # =========================================================================

    def register_scan(self, record: ScanRecord) -> None:
        """Upsert one scan keyed by (run_id, scan_id); the processor is the single writer.

        ``scan_time`` is stored in the canonical format (``to_scan_iso``);
        coverage times are optional per-source metadata kept at full precision.
        """
        scan_iso = to_scan_iso(record.scan_time)
        scan_date = record.scan_time.strftime("%Y%m%d")
        now = datetime.now(UTC).isoformat()

        conn = self._get_connection()
        with self._lock:
            conn.execute(
                """
                INSERT INTO scans
                (run_id, scan_id, scan_time, scan_date, start_time, end_time,
                 source_file_name, processing_status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'complete', ?, ?)
                ON CONFLICT(run_id, scan_id) DO UPDATE SET
                    scan_time=excluded.scan_time,
                    scan_date=excluded.scan_date,
                    start_time=excluded.start_time,
                    end_time=excluded.end_time,
                    source_file_name=excluded.source_file_name,
                    processing_status=excluded.processing_status,
                    updated_at=excluded.updated_at
            """,
                (
                    record.run_id,
                    record.scan_id,
                    scan_iso,
                    scan_date,
                    record.start_time.isoformat() if record.start_time else None,
                    record.end_time.isoformat() if record.end_time else None,
                    record.source_file_name,
                    now,
                    now,
                ),
            )
            conn.commit()

        logger.debug(f"Scan registered: {record.scan_id} at {scan_iso}")

    def get_scan(self, run_id: str, scan_id: str) -> dict | None:
        """Scan record by identity, or None when unknown."""
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM scans WHERE run_id = ? AND scan_id = ?", (run_id, scan_id)
            ).fetchone()

        return dict(row) if row else None

    def list_scans(
        self,
        run_id: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        """Scan records ordered by scan_time ascending (time orders; scan_id identifies)."""
        query = "SELECT * FROM scans WHERE 1=1"
        params: list[Any] = []

        if run_id:
            query += " AND run_id = ?"
            params.append(run_id)
        if start_time:
            query += " AND scan_time >= ?"
            params.append(to_scan_iso(start_time))
        if end_time:
            query += " AND scan_time <= ?"
            params.append(to_scan_iso(end_time))

        query += f" ORDER BY scan_time ASC LIMIT {int(limit)}"

        conn = self._get_connection()
        with self._lock:
            return pd.read_sql_query(query, conn, params=params)
