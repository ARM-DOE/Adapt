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
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

__all__ = ['RadarCatalog']

logger = logging.getLogger(__name__)


class RadarCatalog:
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
    
    def __init__(self, radar_dir: Union[str, Path]):
        """Initialize radar catalog.
        
        Parameters
        ----------
        radar_dir : str or Path
            Radar data directory (e.g., /data/KHTX)
        """
        self.radar_dir = Path(radar_dir).resolve()
        self.radar = self.radar_dir.name
        self.db_path = self.radar_dir / "catalog.db"
        
        # Thread safety
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        
        # Initialize database
        self._init_database()
        
        logger.info(f"RadarCatalog initialized for {self.radar} at {self.db_path}")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-safe database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                isolation_level='DEFERRED'
            )
            self._conn.row_factory = sqlite3.Row
            # Enable WAL mode for concurrent access
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn
    
    def _init_database(self) -> None:
        """Initialize database schema from SQL file."""
        schema_path = Path(__file__).parent / "schemas" / "radar_catalog_schema.sql"
        
        if not schema_path.exists():
            # Fallback to embedded schema
            self._create_schema_inline()
            return
        
        with open(schema_path) as f:
            schema_sql = f.read()
        
        conn = self._get_connection()
        with self._lock:
            conn.executescript(schema_sql)
            conn.commit()
        
        logger.debug(f"Radar catalog schema initialized from {schema_path}")
    
    def _create_schema_inline(self) -> None:
        """Create schema inline (fallback)."""
        conn = self._get_connection()
        with self._lock:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            
            # Items table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    item_type TEXT NOT NULL,
                    scan_time TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    parent_ids TEXT,
                    processing_stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    metadata TEXT,
                    file_size_bytes INTEGER,
                    file_hash TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_run ON items(run_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_type ON items(item_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_scan_time ON items(scan_time DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_type_time ON items(item_type, scan_time DESC)")
            
            # Progress table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS progress (
                    run_id TEXT PRIMARY KEY,
                    latest_downloaded_time TEXT,
                    latest_gridded_time TEXT,
                    latest_segmented_time TEXT,
                    latest_analyzed_time TEXT,
                    num_items_complete INTEGER DEFAULT 0,
                    num_items_failed INTEGER DEFAULT 0,
                    queue_depth INTEGER DEFAULT 0,
                    last_updated TEXT NOT NULL
                )
            """)
            
            # Schemas table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schemas (
                    item_type TEXT PRIMARY KEY,
                    columns_json TEXT NOT NULL,
                    schema_version INTEGER DEFAULT 1,
                    updated_at TEXT NOT NULL
                )
            """)
            
            conn.commit()
    
    # =========================================================================
    # Item Management
    # =========================================================================
    
    def register_item(
        self,
        item_id: str,
        run_id: str,
        item_type: str,
        scan_time: str,
        file_path: str,
        processing_stage: str = "complete",
        status: str = "complete",
        parent_ids: Optional[List[str]] = None,
        metadata: Optional[Dict] = None,
        file_size_bytes: Optional[int] = None,
        file_hash: Optional[str] = None
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
        now = datetime.now(timezone.utc).isoformat()
        parent_ids_json = json.dumps(parent_ids) if parent_ids else None
        metadata_json = json.dumps(metadata) if metadata else None
        
        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                INSERT OR REPLACE INTO items
                (item_id, run_id, item_type, scan_time, file_path, parent_ids,
                 processing_stage, status, metadata, file_size_bytes, file_hash,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (item_id, run_id, item_type, scan_time, file_path, parent_ids_json,
                  processing_stage, status, metadata_json, file_size_bytes, file_hash,
                  now, now))
            conn.commit()
        
        logger.debug(f"Item registered: {item_id} ({item_type})")
    
    def update_item_status(
        self,
        item_id: str,
        status: str,
        error_message: Optional[str] = None
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
        now = datetime.now(timezone.utc).isoformat()
        
        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                UPDATE items 
                SET status = ?, error_message = ?, updated_at = ?
                WHERE item_id = ?
            """, (status, error_message, now, item_id))
            conn.commit()
    
    def query_items(
        self,
        item_type: Optional[str] = None,
        run_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: str = "scan_time DESC"
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
    
    def get_latest_item(
        self,
        item_type: str,
        run_id: Optional[str] = None
    ) -> Optional[Dict]:
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
                row = conn.execute("""
                    SELECT * FROM items 
                    WHERE item_type = ? AND run_id = ? AND status = 'complete'
                    ORDER BY scan_time DESC 
                    LIMIT 1
                """, (item_type, run_id)).fetchone()
            else:
                row = conn.execute("""
                    SELECT * FROM items 
                    WHERE item_type = ? AND status = 'complete'
                    ORDER BY scan_time DESC 
                    LIMIT 1
                """, (item_type,)).fetchone()
        
        return dict(row) if row else None
    
    # =========================================================================
    # Progress Tracking
    # =========================================================================
    
    def update_progress(
        self,
        run_id: str,
        **kwargs
    ) -> None:
        """Update processing progress for a run.
        
        Parameters
        ----------
        run_id : str
            Run identifier
        **kwargs
            Progress fields to update (latest_downloaded_time, etc.)
        """
        now = datetime.now(timezone.utc).isoformat()
        
        # Build update query dynamically
        fields = list(kwargs.keys())
        if not fields:
            return
        
        set_clause = ", ".join(f"{field} = ?" for field in fields)
        values = list(kwargs.values()) + [now, run_id]
        
        conn = self._get_connection()
        with self._lock:
            # Try update first
            cursor = conn.execute(f"""
                UPDATE progress 
                SET {set_clause}, last_updated = ?
                WHERE run_id = ?
            """, values)
            
            # If no rows updated, insert
            if cursor.rowcount == 0:
                conn.execute("""
                    INSERT INTO progress (run_id, last_updated)
                    VALUES (?, ?)
                """, (run_id, now))
                # Retry update
                conn.execute(f"""
                    UPDATE progress 
                    SET {set_clause}, last_updated = ?
                    WHERE run_id = ?
                """, values)
            
            conn.commit()
    
    def get_progress(self, run_id: str) -> Optional[Dict]:
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
            row = conn.execute(
                "SELECT * FROM progress WHERE run_id = ?",
                (run_id,)
            ).fetchone()
        
        return dict(row) if row else None
    
    # =========================================================================
    # Schema Management
    # =========================================================================
    
    def register_schema(
        self,
        item_type: str,
        columns: List[Dict[str, str]],
        schema_version: int = 1
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
        now = datetime.now(timezone.utc).isoformat()
        columns_json = json.dumps(columns)
        
        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                INSERT OR REPLACE INTO schemas
                (item_type, columns_json, schema_version, updated_at)
                VALUES (?, ?, ?, ?)
            """, (item_type, columns_json, schema_version, now))
            conn.commit()
        
        logger.debug(f"Schema registered for {item_type} (v{schema_version})")
    
    def get_schema(self, item_type: str) -> Optional[List[Dict]]:
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
                "SELECT columns_json FROM schemas WHERE item_type = ?",
                (item_type,)
            ).fetchone()
        
        if row:
            return json.loads(row['columns_json'])
        return None
    
    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            with self._lock:
                self._conn.close()
                self._conn = None
        logger.debug(f"Radar catalog connection closed for {self.radar}")
