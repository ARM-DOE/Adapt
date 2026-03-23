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

    # =========================================================================
    # Scan Management
    # =========================================================================

    def register_scan(
        self,
        scan_time: datetime,
        run_id: str,
        nexrad_file_name: Optional[str] = None
    ) -> str:
        """Register a new scan. Idempotent on scan_time+run_id.

        Parameters
        ----------
        scan_time : datetime
            Scan timestamp (UTC)
        run_id : str
            Run identifier
        nexrad_file_name : str, optional
            Original NEXRAD filename

        Returns
        -------
        str
            Scan ID
        """
        import uuid

        scan_time_str = scan_time.isoformat()
        scan_date = scan_time.strftime('%Y%m%d')
        now = datetime.now(timezone.utc).isoformat()

        conn = self._get_connection()
        with self._lock:
            # Check if scan already exists
            row = conn.execute("""
                SELECT scan_id FROM scans
                WHERE scan_time = ? AND run_id = ?
            """, (scan_time_str, run_id)).fetchone()

            if row:
                return row['scan_id']

            # Create new scan
            scan_id = str(uuid.uuid4())[:16]
            conn.execute("""
                INSERT INTO scans
                (scan_id, scan_time, scan_date, run_id, nexrad_file_name,
                 processing_status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
            """, (scan_id, scan_time_str, scan_date, run_id, nexrad_file_name, now, now))
            conn.commit()

        logger.debug(f"Scan registered: {scan_id} at {scan_time_str}")
        return scan_id

    def link_item_to_scan(
        self,
        scan_time: datetime,
        item_type: str,
        item_id: str,
        num_cells: Optional[int] = None,
        max_reflectivity: Optional[float] = None,
        has_tracks: Optional[bool] = None
    ) -> None:
        """Link an item to its parent scan.

        Parameters
        ----------
        scan_time : datetime
            Scan timestamp
        item_type : str
            Item type (gridded3d, segmentation2d, projection2d, analysis2d)
        item_id : str
            Item identifier
        num_cells : int, optional
            Number of cells detected
        max_reflectivity : float, optional
            Maximum reflectivity
        has_tracks : bool, optional
            Whether tracks exist for this scan
        """
        scan_time_str = scan_time.isoformat()
        now = datetime.now(timezone.utc).isoformat()

        # Map item_type to column name
        column_map = {
            'gridded3d': 'gridded3d_item_id',
            'segmentation2d': 'segmentation2d_item_id',
            'projection2d': 'projection2d_item_id',
            'analysis2d': 'analysis2d_item_id',
        }

        column = column_map.get(item_type)
        if not column:
            logger.warning(f"Unknown item_type for scan link: {item_type}")
            return

        conn = self._get_connection()
        with self._lock:
            # Build update query
            updates = [f"{column} = ?"]
            params = [item_id]

            if num_cells is not None:
                updates.append("num_cells = ?")
                params.append(num_cells)
            if max_reflectivity is not None:
                updates.append("max_reflectivity = ?")
                params.append(max_reflectivity)
            if has_tracks is not None:
                updates.append("has_tracks = ?")
                params.append(has_tracks)

            updates.append("updated_at = ?")
            params.append(now)
            params.append(scan_time_str)

            # Check if all items are now linked
            conn.execute(f"""
                UPDATE scans
                SET {', '.join(updates)}
                WHERE scan_time = ?
            """, params)

            # Update processing status
            conn.execute("""
                UPDATE scans
                SET processing_status = CASE
                    WHEN gridded3d_item_id IS NOT NULL
                         AND segmentation2d_item_id IS NOT NULL
                         AND analysis2d_item_id IS NOT NULL
                    THEN 'complete'
                    WHEN gridded3d_item_id IS NOT NULL
                         OR segmentation2d_item_id IS NOT NULL
                    THEN 'partial'
                    ELSE 'pending'
                END
                WHERE scan_time = ?
            """, (scan_time_str,))

            conn.commit()

        logger.debug(f"Item {item_id} linked to scan at {scan_time_str}")

    def get_scan(self, scan_time: datetime) -> Optional[Dict]:
        """Get scan record by time.

        Parameters
        ----------
        scan_time : datetime
            Scan timestamp

        Returns
        -------
        dict or None
            Scan record with all linked items
        """
        scan_time_str = scan_time.isoformat()

        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM scans WHERE scan_time = ?",
                (scan_time_str,)
            ).fetchone()

        return dict(row) if row else None

    def get_scan_by_id(self, scan_id: str) -> Optional[Dict]:
        """Get scan by ID.

        Parameters
        ----------
        scan_id : str
            Scan identifier

        Returns
        -------
        dict or None
            Scan record
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM scans WHERE scan_id = ?",
                (scan_id,)
            ).fetchone()

        return dict(row) if row else None

    def list_scans(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        run_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """List scans with optional time range filter.

        Parameters
        ----------
        start_time : datetime, optional
            Start of time range
        end_time : datetime, optional
            End of time range
        run_id : str, optional
            Filter by run ID
        status : str, optional
            Filter by processing status
        limit : int
            Maximum results (default 100)

        Returns
        -------
        DataFrame
            Scan records
        """
        query = "SELECT * FROM scans WHERE 1=1"
        params = []

        if start_time:
            query += " AND scan_time >= ?"
            params.append(start_time.isoformat())
        if end_time:
            query += " AND scan_time <= ?"
            params.append(end_time.isoformat())
        if run_id:
            query += " AND run_id = ?"
            params.append(run_id)
        if status:
            query += " AND processing_status = ?"
            params.append(status)

        query += " ORDER BY scan_time DESC"
        query += f" LIMIT {limit}"

        conn = self._get_connection()
        with self._lock:
            return pd.read_sql_query(query, conn, params=params)

    def get_latest_scan(self, run_id: Optional[str] = None) -> Optional[Dict]:
        """Get the most recent scan.

        Parameters
        ----------
        run_id : str, optional
            Filter by run ID

        Returns
        -------
        dict or None
            Latest scan record
        """
        conn = self._get_connection()
        with self._lock:
            if run_id:
                row = conn.execute("""
                    SELECT * FROM scans
                    WHERE run_id = ? AND processing_status = 'complete'
                    ORDER BY scan_time DESC
                    LIMIT 1
                """, (run_id,)).fetchone()
            else:
                row = conn.execute("""
                    SELECT * FROM scans
                    WHERE processing_status = 'complete'
                    ORDER BY scan_time DESC
                    LIMIT 1
                """).fetchone()

        return dict(row) if row else None

    # =========================================================================
    # Track Management
    # =========================================================================

    def register_track(
        self,
        track_index: int,
        run_id: str,
        start_time: datetime,
        birth_event: str = "NEW",
        birth_track_id: Optional[str] = None
    ) -> str:
        """Register a new track.

        Parameters
        ----------
        track_index : int
            Human-readable track index (starts at 1)
        run_id : str
            Run identifier
        start_time : datetime
            When track first appeared
        birth_event : str
            How track started: NEW or SPLIT
        birth_track_id : str, optional
            Parent track ID if SPLIT

        Returns
        -------
        str
            Track ID
        """
        import uuid

        track_id = str(uuid.uuid4())[:16]
        start_time_str = start_time.isoformat()
        now = datetime.now(timezone.utc).isoformat()

        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                INSERT INTO tracks
                (track_id, track_index, run_id, start_time, birth_event,
                 birth_track_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (track_id, track_index, run_id, start_time_str, birth_event,
                  birth_track_id, now, now))
            conn.commit()

        logger.debug(f"Track registered: {track_id} (index {track_index})")
        return track_id

    def update_track(
        self,
        track_id: str,
        end_time: Optional[datetime] = None,
        death_event: Optional[str] = None,
        death_track_id: Optional[str] = None,
        max_area_sqkm: Optional[float] = None,
        max_reflectivity: Optional[float] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None
    ) -> None:
        """Update track state.

        Parameters
        ----------
        track_id : str
            Track identifier
        end_time : datetime, optional
            When track ended
        death_event : str, optional
            How track ended: DISSIPATED or MERGED
        death_track_id : str, optional
            Target track ID if MERGED
        max_area_sqkm : float, optional
            Update maximum area
        max_reflectivity : float, optional
            Update maximum reflectivity
        bbox : tuple, optional
            Bounding box (min_x, min_y, max_x, max_y)
        """
        now = datetime.now(timezone.utc).isoformat()

        updates = ["updated_at = ?"]
        params = [now]

        if end_time is not None:
            updates.append("end_time = ?")
            params.append(end_time.isoformat())
            # Calculate lifetime
            conn = self._get_connection()
            with self._lock:
                row = conn.execute(
                    "SELECT start_time FROM tracks WHERE track_id = ?",
                    (track_id,)
                ).fetchone()
            if row:
                start = datetime.fromisoformat(row['start_time'])
                lifetime = (end_time - start).total_seconds() / 60.0
                updates.append("lifetime_minutes = ?")
                params.append(lifetime)

        if death_event is not None:
            updates.append("death_event = ?")
            params.append(death_event)
        if death_track_id is not None:
            updates.append("death_track_id = ?")
            params.append(death_track_id)
        if max_area_sqkm is not None:
            updates.append("max_area_sqkm = COALESCE(MAX(max_area_sqkm, ?), ?)")
            params.extend([max_area_sqkm, max_area_sqkm])
        if max_reflectivity is not None:
            updates.append("max_reflectivity = COALESCE(MAX(max_reflectivity, ?), ?)")
            params.extend([max_reflectivity, max_reflectivity])
        if bbox is not None:
            updates.extend([
                "bbox_min_x = COALESCE(MIN(bbox_min_x, ?), ?)",
                "bbox_min_y = COALESCE(MIN(bbox_min_y, ?), ?)",
                "bbox_max_x = COALESCE(MAX(bbox_max_x, ?), ?)",
                "bbox_max_y = COALESCE(MAX(bbox_max_y, ?), ?)"
            ])
            params.extend([bbox[0], bbox[0], bbox[1], bbox[1],
                           bbox[2], bbox[2], bbox[3], bbox[3]])

        params.append(track_id)

        conn = self._get_connection()
        with self._lock:
            conn.execute(f"""
                UPDATE tracks
                SET {', '.join(updates)}
                WHERE track_id = ?
            """, params)
            conn.commit()

    def add_track_observation(
        self,
        track_id: str,
        scan_time: datetime,
        cell_id: int,
        centroid_x: float,
        centroid_y: float,
        centroid_lat: Optional[float] = None,
        centroid_lon: Optional[float] = None,
        area_sqkm: Optional[float] = None,
        mean_reflectivity: Optional[float] = None,
        max_reflectivity: Optional[float] = None,
        core_area_sqkm: Optional[float] = None,
        vx: Optional[float] = None,
        vy: Optional[float] = None,
        speed: Optional[float] = None,
        lifecycle_phase: Optional[str] = None
    ) -> str:
        """Add observation to track.

        Parameters
        ----------
        track_id : str
            Track identifier
        scan_time : datetime
            Observation timestamp
        cell_id : int
            Segmentation label
        centroid_x : float
            X position (km from radar)
        centroid_y : float
            Y position (km from radar)
        centroid_lat : float, optional
            Geographic latitude
        centroid_lon : float, optional
            Geographic longitude
        area_sqkm : float, optional
            Cell area
        mean_reflectivity : float, optional
            Mean dBZ
        max_reflectivity : float, optional
            Max dBZ
        core_area_sqkm : float, optional
            Core area (>40 dBZ)
        vx : float, optional
            X velocity (km/min)
        vy : float, optional
            Y velocity (km/min)
        speed : float, optional
            Speed (km/min)
        lifecycle_phase : str, optional
            GROWTH | MATURE | DECAY

        Returns
        -------
        str
            Observation ID
        """
        import uuid

        observation_id = str(uuid.uuid4())[:16]
        scan_time_str = scan_time.isoformat()
        now = datetime.now(timezone.utc).isoformat()

        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                INSERT OR REPLACE INTO track_observations
                (observation_id, track_id, scan_time, cell_id,
                 centroid_x, centroid_y, centroid_lat, centroid_lon,
                 area_sqkm, mean_reflectivity, max_reflectivity, core_area_sqkm,
                 vx, vy, speed, lifecycle_phase, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (observation_id, track_id, scan_time_str, cell_id,
                  centroid_x, centroid_y, centroid_lat, centroid_lon,
                  area_sqkm, mean_reflectivity, max_reflectivity, core_area_sqkm,
                  vx, vy, speed, lifecycle_phase, now))
            conn.commit()

        # Update track max values
        self.update_track(
            track_id,
            max_area_sqkm=area_sqkm,
            max_reflectivity=max_reflectivity,
            bbox=(centroid_x, centroid_y, centroid_x, centroid_y)
        )

        return observation_id

    def get_track(self, track_id: str) -> Optional[Dict]:
        """Get track by ID.

        Parameters
        ----------
        track_id : str
            Track identifier

        Returns
        -------
        dict or None
            Track record
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM tracks WHERE track_id = ?",
                (track_id,)
            ).fetchone()

        return dict(row) if row else None

    def get_track_by_index(self, track_index: int, run_id: str) -> Optional[Dict]:
        """Get track by human-readable index and run.

        Parameters
        ----------
        track_index : int
            Track index
        run_id : str
            Run identifier

        Returns
        -------
        dict or None
            Track record
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute("""
                SELECT * FROM tracks
                WHERE track_index = ? AND run_id = ?
            """, (track_index, run_id)).fetchone()

        return dict(row) if row else None

    def get_track_path(self, track_id: str) -> List[Dict]:
        """Get all observations for a track in temporal order.

        Parameters
        ----------
        track_id : str
            Track identifier

        Returns
        -------
        list of dict
            Observations ordered by time
        """
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute("""
                SELECT * FROM track_observations
                WHERE track_id = ?
                ORDER BY scan_time ASC
            """, (track_id,)).fetchall()

        return [dict(row) for row in rows]

    def get_active_tracks(self, run_id: Optional[str] = None) -> List[Dict]:
        """Get all tracks without end_time (still active).

        Parameters
        ----------
        run_id : str, optional
            Filter by run ID

        Returns
        -------
        list of dict
            Active track records
        """
        conn = self._get_connection()
        with self._lock:
            if run_id:
                rows = conn.execute("""
                    SELECT * FROM tracks
                    WHERE end_time IS NULL AND run_id = ?
                    ORDER BY start_time DESC
                """, (run_id,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT * FROM tracks
                    WHERE end_time IS NULL
                    ORDER BY start_time DESC
                """).fetchall()

        return [dict(row) for row in rows]

    def get_tracks_at_time(self, scan_time: datetime) -> List[Dict]:
        """Get all tracks that were active at a specific time.

        Parameters
        ----------
        scan_time : datetime
            Timestamp to query

        Returns
        -------
        list of dict
            Track records
        """
        scan_time_str = scan_time.isoformat()

        conn = self._get_connection()
        with self._lock:
            rows = conn.execute("""
                SELECT t.* FROM tracks t
                INNER JOIN track_observations o ON t.track_id = o.track_id
                WHERE o.scan_time = ?
            """, (scan_time_str,)).fetchall()

        return [dict(row) for row in rows]

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            with self._lock:
                self._conn.close()
                self._conn = None
        logger.debug(f"Radar catalog connection closed for {self.radar}")
