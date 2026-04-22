"""Root-level registry manager for Adapt repository.

Manages the adapt_registry.db database at the repository root level.
This database tracks all runs, radars, and item types across the entire repository.

The Registry is a singleton per root_dir and provides:
- Run registration and status tracking
- Radar directory registration
- Item type definitions
- Global query capabilities

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

__all__ = ['RepositoryRegistry']

logger = logging.getLogger(__name__)

# Cache of registry instances per root directory
_registry_cache: Dict[str, 'RepositoryRegistry'] = {}
_cache_lock = threading.Lock()


class RepositoryRegistry:
    """Root-level registry for Adapt repository.
    
    Manages adapt_registry.db at {root_dir}/adapt_registry.db.
    Tracks all runs and radars across the repository.
    
    Thread-safe singleton per root_dir.
    
    Examples
    --------
    >>> registry = RepositoryRegistry.get_instance("/data/radar_output")
    >>> registry.register_radar("KHTX", "/data/radar_output/KHTX")
    >>> registry.register_run("abc123", "KHTX", mode="realtime")
    >>> runs = registry.list_runs()
    """
    
    def __init__(self, root_dir: Union[str, Path]):
        """Initialize registry at root directory.
        
        Parameters
        ----------
        root_dir : str or Path
            Root directory for the Adapt repository
        """
        self.root_dir = Path(root_dir).resolve()
        self.db_path = self.root_dir / "adapt_registry.db"
        
        # Thread safety
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        
        # Initialize database
        self._init_database()
        
        logger.debug("RepositoryRegistry initialized at %s", self.db_path)
    
    @classmethod
    def get_instance(cls, root_dir: Union[str, Path]) -> 'RepositoryRegistry':
        """Get singleton instance for a root directory.
        
        Parameters
        ----------
        root_dir : str or Path
            Root directory path
            
        Returns
        -------
        RepositoryRegistry
            Registry instance for this root directory
        """
        root_path = str(Path(root_dir).resolve())
        
        with _cache_lock:
            if root_path not in _registry_cache:
                _registry_cache[root_path] = cls(root_dir)
            return _registry_cache[root_path]
    
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
        schema_path = Path(__file__).parent / "schemas" / "registry_schema.sql"
        
        if not schema_path.exists():
            # Fallback to embedded schema if file not found
            self._create_schema_inline()
            return
        
        with open(schema_path) as f:
            schema_sql = f.read()
        
        conn = self._get_connection()
        with self._lock:
            conn.executescript(schema_sql)
            conn.commit()
        
        logger.debug(f"Registry schema initialized from {schema_path}")
    
    def _create_schema_inline(self) -> None:
        """Create schema inline (fallback)."""
        conn = self._get_connection()
        with self._lock:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            
            # Runs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    radar TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    status TEXT NOT NULL,
                    mode TEXT,
                    config_path TEXT,
                    repository_version TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_start_time ON runs(start_time DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_radar ON runs(radar)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            
            # Radars table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS radars (
                    radar TEXT PRIMARY KEY,
                    catalog_path TEXT NOT NULL,
                    data_path TEXT NOT NULL,
                    location_lat REAL,
                    location_lon REAL,
                    created_at TEXT NOT NULL,
                    last_updated TEXT NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_radars_updated ON radars(last_updated DESC)")
            
            # Item types table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS item_types (
                    item_type TEXT PRIMARY KEY,
                    description TEXT NOT NULL,
                    storage_format TEXT NOT NULL,
                    dimensionality TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            
            # Prepopulate item types
            now = datetime.now(timezone.utc).isoformat()
            item_types_data = [
                ('gridded3d', 'Gridded reflectivity volume', 'netcdf', '3d', now),
                ('segmentation2d', 'Cell segmentation masks', 'netcdf', '2d', now),
                ('projection2d', 'Cell motion projections', 'netcdf', '2d', now),
                ('analysis2d', 'Cell-level analysis metrics', 'parquet', 'table', now),
            ]
            
            conn.executemany("""
                INSERT OR IGNORE INTO item_types 
                (item_type, description, storage_format, dimensionality, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, item_types_data)
            
            conn.commit()
    
    # =========================================================================
    # Radar Management
    # =========================================================================
    
    def register_radar(
        self,
        radar: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None
    ) -> None:
        """Register a radar in the repository.
        
        Parameters
        ----------
        radar : str
            Radar station identifier (e.g., "KHTX")
        lat : float, optional
            Radar latitude
        lon : float, optional
            Radar longitude
        """
        radar_dir = self.root_dir / radar
        radar_dir.mkdir(parents=True, exist_ok=True)
        
        catalog_path = str(radar_dir / "catalog.db")
        data_path = str(radar_dir)
        now = datetime.now(timezone.utc).isoformat()
        
        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                INSERT OR REPLACE INTO radars 
                (radar, catalog_path, data_path, location_lat, location_lon, created_at, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (radar, catalog_path, data_path, lat, lon, now, now))
            conn.commit()
        
        logger.debug("Radar registered: %s at %s", radar, data_path)

    def get_radar_location(self, radar: str) -> tuple[Optional[float], Optional[float]]:
        """Get stored radar location (lat, lon) from the registry."""
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT location_lat, location_lon FROM radars WHERE radar = ?",
                (radar,),
            ).fetchone()
        if not row:
            return None, None
        return row["location_lat"], row["location_lon"]

    def ensure_radar_location(self, radar: str, lat: float, lon: float) -> None:
        """Ensure radar location is stored in the registry.

        This is intentionally deterministic and does not use external lookup
        tables. It is meant to be called once the location is available from
        pipeline inputs (e.g., the first NEXRAD file/gridded dataset).
        """
        if lat is None or lon is None:
            raise ValueError("lat/lon must be provided")

        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except Exception as e:
            raise ValueError(f"Invalid lat/lon types: {type(lat)} {type(lon)}") from e

        conn = self._get_connection()
        now = datetime.now(timezone.utc).isoformat()

        with self._lock:
            row = conn.execute(
                "SELECT location_lat, location_lon FROM radars WHERE radar = ?",
                (radar,),
            ).fetchone()
            if not row:
                raise ValueError(f"Radar '{radar}' is not registered in the repository registry")

            existing_lat = row["location_lat"]
            existing_lon = row["location_lon"]
            if existing_lat is not None and existing_lon is not None:
                return

            conn.execute(
                "UPDATE radars SET location_lat = ?, location_lon = ?, last_updated = ? WHERE radar = ?",
                (lat_f, lon_f, now, radar),
            )
            conn.commit()
    
    def get_radar_catalog_path(self, radar: str) -> Optional[Path]:
        """Get path to radar's catalog database.
        
        Parameters
        ----------
        radar : str
            Radar identifier
            
        Returns
        -------
        Path or None
            Path to catalog.db, or None if radar not registered
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT catalog_path FROM radars WHERE radar = ?",
                (radar,)
            ).fetchone()
        
        return Path(row['catalog_path']) if row else None
    
    def list_radars(self) -> pd.DataFrame:
        """Get list of all registered radars.
        
        Returns
        -------
        DataFrame
            Radar metadata
        """
        conn = self._get_connection()
        with self._lock:
            return pd.read_sql_query(
                "SELECT * FROM radars ORDER BY radar",
                conn
            )
    
    # =========================================================================
    # Run Management
    # =========================================================================
    
    def register_run(
        self,
        run_id: str,
        radar: str,
        mode: Optional[str] = None,
        config_path: Optional[str] = None,
        repository_version: str = "0.1.0"
    ) -> None:
        """Register a new pipeline run.
        
        Parameters
        ----------
        run_id : str
            Unique run identifier
        radar : str
            Radar being processed
        mode : str, optional
            Run mode (realtime, historical, backfill)
        config_path : str, optional
            Path to runtime configuration JSON
        repository_version : str
            Adapt version
        """
        now = datetime.now(timezone.utc).isoformat()
        
        conn = self._get_connection()
        with self._lock:
            conn.execute("""
                INSERT OR IGNORE INTO runs 
                (run_id, radar, start_time, status, mode, config_path, repository_version, created_at)
                VALUES (?, ?, ?, 'running', ?, ?, ?, ?)
            """, (run_id, radar, now, mode, config_path, repository_version, now))
            conn.commit()
        
        logger.debug("Run registered: %s for radar %s", run_id, radar)
    
    def update_run_status(
        self,
        run_id: str,
        status: str,
        end_time: Optional[str] = None
    ) -> None:
        """Update run status.
        
        Parameters
        ----------
        run_id : str
            Run identifier
        status : str
            New status (running, complete, failed)
        end_time : str, optional
            ISO8601 end timestamp
        """
        conn = self._get_connection()
        with self._lock:
            if end_time:
                conn.execute(
                    "UPDATE runs SET status = ?, end_time = ? WHERE run_id = ?",
                    (status, end_time, run_id)
                )
            else:
                conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ?",
                    (status, run_id)
                )
            conn.commit()
        
        logger.debug(f"Run {run_id} status updated to {status}")
    
    def list_runs(self, radar: Optional[str] = None) -> pd.DataFrame:
        """Get list of runs, optionally filtered by radar.
        
        Parameters
        ----------
        radar : str, optional
            Filter by radar ID
            
        Returns
        -------
        DataFrame
            Run metadata
        """
        conn = self._get_connection()
        with self._lock:
            if radar:
                query = "SELECT * FROM runs WHERE radar = ? ORDER BY start_time DESC"
                return pd.read_sql_query(query, conn, params=(radar,))
            else:
                query = "SELECT * FROM runs ORDER BY start_time DESC"
                return pd.read_sql_query(query, conn)
    
    def get_latest_run(self, radar: Optional[str] = None) -> Optional[Dict]:
        """Get the most recent run.
        
        Parameters
        ----------
        radar : str, optional
            Filter by radar ID
            
        Returns
        -------
        dict or None
            Run metadata dictionary
        """
        conn = self._get_connection()
        with self._lock:
            if radar:
                row = conn.execute(
                    "SELECT * FROM runs WHERE radar = ? ORDER BY start_time DESC LIMIT 1",
                    (radar,)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM runs ORDER BY start_time DESC LIMIT 1"
                ).fetchone()
        
        return dict(row) if row else None
    
    # =========================================================================
    # Item Types Management
    # =========================================================================
    
    def list_item_types(self) -> List[str]:
        """Get list of registered item types.
        
        Returns
        -------
        list of str
            Item type names
        """
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute("SELECT item_type FROM item_types ORDER BY item_type").fetchall()
        
        return [row['item_type'] for row in rows]
    
    def get_item_type_info(self, item_type: str) -> Optional[Dict]:
        """Get metadata for an item type.
        
        Parameters
        ----------
        item_type : str
            Item type name
            
        Returns
        -------
        dict or None
            Item type metadata
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM item_types WHERE item_type = ?",
                (item_type,)
            ).fetchone()
        
        return dict(row) if row else None
    
    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            with self._lock:
                self._conn.close()
                self._conn = None
        logger.debug("Registry connection closed")
