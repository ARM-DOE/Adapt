# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

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

import logging
import threading
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from adapt.persistence.sqlite_store import SqliteStore

__all__ = ["RepositoryRegistry"]

logger = logging.getLogger(__name__)

# Cache of registry instances per root directory
_registry_cache: dict[str, "RepositoryRegistry"] = {}
_cache_lock = threading.Lock()


class RepositoryRegistry(SqliteStore):
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

    def __init__(self, root_dir: str | Path):
        """Initialize registry at root directory.

        Parameters
        ----------
        root_dir : str or Path
            Root directory for the Adapt repository
        """
        self.root_dir = Path(root_dir).resolve()
        super().__init__(self.root_dir / "adapt_registry.db", "registry_schema.sql")
        logger.debug("RepositoryRegistry initialized at %s", self.db_path)

    @classmethod
    def get_instance(cls, root_dir: str | Path) -> "RepositoryRegistry":
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

    # =========================================================================
    # Radar Management
    # =========================================================================

    def register_radar(
        self, radar: str, lat: float | None = None, lon: float | None = None
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
        now = datetime.now(UTC).isoformat()

        conn = self._get_connection()
        with self._lock:
            conn.execute(
                """
                INSERT OR REPLACE INTO radars
                (radar, catalog_path, data_path,
                 location_lat, location_lon, created_at, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (radar, catalog_path, data_path, lat, lon, now, now),
            )
            conn.commit()

        logger.debug("Radar registered: %s at %s", radar, data_path)

    def get_radar_location(self, radar: str) -> tuple[float | None, float | None]:
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
        now = datetime.now(UTC).isoformat()

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
                "UPDATE radars SET location_lat = ?, location_lon = ?, "
                "last_updated = ? WHERE radar = ?",
                (lat_f, lon_f, now, radar),
            )
            conn.commit()

    def get_radar_catalog_path(self, radar: str) -> Path | None:
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
                "SELECT catalog_path FROM radars WHERE radar = ?", (radar,)
            ).fetchone()

        return Path(row["catalog_path"]) if row else None

    def list_radars(self) -> pd.DataFrame:
        """Get list of all registered radars.

        Returns
        -------
        DataFrame
            Radar metadata
        """
        conn = self._get_connection()
        with self._lock:
            return pd.read_sql_query("SELECT * FROM radars ORDER BY radar", conn)

    # =========================================================================
    # Run Management
    # =========================================================================

    def register_run(
        self,
        run_id: str,
        radar: str,
        mode: str | None = None,
        config_path: str | None = None,
        repository_version: str = "0.1.0",
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
        now = datetime.now(UTC).isoformat()

        conn = self._get_connection()
        with self._lock:
            conn.execute(
                """
                INSERT OR IGNORE INTO runs
                (run_id, radar, start_time, status, mode,
                 config_path, repository_version, created_at)
                VALUES (?, ?, ?, 'running', ?, ?, ?, ?)
                """,
                (run_id, radar, now, mode, config_path, repository_version, now),
            )
            conn.commit()

        logger.debug("Run registered: %s for radar %s", run_id, radar)

    def update_run_status(self, run_id: str, status: str, end_time: str | None = None) -> None:
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
                    (status, end_time, run_id),
                )
            else:
                conn.execute("UPDATE runs SET status = ? WHERE run_id = ?", (status, run_id))
            conn.commit()

        logger.debug(f"Run {run_id} status updated to {status}")

    def list_runs(self, radar: str | None = None) -> pd.DataFrame:
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

    def get_latest_run(self, radar: str | None = None) -> dict | None:
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
                    (radar,),
                ).fetchone()
            else:
                row = conn.execute("SELECT * FROM runs ORDER BY start_time DESC LIMIT 1").fetchone()

        return dict(row) if row else None

    # =========================================================================
    # Item Types Management
    # =========================================================================

    def list_item_types(self) -> list[str]:
        """Get list of registered item types.

        Returns
        -------
        list of str
            Item type names
        """
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute("SELECT item_type FROM item_types ORDER BY item_type").fetchall()

        return [row["item_type"] for row in rows]

    def get_item_type_info(self, item_type: str) -> dict | None:
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
                "SELECT * FROM item_types WHERE item_type = ?", (item_type,)
            ).fetchone()

        return dict(row) if row else None
