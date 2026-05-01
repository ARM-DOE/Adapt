# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Read-only DataClient for Adapt repository (Refactored).

This is the user-facing, read-only interface for querying Adapt pipeline data.
It discovers data through the two-tier database system:
- Root-level registry (adapt_registry.db) for runs and radars
- Radar-level catalogs (catalog.db) for items and progress

Key features:
- Initialize with repository root only
- Auto-discover runs and radars
- Query items via SQL (DuckDB over Parquet)
- Load NetCDF/Parquet data seamlessly
- Stream new data with monotonic polling
- No file path exposure to users

Example usage::

    from adapt.api import DataClient
    
    # Initialize from repository root
    client = DataClient("/data/radar_output")
    
    # Discover what's available
    runs = client.list_runs()
    radars = client.list_radars()
    item_types = client.item_types()
    
    # Load latest data
    df = client.latest("analysis2d", radar="KHTX")
    
    # SQL queries on Parquet
    df = client.query("SELECT * FROM analysis2d WHERE refl_max > 40")
    
    # Stream new data
    for batch in client.stream("SELECT * FROM analysis2d", poll_interval=5):
        print(f"Got {len(batch)} new rows")
"""

import contextlib
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import xarray as xr

from adapt.persistence.catalog import RadarCatalog
from adapt.persistence.registry import RepositoryRegistry
from adapt.persistence.track_store import TrackStore

__all__ = ['DataClient']

logger = logging.getLogger(__name__)


class DataClient:
    """Read-only interface for Adapt repository.
    
    Thread-safe for notebook usage.
    Discovers all data through catalog databases (no filesystem inspection).
    
    Parameters
    ----------
    repository_root : str or Path
        Root directory of Adapt repository
        
    Examples
    --------
    >>> client = DataClient("/data/radar_output")
    >>> runs = client.list_runs()
    >>> df = client.latest("analysis2d", radar="KHTX")
    """
    
    def __init__(self, repository_root: str | Path):
        """Initialize DataClient from repository root.
        
        Parameters
        ----------
        repository_root : str or Path
            Path to Adapt repository root directory
        """
        self.root_dir = Path(repository_root).resolve()
        
        if not self.root_dir.exists():
            raise FileNotFoundError(f"Repository not found: {self.root_dir}")
        
        # Connect to root-level registry
        self.registry = RepositoryRegistry.get_instance(self.root_dir)
        
        # DuckDB connection for SQL queries
        self._duckdb_conn: duckdb.DuckDBPyConnection | None = None
        
        # Cache of radar catalogs
        self._radar_catalogs: dict[str, RadarCatalog] = {}
        
        logger.info(f"DataClient initialized at {self.root_dir}")
    
    def _get_duckdb_conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection."""
        if self._duckdb_conn is None:
            self._duckdb_conn = duckdb.connect(':memory:')
            logger.debug("Created in-memory DuckDB connection")
        return self._duckdb_conn
    
    def _get_radar_catalog(self, radar: str) -> RadarCatalog:
        """Get radar catalog instance."""
        if radar not in self._radar_catalogs:
            radar_dir = self.root_dir / radar
            if not radar_dir.exists():
                raise FileNotFoundError(f"Radar directory not found: {radar_dir}")
            self._radar_catalogs[radar] = RadarCatalog(radar_dir)
        return self._radar_catalogs[radar]
    
    # =========================================================================
    # Repository Validation
    # =========================================================================

    def is_initialized(self) -> bool:
        """Check if repository is properly initialized.

        Returns
        -------
        bool
            True if repository has a valid registry database
        """
        registry_path = self.root_dir / "adapt_registry.db"
        return registry_path.exists()

    def get_repository_info(self) -> dict[str, Any]:
        """Get repository summary information.

        Returns
        -------
        dict
            Repository metadata including:
            - path: str
            - is_initialized: bool
            - num_radars: int
            - num_runs: int
            - radars: list of str
        """
        if not self.is_initialized():
            return {
                'path': str(self.root_dir),
                'is_initialized': False,
                'num_radars': 0,
                'num_runs': 0,
                'radars': [],
            }

        radars = self.list_radars()
        runs = self.list_runs()

        return {
            'path': str(self.root_dir),
            'is_initialized': True,
            'num_radars': len(radars),
            'num_runs': len(runs),
            'radars': radars,
        }

    # =========================================================================
    # Discovery Methods
    # =========================================================================

    def list_runs(self, radar: str | None = None) -> pd.DataFrame:
        """List all runs, optionally filtered by radar.
        
        Parameters
        ----------
        radar : str, optional
            Filter by radar ID
            
        Returns
        -------
        DataFrame
            Run metadata (run_id, radar, start_time, status, etc.)
        """
        return self.registry.list_runs(radar=radar)
    
    def list_radars(self) -> list[str]:
        """List all registered radars.

        Returns
        -------
        list of str
            Radar IDs
        """
        radars_df = self.registry.list_radars()
        return radars_df['radar'].tolist() if not radars_df.empty else []

    def get_radar_info(self, radar: str) -> dict[str, Any]:
        """Get detailed information for a specific radar.

        Parameters
        ----------
        radar : str
            Radar ID

        Returns
        -------
        dict
            Radar metadata including:
            - radar: str
            - num_runs: int
            - runs: list of dict (run_id, start_time, end_time, status, mode)
            - date_range: dict (start, end) or None
            - num_scans: int
        """
        if radar not in self.list_radars():
            raise ValueError(f"Radar '{radar}' not found in repository")

        # Get runs for this radar
        runs_df = self.list_runs(radar=radar)
        runs_list = []
        for _, row in runs_df.iterrows():
            runs_list.append({
                'run_id': row['run_id'],
                'start_time': row['start_time'],
                'end_time': row.get('end_time'),
                'status': row['status'],
                'mode': row.get('mode'),
            })

        # Get date range and scan count from catalog
        date_range = None
        num_scans = 0

        try:
            catalog = self._get_radar_catalog(radar)
            conn = catalog._get_connection()

            with catalog._lock:
                # Get date range from items table
                row = conn.execute("""
                    SELECT
                        MIN(scan_time) as start_time,
                        MAX(scan_time) as end_time,
                        COUNT(*) as num_scans
                    FROM items
                    WHERE status = 'complete'
                """).fetchone()

                if row and row['start_time']:
                    date_range = {
                        'start': row['start_time'],
                        'end': row['end_time'],
                    }
                    num_scans = row['num_scans']

        except FileNotFoundError:
            pass  # Catalog may not exist yet

        return {
            'radar': radar,
            'num_runs': len(runs_list),
            'runs': runs_list,
            'date_range': date_range,
            'num_scans': num_scans,
        }

    def get_run_info(self, run_id: str, radar: str | None = None) -> dict[str, Any]:
        """Get detailed information for a specific run.

        Parameters
        ----------
        run_id : str
            Run ID to query
        radar : str, optional
            Radar ID (will be looked up if not provided)

        Returns
        -------
        dict
            Run metadata including:
            - run_id: str
            - radar: str
            - start_time: str
            - end_time: str or None
            - status: str
            - mode: str
            - date_range: dict (start, end) or None
            - num_scans: int
        """
        runs_df = self.list_runs(radar=radar)
        run_row = runs_df[runs_df['run_id'] == run_id]

        if run_row.empty:
            raise ValueError(f"Run '{run_id}' not found")

        run = run_row.iloc[0]
        radar = run['radar']

        # Get date range for this specific run from catalog
        date_range = None
        num_scans = 0

        try:
            catalog = self._get_radar_catalog(radar)
            conn = catalog._get_connection()

            with catalog._lock:
                row = conn.execute("""
                    SELECT
                        MIN(scan_time) as start_time,
                        MAX(scan_time) as end_time,
                        COUNT(*) as num_scans
                    FROM items
                    WHERE run_id = ? AND status = 'complete'
                """, (run_id,)).fetchone()

                if row and row['start_time']:
                    date_range = {
                        'start': row['start_time'],
                        'end': row['end_time'],
                    }
                    num_scans = row['num_scans']

        except FileNotFoundError:
            pass

        return {
            'run_id': run_id,
            'radar': radar,
            'start_time': run['start_time'],
            'end_time': run.get('end_time'),
            'status': run['status'],
            'mode': run.get('mode'),
            'date_range': date_range,
            'num_scans': num_scans,
        }
    
    def item_types(self) -> list[str]:
        """List registered item types.
        
        Returns
        -------
        list of str
            Item type names (e.g., ['analysis2d', 'gridded3d', ...])
        """
        return self.registry.list_item_types()
    
    def fields(self, item_type: str, radar: str | None = None) -> list[str]:
        """Get column names for a Parquet table item type.
        
        Parameters
        ----------
        item_type : str
            Item type name
        radar : str, optional
            Radar to query (uses first available if not specified)
            
        Returns
        -------
        list of str
            Column names
        """
        # Get item type info to check if it's a table type
        info = self.registry.get_item_type_info(item_type)
        if not info or info['storage_format'] != 'parquet':
            raise ValueError(f"{item_type} is not a Parquet table type")
        
        # Find a radar with this item type
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise ValueError("No radars found in repository")
            radar = radars[0]
        
        # Get schema from radar catalog
        catalog = self._get_radar_catalog(radar)
        schema = catalog.get_schema(item_type)
        
        if schema:
            return [col['name'] for col in schema]
        
        # Fallback: query actual Parquet file
        item = catalog.get_latest_item(item_type)
        if item:
            file_path = self.root_dir / radar / item['file_path']
            if file_path.exists():
                df = pd.read_parquet(file_path, engine='pyarrow')
                return df.columns.tolist()
        
        return []
    
    def status(self, run_id: str | None = None, radar: str | None = None) -> dict:
        """Get processing status/progress.
        
        Parameters
        ----------
        run_id : str, optional
            Run ID (uses latest if not specified)
        radar : str, optional
            Radar ID (uses first available if not specified)
            
        Returns
        -------
        dict
            Progress metadata
        """
        if not run_id:
            latest_run = self.registry.get_latest_run(radar=radar)
            if not latest_run:
                return {}
            run_id = latest_run['run_id']
            radar = latest_run['radar']
        
        if not radar:
            # Get radar from run
            runs = self.list_runs()
            run_row = runs[runs['run_id'] == run_id]
            if run_row.empty:
                return {}
            radar = run_row.iloc[0]['radar']
        
        catalog = self._get_radar_catalog(radar)
        progress = catalog.get_progress(run_id)
        
        return progress if progress else {}
    
    # =========================================================================
    # Data Access Methods
    # =========================================================================
    
    def latest(
        self,
        item_type: str,
        radar: str | None = None
    ) -> pd.DataFrame | xr.Dataset:
        """Load the most recent item of a given type.
        
        Parameters
        ----------
        item_type : str
            Item type to load
        radar : str, optional
            Radar ID (uses first available if not specified)
            
        Returns
        -------
        DataFrame or Dataset
            Loaded data (DataFrame for Parquet, Dataset for NetCDF)
        """
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise ValueError("No radars found in repository")
            radar = radars[0]
        
        catalog = self._get_radar_catalog(radar)
        item = catalog.get_latest_item(item_type)
        
        if not item:
            raise FileNotFoundError(f"No items found for type '{item_type}' in radar {radar}")
        
        # Construct full file path
        file_path = self.root_dir / radar / item['file_path']
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Load based on storage format
        info = self.registry.get_item_type_info(item_type)
        if info and info['storage_format'] == 'parquet':
            return pd.read_parquet(file_path, engine='pyarrow')
        elif info and info['storage_format'] == 'netcdf':
            return xr.open_dataset(file_path)
        else:
            # Try to infer from extension
            if file_path.suffix == '.parquet':
                return pd.read_parquet(file_path, engine='pyarrow')
            elif file_path.suffix in ['.nc', '.nc4', '.netcdf']:
                return xr.open_dataset(file_path)
            else:
                raise ValueError(f"Unknown file format for {file_path}")
    
    def query(self, sql: str, radar: str | None = None) -> pd.DataFrame:
        """Execute SQL query on Parquet tables.
        
        Only SELECT queries are allowed. Dynamically creates DuckDB views
        for Parquet files based on catalog metadata.
        
        Parameters
        ----------
        sql : str
            SELECT SQL query
        radar : str, optional
            Radar to query (uses first available if not specified)
            
        Returns
        -------
        DataFrame
            Query results
        """
        # Validate SELECT only
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed")
        
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise ValueError("No radars found in repository")
            radar = radars[0]
        
        conn = self._get_duckdb_conn()
        catalog = self._get_radar_catalog(radar)
        
        # Get all Parquet item types
        parquet_types = [ it for it in self.item_types()
            if self.registry.get_item_type_info(it)['storage_format'] == 'parquet'
        ]
        
        # Create views for each Parquet type
        for item_type in parquet_types:
            items = catalog.query_items(item_type=item_type, status='complete')
            
            if items.empty:
                continue
            
            # Get all Parquet file paths
            file_paths = [
                str(self.root_dir / radar / row['file_path'])
                for _, row in items.iterrows()
            ]
            
            # Create or replace view
            if file_paths:
                # Use read_parquet with glob pattern or list
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {item_type}")
                    # Register table view
                    conn.execute(f"""
                        CREATE VIEW {item_type} AS 
                        SELECT * FROM read_parquet({file_paths})
                    """)
                except Exception as e:
                    logger.warning(f"Could not create view for {item_type}: {e}")
        
        # Execute user query
        try:
            result = conn.execute(sql).fetchdf()
            return result
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    # =========================================================================
    # Scan Listing and Time-Based Access
    # =========================================================================

    def list_scans(
        self,
        item_type: str,
        radar: str | None = None,
        limit: int = 50
    ) -> pd.DataFrame:
        """List available scans with timestamps.

        Parameters
        ----------
        item_type : str
            Item type to list (e.g., 'segmentation2d', 'analysis2d')
        radar : str, optional
            Radar ID (uses first available if not specified)
        limit : int
            Maximum number of scans to return (default 50)

        Returns
        -------
        DataFrame
            Scan metadata with columns: scan_time, item_id, file_path, status
        """
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise ValueError("No radars found in repository")
            radar = radars[0]

        catalog = self._get_radar_catalog(radar)
        items = catalog.query_items(
            item_type=item_type,
            status='complete',
            limit=limit,
            order_by='scan_time DESC'
        )

        if items.empty:
            return pd.DataFrame(columns=['scan_time', 'item_id', 'file_path'])

        return items[['scan_time', 'item_id', 'file_path', 'status']].copy()

    def get_scan_at(
        self,
        scan_time: str | datetime,
        item_type: str,
        radar: str | None = None
    ) -> pd.DataFrame | xr.Dataset:
        """Load a specific scan by timestamp.

        Parameters
        ----------
        scan_time : str or datetime
            Target scan time (ISO8601 string or datetime)
        item_type : str
            Item type to load
        radar : str, optional
            Radar ID (uses first available if not specified)

        Returns
        -------
        DataFrame or Dataset
            Loaded data (DataFrame for Parquet, Dataset for NetCDF)

        Raises
        ------
        FileNotFoundError
            If no scan found at or near the specified time
        """
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise ValueError("No radars found in repository")
            radar = radars[0]

        # Convert to string for comparison
        scan_time_str = scan_time.isoformat() if isinstance(scan_time, datetime) else scan_time

        catalog = self._get_radar_catalog(radar)
        conn = catalog._get_connection()

        # Find the exact or nearest scan
        with catalog._lock:
            row = conn.execute("""
                SELECT * FROM items
                WHERE item_type = ? AND status = 'complete'
                ORDER BY ABS(julianday(scan_time) - julianday(?))
                LIMIT 1
            """, (item_type, scan_time_str)).fetchone()

        if not row:
            raise FileNotFoundError(
                f"No scan found for type '{item_type}' near time {scan_time_str}"
            )

        item = dict(row)
        file_path = self.root_dir / radar / item['file_path']

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Load based on storage format
        info = self.registry.get_item_type_info(item_type)
        if info and info['storage_format'] == 'parquet':
            return pd.read_parquet(file_path, engine='pyarrow')
        elif info and info['storage_format'] == 'netcdf':
            return xr.open_dataset(file_path)
        else:
            # Infer from extension
            if file_path.suffix == '.parquet':
                return pd.read_parquet(file_path, engine='pyarrow')
            elif file_path.suffix in ['.nc', '.nc4', '.netcdf']:
                return xr.open_dataset(file_path)
            else:
                raise ValueError(f"Unknown file format for {file_path}")

    # =========================================================================
    # Cell Tracking Methods
    # =========================================================================

    def _track_store(self, radar: str | None = None) -> TrackStore:
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise ValueError("No radars found in repository")
            radar = radars[0]
        catalog = RadarCatalog(self.root_dir / radar)
        return TrackStore(catalog.db_path)

    def cells_by_scan(
        self,
        run_id: str,
        scan_time: datetime,
        radar: str | None = None,
    ) -> pd.DataFrame:
        """All tracked cells for a single scan."""
        return self._track_store(radar).get_cells_by_scan(run_id, scan_time)

    def track_history(
        self,
        run_id: str,
        cell_uid: str,
        radar: str | None = None,
    ) -> pd.DataFrame:
        """All scan rows for one track, ordered by scan_time."""
        return self._track_store(radar).get_track_history(run_id, cell_uid)

    def cell_events(
        self,
        run_id: str,
        cell_uid: str | None = None,
        radar: str | None = None,
    ) -> pd.DataFrame:
        """Lineage events for a run, optionally filtered to one cell_uid."""
        return self._track_store(radar).get_cell_events(run_id, cell_uid)

    def cell_tracks(
        self,
        run_id: str,
        radar: str | None = None,
    ) -> pd.DataFrame:
        """Lifecycle summary for all tracks in a run."""
        return self._track_store(radar).get_cell_tracks(run_id)

    # =========================================================================
    # Pipeline Status Methods
    # =========================================================================

    def is_pipeline_running(self, radar: str | None = None) -> bool:
        """Check if pipeline is actively processing.

        Checks for active run status and recent progress updates.

        Parameters
        ----------
        radar : str, optional
            Radar to check (uses first available if not specified)

        Returns
        -------
        bool
            True if pipeline appears to be running
        """
        # Check PID file first
        pid_file = Path.home() / '.adapt' / 'pipeline.pid'
        if pid_file.exists():
            try:
                import os
                pid = int(pid_file.read_text().strip())
                os.kill(pid, 0)  # Check if process exists
                return True
            except (ValueError, OSError, ProcessLookupError):
                pass

        # Check for recent progress updates
        if not radar:
            radars = self.list_radars()
            if not radars:
                return False
            radar = radars[0]

        try:
            catalog = self._get_radar_catalog(radar)
            runs = self.registry.list_runs(radar=radar)

            if runs.empty:
                return False

            # Check if any run has status 'running'
            running_runs = runs[runs['status'] == 'running']
            if not running_runs.empty:
                return True

            # Check for recent progress (within last 60 seconds)
            latest_run = runs.iloc[0]
            progress = catalog.get_progress(latest_run['run_id'])

            if progress and progress.get('last_updated'):
                last_update = datetime.fromisoformat(
                    progress['last_updated'].replace('Z', '+00:00')
                )
                age_seconds = (datetime.now(UTC) - last_update).total_seconds()
                return age_seconds < 60

        except Exception as e:
            logger.debug(f"Error checking pipeline status: {e}")

        return False

    def get_pipeline_progress(
        self,
        radar: str | None = None,
        run_id: str | None = None
    ) -> dict[str, Any]:
        """Get detailed pipeline progress.

        Parameters
        ----------
        radar : str, optional
            Radar to query
        run_id : str, optional
            Specific run ID (uses latest if not specified)

        Returns
        -------
        dict
            Progress metadata including:
            - is_running: bool
            - num_items_complete: int
            - num_items_failed: int
            - latest_scan_time: str
            - queue_depth: int
            - last_updated: str
        """
        if not radar:
            radars = self.list_radars()
            if not radars:
                return {'is_running': False, 'error': 'No radars found'}
            radar = radars[0]

        try:
            catalog = self._get_radar_catalog(radar)

            if not run_id:
                runs = self.registry.list_runs(radar=radar)
                if runs.empty:
                    return {'is_running': False, 'error': 'No runs found'}
                run_id = runs.iloc[0]['run_id']

            progress = catalog.get_progress(run_id)

            if not progress:
                return {
                    'is_running': False,
                    'run_id': run_id,
                    'num_items_complete': 0,
                    'num_items_failed': 0,
                }

            # Determine if running
            is_running = self.is_pipeline_running(radar=radar)

            return {
                'is_running': is_running,
                'run_id': run_id,
                'radar': radar,
                'num_items_complete': progress.get('num_items_complete', 0),
                'num_items_failed': progress.get('num_items_failed', 0),
                'queue_depth': progress.get('queue_depth', 0),
                'latest_downloaded_time': progress.get('latest_downloaded_time'),
                'latest_gridded_time': progress.get('latest_gridded_time'),
                'latest_segmented_time': progress.get('latest_segmented_time'),
                'latest_analyzed_time': progress.get('latest_analyzed_time'),
                'last_updated': progress.get('last_updated'),
            }

        except Exception as e:
            logger.error(f"Error getting pipeline progress: {e}")
            return {'is_running': False, 'error': str(e)}

    # =========================================================================
    # Scan Bundle Methods
    # =========================================================================

    def get_scan_bundle(
        self,
        scan_time: str | datetime,
        radar: str | None = None
    ) -> dict[str, Any]:
        """Get all data for a specific scan in a single call.

        Returns all linked data products for a scan: segmentation, cells DataFrame,
        tracks, and metadata. This is the primary method for GUI display.

        Parameters
        ----------
        scan_time : str or datetime
            Scan timestamp (ISO8601 or datetime)
        radar : str, optional
            Radar ID (uses first available if not specified)

        Returns
        -------
        dict
            Bundle containing:
            - scan_time: str - ISO8601 timestamp
            - segmentation2d: xr.Dataset or None - Segmentation NetCDF
            - cells: pd.DataFrame or None - Cell analysis data
            - tracks: list of dict - Tracking summary entries for this scan
            - metadata: dict - Scan metadata (num_cells, max_reflectivity, etc.)

        Raises
        ------
        FileNotFoundError
            If radar or scan not found
        """
        if not radar:
            radars = self.list_radars()
            if not radars:
                raise FileNotFoundError("No radars found in repository")
            radar = radars[0]

        # Convert to datetime if string
        if isinstance(scan_time, str):
            scan_time_dt = datetime.fromisoformat(scan_time.replace('Z', '+00:00'))
        else:
            scan_time_dt = scan_time

        catalog = self._get_radar_catalog(radar)

        bundle: dict[str, Any] = {
            'scan_time': (
                scan_time_dt.isoformat() if isinstance(scan_time_dt, datetime) else scan_time
            ),
            'radar': radar,
            'segmentation2d': None,
            'cells': None,
            'tracks': [],
            'metadata': {},
        }

        # Try to get scan from scans table, fall back to items if table doesn't exist
        scan = None
        with contextlib.suppress(Exception):
            scan = catalog.get_scan(scan_time_dt)

        # If no scan record, fall back to item-based lookup
        if not scan:
            return self._get_scan_bundle_fallback(scan_time_dt, radar, bundle)

        # Populate metadata from scan record
        bundle['metadata'] = {
            'scan_id': scan.get('scan_id'),
            'run_id': scan.get('run_id'),
            'processing_status': scan.get('processing_status'),
            'num_cells': scan.get('num_cells', 0),
            'max_reflectivity': scan.get('max_reflectivity'),
            'has_tracks': scan.get('has_tracks', False),
            'nexrad_file_name': scan.get('nexrad_file_name'),
        }

        # Load segmentation2d if available
        seg_item_id = scan.get('segmentation2d_item_id')
        if seg_item_id:
            seg_item = self._get_item_by_id(radar, seg_item_id)
            if seg_item:
                seg_path = self.root_dir / radar / seg_item['file_path']
                if seg_path.exists():
                    bundle['segmentation2d'] = xr.open_dataset(seg_path)

        # Load analysis2d cells as DataFrame
        analysis_item_id = scan.get('analysis2d_item_id')
        if analysis_item_id:
            analysis_item = self._get_item_by_id(radar, analysis_item_id)
            if analysis_item:
                analysis_path = self.root_dir / radar / analysis_item['file_path']
                if analysis_path.exists():
                    bundle['cells'] = pd.read_parquet(analysis_path, engine='pyarrow')

        # Get tracks active at this scan time
        run_id = scan.get('run_id')
        if run_id:
            scan_cells = TrackStore(catalog.db_path).get_cells_by_scan(run_id, scan_time_dt)
            bundle['tracks'] = scan_cells.to_dict('records') if not scan_cells.empty else []

        return bundle

    def _get_scan_bundle_fallback(
        self,
        scan_time: datetime,
        radar: str,
        bundle: dict[str, Any]
    ) -> dict[str, Any]:
        """Fallback scan bundle using item queries (for legacy data)."""
        scan_time_str = scan_time.isoformat()
        catalog = self._get_radar_catalog(radar)
        conn = catalog._get_connection()

        # Find segmentation2d at or near this time
        with catalog._lock:
            seg_row = conn.execute("""
                SELECT * FROM items
                WHERE item_type = 'segmentation2d' AND status = 'complete'
                ORDER BY ABS(julianday(scan_time) - julianday(?))
                LIMIT 1
            """, (scan_time_str,)).fetchone()

        if seg_row:
            seg_item = dict(seg_row)
            seg_path = self.root_dir / radar / seg_item['file_path']
            if seg_path.exists():
                bundle['segmentation2d'] = xr.open_dataset(seg_path)
            bundle['scan_time'] = seg_item['scan_time']

        # Find analysis2d
        with catalog._lock:
            analysis_row = conn.execute("""
                SELECT * FROM items
                WHERE item_type = 'analysis2d' AND status = 'complete'
                ORDER BY ABS(julianday(scan_time) - julianday(?))
                LIMIT 1
            """, (scan_time_str,)).fetchone()

        if analysis_row:
            analysis_item = dict(analysis_row)
            analysis_path = self.root_dir / radar / analysis_item['file_path']
            if analysis_path.exists():
                bundle['cells'] = pd.read_parquet(analysis_path, engine='pyarrow')

                # Extract cell tracking info from cells DataFrame
                if 'cell_uid' in bundle['cells'].columns:
                    uids = sorted(
                        bundle['cells']['cell_uid'].dropna().astype(str).unique().tolist()
                    )
                    for uid in uids:
                        bundle['tracks'].append({'cell_uid': uid})

        return bundle

    def _get_item_by_id(self, radar: str, item_id: str) -> dict | None:
        """Get item record by ID."""
        catalog = self._get_radar_catalog(radar)
        conn = catalog._get_connection()

        with catalog._lock:
            row = conn.execute(
                "SELECT * FROM items WHERE item_id = ?",
                (item_id,)
            ).fetchone()

        return dict(row) if row else None

    def list_scan_times(
        self,
        radar: str | None = None,
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """List available scan times from scans table or items fallback.

        Parameters
        ----------
        radar : str, optional
            Radar ID (uses first available if not specified)
        start_time : str or datetime, optional
            Start of time range
        end_time : str or datetime, optional
            End of time range
        limit : int
            Maximum results (default 100)

        Returns
        -------
        DataFrame
            Scan records with columns: scan_id, scan_time, processing_status,
            num_cells, has_tracks (or subset if from items table fallback)
        """
        if not radar:
            radars = self.list_radars()
            if not radars:
                return pd.DataFrame()
            radar = radars[0]

        catalog = self._get_radar_catalog(radar)

        # Convert times
        start_dt = None
        end_dt = None
        if start_time:
            if isinstance(start_time, str):
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            else:
                start_dt = start_time
        if end_time:
            if isinstance(end_time, str):
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            else:
                end_dt = end_time

        # Try scans table first
        try:
            return catalog.list_scans(
                start_time=start_dt,
                end_time=end_dt,
                status='complete',
                limit=limit
            )
        except Exception as e:
            logger.debug(f"scans table not available: {e}")

        # Fallback: query items table for segmentation2d
        return self._list_scan_times_from_items(
            radar, start_dt, end_dt, limit
        )

    def _list_scan_times_from_items(
        self,
        radar: str,
        start_time: datetime | None,
        end_time: datetime | None,
        limit: int
    ) -> pd.DataFrame:
        """Fallback: get scan times from items table."""
        catalog = self._get_radar_catalog(radar)
        conn = catalog._get_connection()

        query = """
            SELECT DISTINCT scan_time, item_id, item_type, status
            FROM items
            WHERE item_type = 'segmentation2d' AND status = 'complete'
        """
        params = []

        if start_time:
            query += " AND scan_time >= ?"
            params.append(start_time.isoformat())
        if end_time:
            query += " AND scan_time <= ?"
            params.append(end_time.isoformat())

        query += " ORDER BY scan_time DESC LIMIT ?"
        params.append(limit)

        with catalog._lock:
            rows = conn.execute(query, params).fetchall()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([dict(row) for row in rows])

    # =========================================================================
    # Streaming Methods
    # =========================================================================

    def stream(
        self,
        sql: str,
        poll_interval: int = 5,
        radar: str | None = None
    ):
        """Stream new results from a SQL query (generator).
        
        Continuously polls for new items where scan_time > last_seen.
        Yields DataFrame batches of new rows.
        
        Parameters
        ----------
        sql : str
            Base SELECT query
        poll_interval : int
            Seconds between polls
        radar : str, optional
            Radar to query
            
        Yields
        ------
        DataFrame
            New rows since last poll
        """
        last_seen_time = None
        
        while True:
            try:
                # Build wrapped query if we have a checkpoint
                if last_seen_time:
                    wrapped_sql = f"""
                        SELECT * FROM ({sql})
                        WHERE scan_time > '{last_seen_time}'
                        ORDER BY scan_time ASC
                    """
                else:
                    wrapped_sql = f"""
                        SELECT * FROM ({sql})
                        ORDER BY scan_time ASC
                        LIMIT 1
                    """
                
                result = self.query(wrapped_sql, radar=radar)
                
                if not result.empty:
                    # Update checkpoint
                    if 'scan_time' in result.columns:
                        last_seen_time = result['scan_time'].max()
                    
                    yield result
                
                time.sleep(poll_interval)
                
            except KeyboardInterrupt:
                logger.info("Stream interrupted by user")
                break
            except Exception as e:
                logger.error(f"Stream error: {e}")
                time.sleep(poll_interval)
    
    def close(self) -> None:
        """Close all connections."""
        if self._duckdb_conn:
            self._duckdb_conn.close()
            self._duckdb_conn = None
        
        for catalog in self._radar_catalogs.values():
            catalog.close()
        
        self._radar_catalogs.clear()
        
        logger.info("DataClient connections closed")
