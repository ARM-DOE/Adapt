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

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import duckdb
import pandas as pd
import xarray as xr

from adapt.core.registry import RepositoryRegistry
from adapt.core.radar_catalog import RadarCatalog

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
    
    def __init__(self, repository_root: Union[str, Path]):
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
        self._duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None
        
        # Cache of radar catalogs
        self._radar_catalogs: Dict[str, RadarCatalog] = {}
        
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
    # Discovery Methods
    # =========================================================================
    
    def list_runs(self, radar: Optional[str] = None) -> pd.DataFrame:
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
    
    def list_radars(self) -> List[str]:
        """List all registered radars.
        
        Returns
        -------
        list of str
            Radar IDs
        """
        radars_df = self.registry.list_radars()
        return radars_df['radar'].tolist() if not radars_df.empty else []
    
    def item_types(self) -> List[str]:
        """List registered item types.
        
        Returns
        -------
        list of str
            Item type names (e.g., ['analysis2d', 'gridded3d', ...])
        """
        return self.registry.list_item_types()
    
    def fields(self, item_type: str, radar: Optional[str] = None) -> List[str]:
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
    
    def status(self, run_id: Optional[str] = None, radar: Optional[str] = None) -> Dict:
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
        radar: Optional[str] = None
    ) -> Union[pd.DataFrame, xr.Dataset]:
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
    
    def query(self, sql: str, radar: Optional[str] = None) -> pd.DataFrame:
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
    # Streaming Methods
    # =========================================================================
    
    def stream(
        self,
        sql: str,
        poll_interval: int = 5,
        radar: Optional[str] = None
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
