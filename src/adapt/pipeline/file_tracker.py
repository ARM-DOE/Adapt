"""SQLite-based file processing state tracker.

Tracks radar files through pipeline stages (downloaded, regridded, analyzed, plotted).
Enables idempotent processing with stop/restart, progress tracking, and failure recovery.
"""

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List
import threading

__all__ = ['FileProcessingTracker']

logger = logging.getLogger(__name__)


class FileProcessingTracker:
    """Tracks file processing state and progress through pipeline stages.

    Records the processing lifecycle of each NEXRAD file as it moves through
    the pipeline: download → regridding → analysis → visualization. Enables
    resumable processing (stop and restart without reprocessing completed files).

    **Pipeline Stages:**

    1. **Downloaded**: Level-II file exists on disk (from AWS)
    2. **Regridded**: NetCDF file created with Cartesian grid
    3. **Analyzed**: Cell statistics extracted to SQLite database
    4. **Plotted**: Visualization PNG generated

    **Database Schema:**

    SQLite table `radar_file_processing`:

    - file_id: Unique filename (e.g., KDIX20250305_000310_V06)
    - radar: Radar identifier (e.g., KDIX)
    - scan_time: Scan timestamp (ISO format)
    - Status: pending, processing, completed, failed
    - Timestamps: When each stage completed (ISO format)
    - File paths: nexrad_path, gridnc_path, analysis_path, plot_path
    - Metadata: file_size_mb, num_cells, error_message

    **Resumability:**

    If pipeline stops/crashes after marking a stage complete, that stage is
    skipped on restart. Use `reset_failed()` to retry files marked as failed.
    Use `cleanup_deleted_files()` to reprocess files deleted from disk.

    **Thread Safety:**

    All methods are thread-safe via internal locking. Multiple threads can
    check status concurrently.

    **Typical Usage:**

    Called internally by orchestrator and processor. Advanced users can query
    for progress or manually reset failed files::

        tracker = FileProcessingTracker(db_path)
        
        # Check if file needs processing
        if tracker.should_process(file_id, "analyzed"):
            # Process file
            ...
            tracker.mark_stage_complete(file_id, "analyzed", num_cells=42)
        
        # Get stats
        stats = tracker.get_statistics()
        print(f"Processed {stats['completed']} files")
        
        tracker.close()
    """

    def __init__(self, db_path: Path | str):
        """Initialize tracker.

        Parameters
        ----------
        db_path : Path or str
            Path to SQLite database file. Created if doesn't exist.
            Typically: output_dirs/analysis/{radar}_processing_tracker.db
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = None
        self._lock = threading.Lock()

        # Initialize database
        self._init_database()
        logger.info(f" File tracker initialized: {self.db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self._conn.row_factory = sqlite3.Row  # Enable dict-like access
        return self._conn

    def _init_database(self):
        """Create database schema if it doesn't exist."""
        conn = self._get_connection()

        with self._lock:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS radar_file_processing (
                    file_id TEXT PRIMARY KEY,
                    radar TEXT NOT NULL,
                    scan_time TEXT NOT NULL,

                    nexrad_path TEXT,
                    gridnc_path TEXT,
                    analysis_path TEXT,
                    plot_path TEXT,

                    downloaded_at TEXT,
                    regridded_at TEXT,
                    analyzed_at TEXT,
                    plotted_at TEXT,

                    status TEXT DEFAULT 'pending',
                    error_message TEXT,

                    file_size_mb REAL,
                    num_cells INTEGER,

                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_radar_file_processing_radar_id ON radar_file_processing(radar)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_radar_file_processing_status ON radar_file_processing(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_radar_file_processing_scan_time ON radar_file_processing(scan_time)")

            conn.commit()

    def register_file(self, file_id: str, radar: str, scan_time: datetime,
                     nexrad_path: Optional[Path] = None) -> bool:
        """Register a new file for tracking.

        Creates an initial database record for a newly discovered NEXRAD file.
        Called by downloader when a new file is discovered.

        Parameters
        ----------
        file_id : str
            Unique file identifier (e.g., KDIX20250305_000310_V06).
            Typically the Level-II filename without extension.
        radar : str
            Radar identifier (e.g., KDIX, KHTX).
        scan_time : datetime
            Scan timestamp (typically UTC). Used to filter historical ranges.
        nexrad_path : Path, optional
            Path to original NEXRAD Level-II file on disk.
            Used to compute file size for logging.

        Returns
        -------
        bool
            True if file newly registered, False if already in database.
            Returning False does not indicate an error (file might be
            mid-processing or already complete).

        Notes
        -----
        Safe to call multiple times with same file_id (returns False on duplicates).
        """
        conn = self._get_connection()

        with self._lock:
            # Check if already exists
            cursor = conn.execute("SELECT file_id FROM radar_file_processing WHERE file_id = ?", (file_id,))
            if cursor.fetchone():
                return False

            # Calculate file size if path provided
            file_size_mb = None
            if nexrad_path and nexrad_path.exists():
                file_size_mb = nexrad_path.stat().st_size / (1024 * 1024)

            conn.execute("""
                INSERT INTO radar_file_processing
                (file_id, radar, scan_time, nexrad_path, file_size_mb, downloaded_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending')
            """, (
                file_id,
                radar,
                scan_time.isoformat(),
                str(nexrad_path) if nexrad_path else None,
                file_size_mb,
                datetime.now(timezone.utc).isoformat()
            ))
            conn.commit()

            logger.debug(f"Registered file: {file_id}")
            return True

    def mark_stage_complete(self, file_id: str, stage: str,
                          path: Optional[Path] = None,
                          num_cells: Optional[int] = None,
                          error: Optional[str] = None):
        """Mark a pipeline stage as complete or failed for a file.

        Called by downloader, processor, and plotter threads to record progress.
        Enables resumable processing: next run skips stages marked complete.

        Parameters
        ----------
        file_id : str
            File identifier (must be pre-registered via register_file).
        stage : str
            Pipeline stage: 'downloaded', 'regridded', 'analyzed', 'plotted'.
        path : Path, optional
            Path to output file created by this stage (for logging/debugging).
            - 'downloaded': NEXRAD Level-II path
            - 'regridded': Gridded NetCDF path
            - 'analyzed': Analysis NetCDF path
            - 'plotted': PNG plot path
        num_cells : int, optional
            Number of cells detected (for 'analyzed' stage).
        error : str, optional
            Error message if stage failed. If provided, status set to 'failed'
            and future runs will retry this stage.

        Raises
        ------
        ValueError
            If stage is not one of the valid pipeline stages.

        Notes
        -----
        Thread-safe. If called multiple times for the same stage, uses the
        most recent timestamp. Error stages can be reset with reset_failed().
        """
        valid_stages = ['downloaded', 'regridded', 'analyzed', 'plotted']
        if stage not in valid_stages:
            raise ValueError(f"Invalid stage: {stage}. Must be one of {valid_stages}")

        conn = self._get_connection()
        timestamp_col = f"{stage}_at"

        # Map stage to path column
        stage_to_path = {
            'downloaded': 'nexrad_path',
            'regridded': 'gridnc_path',
            'analyzed': 'analysis_path',
            'plotted': 'plot_path'
        }
        path_col = stage_to_path[stage]

        with self._lock:
            # Determine new status
            if error:
                new_status = 'failed'
            elif stage == 'plotted':
                new_status = 'completed'
            else:
                new_status = 'processing'

            # Update record
            if num_cells is not None:
                conn.execute(f"""
                    UPDATE radar_file_processing
                    SET {timestamp_col} = ?,
                        {path_col} = ?,
                        num_cells = ?,
                        status = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE file_id = ?
                """, (
                    datetime.now(timezone.utc).isoformat(),
                    str(path) if path else None,
                    num_cells,
                    new_status,
                    error,
                    datetime.now(timezone.utc).isoformat(),
                    file_id
                ))
            else:
                conn.execute(f"""
                    UPDATE radar_file_processing
                    SET {timestamp_col} = ?,
                        {path_col} = ?,
                        status = ?,
                        error_message = ?,
                        updated_at = ?
                    WHERE file_id = ?
                """, (
                    datetime.now(timezone.utc).isoformat(),
                    str(path) if path else None,
                    new_status,
                    error,
                    datetime.now(timezone.utc).isoformat(),
                    file_id
                ))
            conn.commit()

            logger.debug(f"Marked {stage} complete: {file_id}")

    def get_file_status(self, file_id: str) -> Optional[Dict]:
        """Get complete processing status for a file.

        Parameters
        ----------
        file_id : str
            File identifier

        Returns
        -------
        dict or None
            Dict with file metadata and stage completion status:
            - `file_id`, `radar`, `scan_time`
            - `status`: 'pending', 'processing', 'completed', 'failed'
            - `error_message`: Error details if failed
            - `file_size_mb`: Original NEXRAD file size
            - `num_cells`: Number of cells if analyzed
            - Timestamps: `downloaded_at`, `regridded_at`, `analyzed_at`, `plotted_at`
            - File paths: `nexrad_path`, `gridnc_path`, `analysis_path`, `plot_path`

            None if file_id not found in database.
        """
        conn = self._get_connection()

        with self._lock:
            cursor = conn.execute("""
                SELECT * FROM radar_file_processing WHERE file_id = ?
            """, (file_id,))
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

    def get_pending_files(self, stage: Optional[str] = None,
                         radar: Optional[str] = None,
                         limit: Optional[int] = None) -> List[Dict]:
        """Get files awaiting processing at a specific stage.

        Used by downloader/processor/plotter to find files needing work.

        Parameters
        ----------
        stage : str, optional
            Filter by processing stage:
            - 'regridded': files downloaded but not regridded
            - 'analyzed': files regridded but not analyzed
            - 'plotted': files analyzed but not plotted
            
            If None, returns files with any pending stage.

        radar : str, optional
            Filter by radar ID (e.g., "KDIX").

        limit : int, optional
            Max files to return (for batching). Default: None (all pending).

        Returns
        -------
        list of dict
            List of file records matching criteria, ordered by scan_time (oldest first).
        """
        conn = self._get_connection()

        # Build query based on stage
        if stage == 'regridded':
            condition = "downloaded_at IS NOT NULL AND regridded_at IS NULL"
        elif stage == 'analyzed':
            condition = "regridded_at IS NOT NULL AND analyzed_at IS NULL"
        elif stage == 'plotted':
            condition = "analyzed_at IS NOT NULL AND plotted_at IS NULL"
        else:
            condition = "status != 'completed' AND status != 'failed'"

        query = f"SELECT * FROM radar_file_processing WHERE {condition}"
        params = []

        if radar:
            query += " AND radar = ?"
            params.append(radar)

        query += " ORDER BY scan_time"

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        with self._lock:
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_statistics(self, radar: Optional[str] = None) -> Dict:
        """Get summary statistics for processing progress.

        Parameters
        ----------
        radar : str, optional
            Filter to specific radar. If None, returns all radars.

        Returns
        -------
        dict
            Summary statistics:
            - `total`: Total files registered
            - `completed`: Files fully processed (through plotting)
            - `pending`: Files awaiting any stage
            - `failed`: Files with errors
            - `total_cells`: Sum of cells across all analyzed files
            - `radar`: Filtered radar (if specified)

        Notes
        -----
        Used by orchestrator to log progress every 30 seconds.
        """
        conn = self._get_connection()

        where_clause = f"WHERE radar = '{radar}'" if radar else ""

        with self._lock:
            cursor = conn.execute(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(downloaded_at) as downloaded,
                    COUNT(regridded_at) as regridded,
                    COUNT(analyzed_at) as analyzed,
                    COUNT(plotted_at) as plotted,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(num_cells) as total_cells
                FROM radar_file_processing
                {where_clause}
            """)
            row = cursor.fetchone()
            return dict(row) if row else {}

    def should_process(self, file_id: str, stage: str) -> bool:
        """Check if a file needs processing at a given stage.

        Used by processor/plotter threads to determine if a stage should be skipped.

        Parameters
        ----------
        file_id : str
            File identifier
        stage : str
            Stage to check: 'regridded', 'analyzed', 'plotted'

        Returns
        -------
        bool
            True if file should be processed (stage incomplete), False if already done.
        """
        status = self.get_file_status(file_id)

        if not status:
            # File not registered, should process
            return True

        # Check if stage already completed
        timestamp_col = f"{stage}_at"
        return status.get(timestamp_col) is None

    def reset_failed(self, radar: Optional[str] = None):
        """Reset all failed files to pending for retry.

        Useful for recovery after fixing errors (e.g., config changes, bug fixes).
        Clears error_message and marks status='pending' so files reprocess.

        Parameters
        ----------
        radar : str, optional
            Filter to specific radar. If None, resets all radars.

        Notes
        -----
        Does not delete output files. On next run, stages will be skipped based
        on existing output files, not status. Use `cleanup_deleted_files()` if
        files were deleted and should be fully reprocessed.
        """
        conn = self._get_connection()

        with self._lock:
            if radar:
                conn.execute("""
                    UPDATE radar_file_processing
                    SET status = 'pending', error_message = NULL, updated_at = ?
                    WHERE status = 'failed' AND radar = ?
                """, (datetime.now(timezone.utc).isoformat(), radar))
            else:
                conn.execute("""
                    UPDATE radar_file_processing
                    SET status = 'pending', error_message = NULL, updated_at = ?
                    WHERE status = 'failed'
                """, (datetime.now(timezone.utc).isoformat(),))
            conn.commit()

            logger.info(f"Reset failed files to pending")

    def cleanup_deleted_files(self, radar: Optional[str] = None):
        """Remove database records for files deleted from disk.

        Useful after clearing output directories. On next run, these files
        will be re-downloaded and reprocessed.

        Parameters
        ----------
        radar : str, optional
            Filter to specific radar. If None, cleans all radars.

        Notes
        -----
        Only checks NEXRAD Level-II paths. Does not verify gridded/analysis
        NetCDF or PNG files (those are considered intermediate).
        """
        conn = self._get_connection()

        with self._lock:
            # Get all files
            where_clause = f"WHERE radar = '{radar}'" if radar else ""
            cursor = conn.execute(f"""
                SELECT file_id, nexrad_path FROM radar_file_processing {where_clause}
            """)

            deleted = []
            for row in cursor.fetchall():
                file_id = row['file_id']
                nexrad_path = row['nexrad_path']

                if nexrad_path and not Path(nexrad_path).exists():
                    deleted.append(file_id)

            # Delete records
            if deleted:
                placeholders = ','.join('?' * len(deleted))
                conn.execute(f"""
                    DELETE FROM radar_file_processing WHERE file_id IN ({placeholders})
                """, deleted)
                conn.commit()

                logger.info(f"Cleaned up {len(deleted)} deleted file(s)")

    def close(self):
        """Close database connection.

        Called automatically by orchestrator.stop(). Safe to call multiple times.
        """
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


