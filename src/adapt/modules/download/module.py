"""AWS S3 NEXRAD Level-II file discovery and download.

Monitors AWS S3 bucket for new NEXRAD radar files and downloads them locally
in realtime or historical batches. Deduplicates files to avoid re-downloading.
"""

import threading
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from nexradaws import NexradAwsInterface

from adapt.schemas.directories import get_nexrad_path

if TYPE_CHECKING:
    from adapt.schemas import InternalConfig

__all__ = ['AwsNexradDownloader']

logger = logging.getLogger(__name__)


class AwsNexradDownloader(threading.Thread):
    """Downloads NEXRAD Level-II files from AWS S3 in realtime or historical mode.

    **Realtime Mode:** Continuously monitors S3 for new files within a rolling
    time window (e.g., "last 60 minutes"). Useful for operational nowcasting.
    The latest N files are retained; older files are not re-downloaded.

    **Historical Mode:** Downloads all files within a fixed time range
    (start_time to end_time) then exits. Useful for batch reprocessing and
    research studies.

    **AWS S3 Bucket:** Files stored at
    `s3://noaa-nexrad-level2/{YYYY}/{MM}/{DD}/{radar}/`
    Example: `s3://noaa-nexrad-level2/2025/03/05/KDIX/KDIX20250305_000310_V06`

    **Deduplication:** Maintains set of known files to avoid re-downloading.
    Safe to restart mid-execution.

    **Queue Communication:** Sends filepath to result_queue for each new file.
    Downstream processor can begin work immediately (streaming architecture).

    **File Size Filtering:** Ignores files < 1 KB (corrupted downloads or
    metadata-only files).

    **Thread Safety:** Safe to call status methods while run() is executing.
    Uses locks for shared state (_known_files, historical tracking).

    Example usage (typically called by orchestrator)::

        downloader = AwsNexradDownloader(
            config={
                "radar": "KDIX",
                "output_dir": "/data/nexrad",
                "latest_files": 5,
                "latest_minutes": 60,
                "poll_interval_sec": 30
            },
            result_queue=processor_queue
        )
        downloader.start()
        ...
        downloader.stop()
        downloader.join(timeout=10)
    """

    def __init__(
        self,
        config: "InternalConfig",
        output_dir: Path = None,
        output_dirs: dict = None,
        result_queue=None,
        file_tracker=None,
        conn=None,
        clock=None,
        sleeper=None
    ):
        """Initialize downloader.

        Parameters
        ----------
        config : InternalConfig
            Fully validated runtime configuration.

        output_dir : Path, optional
            DEPRECATED: Use output_dirs instead.
            Legacy parameter for backward compatibility.

        output_dirs : dict, optional
            Output directories dict from setup_output_directories().
            Used for new path structure (RADAR_ID/nexrad/YYYYMMDD/).

        result_queue : queue.Queue, optional
            Queue to push filepaths of downloaded files. Processor reads from
            this queue. If None, no downstream notification (download-only mode).

        file_tracker : FileProcessingTracker, optional
            Optional file processing tracker to record download completion.

        conn : nexradaws.NexradAwsInterface, optional
            AWS S3 connection object. If None, creates new connection.
            Allows injection for testing.

        clock : callable, optional
            Function returning current datetime (for testing). If None, uses
            `datetime.now(timezone.utc)`. Signature: `callable() -> datetime`.

        sleeper : callable, optional
            Function to sleep (for testing). If None, uses `time.sleep`.
            Allows mocking time in tests.

        Notes
        -----
        Requires AWS credentials configured via environment variables or
        ~/.aws/credentials. The S3 bucket is public and requires no auth,
        but credentials can speed up downloads (higher rate limits).
        """

        super().__init__(daemon=True)

        self.config = config
        self.radar = config.downloader.radar
        self.output_dirs = output_dirs
        # Legacy support: if output_dir provided but not output_dirs, use old behavior
        self.output_dir = Path(output_dir) if output_dir else None
        self.poll_interval_sec = config.downloader.poll_interval_sec
        self.latest_files = config.downloader.latest_files
        self.latest_minutes = config.downloader.latest_minutes
        self.start_time = config.downloader.start_time
        self.end_time = config.downloader.end_time
        self.file_tracker = file_tracker

        self.result_queue = result_queue
        self.conn = conn or NexradAwsInterface()
        # injectable time helpers for testing
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleep = sleeper or time.sleep

        self._stop_event = threading.Event()
        self._known_files = set()
        self._known_files_lock = threading.Lock()
        self._min_file_size = config.downloader.min_file_size

        # Historical mode tracking
        self._historical_complete = threading.Event()
        self._expected_scans = 0
        self._processed_scans = 0

        self.name = f"Downloader-{self.radar}"

    # ========================================================================
    # Thread control
    # ========================================================================

    def stop(self):
        """Signal the downloader thread to stop gracefully.
        
        Calling this method sets the internal stop event, which causes the
        `run()` main loop to exit. The thread will finish its current download
        task before stopping (not immediate).
        
        Safe to call from any thread. Can be called multiple times.
        
        Examples
        --------
        >>> downloader = AwsNexradDownloader(config, queue)
        >>> downloader.start()
        >>> time.sleep(60)
        >>> downloader.stop()  # Signal thread to exit
        >>> downloader.join()  # Wait for thread termination
        """
        self._stop_event.set()

    def stopped(self) -> bool:
        """Check if a stop request has been issued.
        
        Returns
        -------
        bool
            True if `stop()` has been called, False otherwise.
        
        Notes
        -----
        This is non-blocking and reflects whether the stop event has been set.
        The thread may still be running even if this returns True (it finishes
        its current task before exiting).
        
        Examples
        --------
        >>> if downloader.stopped():
        ...     print("Stop was requested")
        """
        return self._stop_event.is_set()

    def is_historical_complete(self) -> bool:
        """Check if historical download has finished.
        
        In historical mode, this indicates whether all scans in the time range
        have been downloaded and queued. In realtime mode, this always returns False.
        
        Returns
        -------
        bool
            True if all available scans in the start_time to end_time range
            have been processed.
        
        Notes
        -----
        "Complete" means scans have been QUEUED, not necessarily processed
        by the pipeline. The main loop will exit automatically when this is True.
        
        Examples
        --------
        >>> while not downloader.is_historical_complete():
        ...     downloader.run()  # Wait for completion
        """
        return self._historical_complete.is_set()

    def get_historical_progress(self) -> tuple:
        """Get progress of historical download as (processed, expected) counts.
        
        Returns
        -------
        tuple of (int, int)
            First element: number of scans successfully processed/queued.
            Second element: total number of scans expected in time range.
        
        Notes
        -----
        Only meaningful in historical mode. In realtime mode, expected is always 0.
        Progress is (processed, expected) where processed <= expected.
        This can be used to display download progress to the user.
        
        Examples
        --------
        >>> processed, expected = downloader.get_historical_progress()
        >>> print(f"Downloaded {processed}/{expected} scans")
        
        >>> while not downloader.is_historical_complete():
        ...     processed, expected = downloader.get_historical_progress()
        ...     print(f"Progress: {processed}/{expected}")
        ...     time.sleep(10)
        """
        return self._processed_scans, self._expected_scans

    def run(self):
        """Main thread loop - automatically invoked when thread starts.
        
        This is the primary execution method called by threading.Thread.start().
        Do NOT call directly; use thread.start() instead.
        
        Behavior:
            1. Logs startup with mode (realtime or historical)
            2. Repeatedly calls download_task() at intervals
            3. Handles exceptions in download task (logs and continues)
            4. In historical mode: exits automatically when complete
            5. In realtime mode: runs until stop() is called
            6. Performs interruptible sleep between iterations (can exit quickly)
        
        Notes
        -----
        - Running in a background daemon thread (set at __init__)
        - All exceptions are logged; failures don't crash the thread
        - Sleep is interruptible: responds to stop() and historical completion
          within 2 seconds (default is 2-second sleep chunks)
        - Thread-safe: can call stop() from another thread
        
        Examples
        --------
        >>> downloader = AwsNexradDownloader(config, queue)
        >>> downloader.start()  # Calls run() in background thread
        >>> downloader.join()   # Wait for thread to finish
        """
        logger.info("Starting %s in %s mode", self.name, self.config.downloader.mode)

        while not self.stopped():
            try:
                self._download_task()
            except Exception as e:
                logger.exception("Download task failed")

            # Historical: exit after completion
            if self.config.downloader.mode == "historical" and self.is_historical_complete():
                logger.info("Historical download complete")
                break

            # Sleep between iterations (interruptible)
            self._interruptible_sleep(self.poll_interval_sec)

        logger.info("Stopped %s", self.name)

    def _interruptible_sleep(self, seconds: int):
        """Sleep that can be interrupted by stop event."""
        for _ in range(seconds // 2):
            if self.stopped():
                break
            if self.config.downloader.mode == "historical" and self.is_historical_complete():
                break
            self._sleep(2)


    # ========================================================================
    # Download task - dispatches to realtime or historical
    # ========================================================================

    def _download_task(self) -> list:
        """Execute a single download iteration (realtime or historical).
        
        Dispatches to appropriate download strategy based on mode:
        - Historical: downloads scans in specified date range (one batch)
        - Realtime: downloads latest scans within rolling time window
        
        Returns
        -------
        list of Path
            Paths to newly downloaded files (files that didn't exist before
            this call). Existing files already in the output directory are
            queued but not included in return list.
        
        Notes
        -----
        - Typically called repeatedly in the run() loop
        - Can raise exceptions; the run() loop catches and logs them
        - Files are downloaded to a temporary directory, then moved to
          the final location to ensure atomic writes
        - Only files >= 1 KB are considered valid
        - All queued files (new or existing) are put in result_queue
        - In historical mode: after one complete iteration, completion
          is marked (but loop continues to allow processing time)
        """
        if self.config.downloader.mode == "historical":
            return self._download_historical()
        else:
            return self._download_realtime()

    def _download_historical(self) -> list:
        """Download files for historical time range."""
        start, end = self._parse_time_range()
        logger.info("Historical: %s to %s", start, end)

        self._check_radar_available(start, end)

        scans = self._fetch_scans(start, end)
        if not scans:
            self._historical_complete.set()
            return []

        self._expected_scans = len(scans)
        return self._process_scans(scans)

    def _download_realtime(self) -> list:
        """Download latest files for realtime mode."""
        end = self._clock()
        start = end - timedelta(minutes=self.latest_minutes)

        logger.info("Realtime: last %d min (%s to %s)", self.latest_minutes, start, end)

        self._check_radar_available(end, end)

        scans = self._fetch_scans(start, end)
        if not scans:
            return []

        # Keep only latest N
        scans = scans[-self.latest_files :]
        logger.info("Keeping latest %d scans", len(scans))

        return self._process_scans(scans)

    # ========================================================================
    # Scan fetching and processing
    # ========================================================================

    def _parse_time_range(self) -> tuple:
        """Parse ISO timestamps to datetime objects."""
        start = datetime.fromisoformat(self.start_time.replace("Z", "+00:00"))
        end = datetime.fromisoformat(self.end_time.replace("Z", "+00:00"))
        return start, end

    def _fetch_scans(self, start: datetime, end: datetime) -> list:
        """Fetch available scans from AWS."""
        try:
            scans = self.conn.get_avail_scans_in_range(start, end, self.radar)
            scans = sorted(scans, key=lambda s: s.scan_time)

            # Filter out MDM files
            scans = [s for s in scans if not s.key.endswith("_MDM")]

            logger.info("Found %d scans for %s", len(scans), self.radar)
            return scans
        except Exception as e:
            logger.error("Failed to fetch scans: %s", e)
            return []

    # ========================================================================
    # Radar availability helpers
    # ========================================================================

    def _check_radar_available(self, start: datetime, end: datetime) -> None:
        """Check radar availability and log warnings (best-effort).
        
        Checks if radar is listed in AWS inventory for the given date range.
        This is informational only — it does not block downloads.
        
        For a single date (start == end): checks that date.
        For a date range: checks each day, warns only if explicitly not found on any day.
        
        Uses `self.conn.get_avail_radars(year, month, day)` from NexradAwsInterface.
        Exceptions are silently ignored (inventory may be temporarily unavailable).
        Only warns if we successfully checked and radar is explicitly not present.
        """
        current = start.date()
        end_date = end.date()
        found_on_any_day = False
        all_checks_failed = True  # Track if all checks failed (don't warn in that case)

        while current <= end_date:
            dt = datetime(current.year, current.month, current.day, tzinfo=timezone.utc)
            y = dt.strftime("%Y")
            m = dt.strftime("%m")
            d = dt.strftime("%d")
            try:
                available = self.conn.get_avail_radars(y, m, d)
                all_checks_failed = False  # At least one check succeeded
                if self.radar in available:
                    found_on_any_day = True
                    break
            except Exception as e:
                logger.debug("Availability check failed for %s: %s", dt, e)
            current = current + timedelta(days=1)

        # Warn ONLY if we successfully checked and radar was explicitly not found
        # Don't warn if all checks failed (inventory temporarily unavailable)
        if not found_on_any_day and not all_checks_failed:
            logger.warning(
                "⚠️ Radar %s not found in AWS for %s - %s."
                " Downloader will continue polling for scans.",
                self.radar,
                start.date(),
                end.date(),
            )

    def _process_scans(self, scans: list) -> list:
        """Process list of scans: download if needed, queue only if file exists."""
        new_downloads = []
        queued = 0
        processed = 0

        for scan in scans:
            if self.stopped():
                break

            processed += 1
            local_path = self._get_local_path(scan)
            is_new = False

            # Download if not exists
            if not self._file_exists(local_path):
                if self._download_scan(scan, local_path):
                    is_new = True
                    new_downloads.append(local_path)
                else:
                    logger.warning("Failed to download: %s", scan.key)
                    continue  # Skip queueing if download failed

            # Queue the file ONLY if it now exists on disk
            if self._file_exists(local_path):
                with self._known_files_lock:
                    if local_path not in self._known_files:
                        self._notify_queue(local_path, scan.scan_time, is_new)
                        self._known_files.add(local_path)
                        queued += 1
            else:
                logger.error("File missing after download attempt: %s", local_path)

        logger.info(
            "Processed: %d queued, %d new downloads (attempted %d/%d scans)",
            queued,
            len(new_downloads),
            processed,
            len(scans),
        )

        # Mark historical complete when all scans have been attempted
        if self.config.downloader.mode == "historical":
            self._processed_scans = queued
            if processed >= len(scans):
                self._historical_complete.set()
                logger.info(
                    "Historical mode complete after processing %d scans", processed
                )

        return new_downloads

    def _get_local_path(self, scan) -> Path:
        """Get local path for scan using new structure: base/RADAR_ID/nexrad/YYYYMMDD/filename."""
        filename = Path(scan.key).name

        # Use new path function if output_dirs available
        if self.output_dirs:
            return get_nexrad_path(
                self.output_dirs,
                self.radar,
                filename,
                scan_time=scan.scan_time
            )

        # Legacy fallback: use output_dir directly with old structure
        date_str = scan.scan_time.strftime("%Y%m%d")
        return (self.output_dir / date_str / self.radar / filename).resolve()

    def _file_exists(self, path: Path) -> bool:
        """Check if valid file exists."""
        try:
            return path.exists() and path.stat().st_size >= self._min_file_size
        except:
            return False

    def _download_scan(self, scan, local_path: Path) -> bool:
        """Download single scan to local path."""
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download to temp then move
            # Use output_dirs["base"] if available, otherwise output_dir
            base_dir = self.output_dirs["base"] if self.output_dirs else self.output_dir
            temp_dir = base_dir / "_temp"
            temp_dir.mkdir(exist_ok=True)

            results = self.conn.download([scan], temp_dir, keep_aws_folders=False)
            success = list(results.iter_success())

            if success:
                temp_file = Path(success[0].filepath)
                if temp_file.exists():
                    temp_file.rename(local_path)
                    logger.info("Downloaded: %s", local_path.name)
                    return True

            return False
        except Exception as e:
            logger.error("Download failed: %s - %s", scan.key, e)
            return False

    def _notify_queue(self, path: Path, scan_time: datetime, is_new: bool):
        """Put file notification in result queue."""
        # Keep the original behavior: if there is no result_queue, we don't
        # queue items or attempt to register/mark the file with the tracker.
        if self.result_queue is None:
            return

        try:
            # Register with tracker if available
            tracker = self.file_tracker
            file_id = path.stem
            if tracker:
                tracker.register_file(file_id, self.radar, scan_time, path)
                tracker.mark_stage_complete(file_id, "downloaded", path=path)

            self.result_queue.put(
                {
                    "path": path,
                    "scan_time": scan_time,
                    "radar": self.radar,
                    "file_id": file_id,
                }
            )
        except Exception as e:
            logger.error("Failed to queue notification: %s", e)
