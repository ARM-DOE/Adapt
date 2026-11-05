# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""AWS S3 NEXRAD Level-II file discovery and download.

Monitors AWS S3 bucket for new NEXRAD radar files and acquires them into the
store in realtime or historical batches. Deduplicates via the catalog (by
source URI) to avoid re-downloading.
"""

import contextlib
import io
import logging
import queue
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from adapt.downloaders import NexradS3

__all__ = ["AwsNexradDownloader"]

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
    `s3://unidata-nexrad-level2/{YYYY}/{MM}/{DD}/{radar}/`
    Example: `s3://unidata-nexrad-level2/2025/03/05/KDIX/KDIX20250305_000310_V06`

    **Deduplication:** Maintains set of known files to avoid re-downloading.
    Safe to restart mid-execution.

    **Queue Communication:** Puts one acquired-scan message per new file on
    result_queue — ``{artifact_id, scan_id, scan_time, queued_at}`` — built by
    the injected acquisition gateway, which commits the raw bytes into the
    store at the source boundary. Downstream processor can begin work
    immediately (streaming architecture).

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
        config,
        result_queue=None,
        acquire=None,
        conn=None,
        clock=None,
        sleeper=None,
    ):
        """Initialize downloader.

        Parameters
        ----------
        config : InternalConfig
            Fully validated runtime configuration.

        result_queue : queue.Queue, optional
            Queue for acquired-scan messages. Processor reads from this queue.
            If None, scans are acquired into the store without notification.

        acquire : acquisition gateway
            Injected by the runtime; commits raw scans into the store and
            registers them for the run. Required — the module never touches
            persistence itself.

        conn : adapt.downloaders.NexradS3, optional
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
        The S3 bucket is public; downloads use anonymous (unsigned) requests
        and require no AWS credentials.
        """

        super().__init__(daemon=True)

        if acquire is None:
            raise ValueError("AwsNexradDownloader requires the acquisition gateway")

        self.config = config
        self.radar = config.downloader.radar
        self.poll_interval_sec = config.downloader.poll_interval_sec
        self.max_fetch_retries = config.downloader.max_fetch_retries
        self.latest_files = config.downloader.latest_files
        self.latest_minutes = config.downloader.latest_minutes
        self.start_time = config.downloader.start_time
        self.end_time = config.downloader.end_time
        # Backpressure with hysteresis: pause downloading when the queue reaches
        # `high`, resume once it has drained to `low`. The processor is never
        # paused — only this thread waits.
        self._queue_high = config.downloader.max_queue_size
        self._queue_low = max(1, int(self._queue_high * config.downloader.queue_resume_fraction))
        self._queue_pauses = 0
        self._queue_paused_seconds = 0.0

        self.result_queue = result_queue
        self._acquire = acquire
        self.conn = conn or NexradS3()
        # injectable time helpers for testing
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleeper or time.sleep

        self._stop_event = threading.Event()
        # Sources already handed to the processor this run (by source URI).
        self._known_files: set[str] = set()
        self._known_files_lock = threading.Lock()
        self._min_file_size = config.downloader.min_file_size
        # Store-external scratch space for in-flight S3 downloads; every file is
        # moved into the store (or deleted) immediately after download.
        self._staging_dir = Path(tempfile.mkdtemp(prefix="adapt_download_"))
        self._last_availability_warning: tuple[object, object] | None = None

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
            except Exception:
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

        logger.debug("Realtime: last %d min (%s to %s)", self.latest_minutes, start, end)

        # Use the full realtime window for availability checks. This avoids
        # false "not found" warnings around UTC midnight when the window spans
        # two dates (yesterday has inventory, today may not yet).
        self._check_radar_available(start, end)

        scans = self._fetch_scans(start, end)
        if not scans:
            return []

        # Keep only latest N
        scans = scans[-self.latest_files :]
        logger.debug("Keeping latest %d scans", len(scans))

        return self._process_scans(scans)

    # ========================================================================
    # Scan fetching and processing
    # ========================================================================

    # ========================================================================
    # Queue backpressure
    # ========================================================================

    def queue_backpressure_stats(self) -> dict:
        """How often, and for how long, the downloader waited on the processor."""
        return {
            "queue_high": self._queue_high,
            "queue_low": self._queue_low,
            "pauses": self._queue_pauses,
            "paused_seconds": round(self._queue_paused_seconds, 1),
        }

    def _wait_for_queue_room(self, poll_seconds: float = 0.5) -> None:
        """Block this thread while the queue is at its high-water mark.

        Returns as soon as the queue has drained to the low-water mark, or a stop
        is requested. Only the downloader waits; the processor keeps consuming.
        The two levels are deliberately apart so a full queue does not turn into
        one download per consumed scan.
        """
        if self.result_queue is None or self.result_queue.qsize() < self._queue_high:
            return
        self._queue_pauses += 1
        t0 = time.monotonic()
        logger.info(
            "Queue at %d/%d — pausing downloads until it drains to %d",
            self.result_queue.qsize(),
            self._queue_high,
            self._queue_low,
        )
        while not self.stopped() and self.result_queue.qsize() > self._queue_low:
            self._sleep(poll_seconds)
        waited = time.monotonic() - t0
        self._queue_paused_seconds += waited
        if not self.stopped():
            logger.info(
                "Queue at %d — resuming downloads after %.0f s", self.result_queue.qsize(), waited
            )

    def _put_until_stopped(self, message: dict, timeout: float = 1.0) -> bool:
        """Put with a bounded wait so a stop request is never ignored.

        The hysteresis pause normally guarantees room, so this only ever waits
        if something else filled the queue. Returns False if stopped first.
        """
        while not self.stopped():
            try:
                self.result_queue.put(message, timeout=timeout)
                return True
            except queue.Full:
                continue
        return False

    def _parse_time_range(self) -> tuple:
        """Parse ISO timestamps to datetime objects."""
        start = datetime.fromisoformat(self.start_time.replace("Z", "+00:00"))
        end = datetime.fromisoformat(self.end_time.replace("Z", "+00:00"))
        return start, end

    def _fetch_scans(self, start: datetime, end: datetime) -> list:
        """Fetch available scans from AWS, retrying transient failures.

        Retries up to ``max_fetch_retries`` times with linear backoff. After the
        final attempt fails, returns ``[]`` so a transient AWS error does not
        stop the downloader — the next poll retries from scratch.
        """
        for attempt in range(1, self.max_fetch_retries + 1):
            try:
                scans = self.conn.get_avail_scans_in_range(start, end, self.radar)
                scans = sorted(scans, key=lambda s: s.scan_time)

                # Filter out MDM files
                scans = [s for s in scans if not s.key.endswith("_MDM")]

                logger.debug("Found %d scans for %s", len(scans), self.radar)
                return scans
            except Exception as e:
                if attempt < self.max_fetch_retries:
                    logger.warning(
                        "Fetch scans attempt %d/%d failed: %s; retrying",
                        attempt,
                        self.max_fetch_retries,
                        e,
                    )
                    self._sleep(attempt)
                else:
                    logger.error(
                        "Failed to fetch scans after %d attempts: %s",
                        self.max_fetch_retries,
                        e,
                    )
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
            dt = datetime(current.year, current.month, current.day, tzinfo=UTC)
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
            warned_key = (start.date(), end.date())
            if self._last_availability_warning == warned_key:
                return
            self._last_availability_warning = warned_key
            logger.warning(
                "⚠️ Radar %s not found in AWS for %s - %s."
                " Downloader will continue polling for scans.",
                self.radar,
                start.date(),
                end.date(),
            )

    def _process_scans(self, scans: list) -> list:
        """Acquire each scan into the store (downloading only when needed) and queue it."""
        acquired = []
        queued = 0
        processed = 0

        for scan in scans:
            if self.stopped():
                break

            processed += 1
            source_uri = str(scan.key)
            with self._known_files_lock:
                already_queued = source_uri in self._known_files
            if already_queued:
                continue

            # Hold here (not after the download) so the queue depth is exactly
            # how far downloads run ahead of processing.
            self._wait_for_queue_room()
            if self.stopped():
                break

            if self._acquire.is_acquired(source_uri):
                message = self._acquire.acquire_existing(source_uri, scan_time=scan.scan_time)
            else:
                local = self._download_scan(scan)
                if local is None:
                    logger.warning("Failed to download: %s", scan.key)
                    continue
                try:
                    message = self._acquire.acquire_file(
                        local, source_uri=source_uri, scan_time=scan.scan_time
                    )
                finally:
                    local.unlink(missing_ok=True)
                acquired.append(source_uri)

            if self.result_queue is not None and not self._put_until_stopped(message):
                break
            with self._known_files_lock:
                self._known_files.add(source_uri)
            queued += 1

        if queued > 0 or acquired:
            logger.info("Queued %d files (%d new downloads)", queued, len(acquired))

        # Mark historical complete when all scans have been attempted
        if self.config.downloader.mode == "historical":
            self._processed_scans = queued
            if processed >= len(scans):
                self._historical_complete.set()
                logger.info("Historical mode complete after processing %d scans", processed)

        return acquired

    def _download_scan(self, scan) -> Path | None:
        """Download one scan into the scratch dir; return the path or None."""
        try:
            # Contain any stdout the conn's download() may emit (we log our own
            # controlled "Downloaded: <name>" below). Logging uses stderr, so this
            # narrow stdout redirect never swallows our own output.
            with contextlib.redirect_stdout(io.StringIO()):
                results = self.conn.download([scan], self._staging_dir, keep_aws_folders=False)
            success = list(results.iter_success())
            if not success:
                return None
            temp_file = Path(success[0].filepath)
            if not temp_file.exists():
                return None
            if temp_file.stat().st_size < self._min_file_size:
                logger.warning("Downloaded file below minimum size, discarding: %s", temp_file.name)
                temp_file.unlink(missing_ok=True)
                return None
            logger.info("Downloaded: %s", temp_file.name)
            return temp_file
        except Exception as e:
            logger.error("Download failed: %s - %s", scan.key, e)
            return None
