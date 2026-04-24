"""Multi-threaded pipeline orchestration.

Coordinates downloader and processor threads with queue-based inter-thread
communication. Manages lifecycle, monitoring, and graceful shutdown.

Note: Plotting is handled by a separate PlotConsumer thread that polls
the DataRepository independently. This decoupling ensures processing
is not blocked by visualization and validates repository API integrity.
"""

import queue
import time
import logging
from pathlib import Path
from typing import Optional, Dict, TYPE_CHECKING

from adapt.modules.acquisition.module import AwsNexradDownloader
from adapt.runtime.processor import RadarProcessor
from adapt.runtime.file_tracker import FileProcessingTracker
from adapt.persistence import DataRepository

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig

__all__ = ['PipelineOrchestrator']

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Manages the multi-threaded radar processing pipeline.

    This is the main entry point for running ``adapt``. It coordinates two
    worker threads (downloader, processor) using queues for inter-thread
    communication. The orchestrator handles startup, monitoring, and graceful
    shutdown of the processing pipeline.

    **Pipeline Architecture:**

    1. **Downloader Thread**: Discovers and downloads NEXRAD Level-II files
       from AWS in realtime or historical mode.

    2. **Processor Thread**: Processes downloaded files through the full
       scientific pipeline:
       - Load and regrid Level-II data to Cartesian grid
       - Segment cells using configurable threshold method
       - Compute motion projections (frame 2 and beyond)
       - Extract cell-level statistics
       - Persist segmentation to NetCDF and statistics to SQLite
       - Register all artifacts in DataRepository


    **Modes:**

    - **Realtime**: Polls for latest files within a rolling time window
      (e.g., "files from last 60 minutes"). Useful for operational monitoring.

    - **Historical**: Downloads all files within a fixed time range
      (start_time to end_time). Useful for batch reprocessing and research.

    **Queue Management:**

    Inter-thread queues have configurable size limits (default 100 items).
    Larger queues enable higher throughput but use more memory. Smaller
    queues provide backpressure (slow down downloader if processor falls behind).

    **File Tracking:**

    The FileProcessingTracker SQLite database records the state of each file
    (downloaded, regridded, analyzed, plotted, or failed). This enables:
    - Resumable processing (restart without reprocessing completed files)
    - Progress tracking
    - Failure recovery and debugging

    **Logging:**

    All output goes to both console and log file (logs/{radar}_pipeline.log).
    Log level controlled via config: "DEBUG", "INFO", "WARNING", "ERROR".

    Example usage::

        from adapt.runtime.orchestrator import PipelineOrchestrator
        
        config = {
            "mode": "realtime",
            "downloader": {"radar": "KDIX", ...},
            "output_dirs": {...},
            ...
        }
        
        orch = PipelineOrchestrator(config)
        orch.start(max_runtime=60)  # Run for 60 minutes then stop
    """

    def __init__(
        self,
        config: "InternalConfig",
        max_queue_size: int = 20,
        close_repository_on_stop: bool = True,
    ):
        """Initialize orchestrator with fully resolved runtime configuration.

        Parameters
        ----------
        config : InternalConfig
            Fully resolved runtime configuration from init_runtime_config().
            Already contains all directory paths, run ID, and validated settings.
            
        max_queue_size : int, optional
            Maximum size of inter-thread communication queues (default: 100).
        """
        self.config = config
        self.max_queue_size = max_queue_size

        # Queue for downloader -> processor communication
        self.downloader_queue = queue.Queue(maxsize=max_queue_size)

        # Extract output_dirs from validated config
        self.output_dirs = {k: Path(v) for k, v in config.output_dirs.items()}

        # Threads (created in start())
        self.downloader = None
        self.processor = None

        # File tracking (initialized in _setup_logging)
        self.tracker = None

        # DataRepository (initialized in start()) - use run_id from config or generate
        self.run_id = config.run_id
        self.repository: Optional[DataRepository] = None

        # Lifecycle state
        self._stop_event = False
        self._interrupted = False  # Track user interrupt (Ctrl+C) vs normal completion
        self._start_time = None
        self._max_duration = None
        self._close_repository_on_stop = close_repository_on_stop

    def _setup_logging(self):
        """Configure logging and file tracking systems.

        Initializes root logger with file and console handlers, creates output
        directories, and sets up FileProcessingTracker for pipeline state management.
        Log level and paths derived from config.
        """
        log_level = getattr(logging, self.config.logging.level.upper(), logging.INFO)

        # Get log path from output_dirs
        radar = self.config.downloader.radar
        log_dir = Path(self.output_dirs["logs"])
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"pipeline_{radar}.log"

        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Clear existing handlers and add new ones
        root = logging.getLogger()
        root.setLevel(log_level)
        for handler in root.handlers[:]:
            root.removeHandler(handler)

        # File handler
        fh = logging.FileHandler(log_path)
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        root.addHandler(fh)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(formatter)
        root.addHandler(ch)

        logger.debug("Logging: level=%s, file=%s", log_level, log_path)

        # Initialize file tracker (stored in RADAR_ID/analysis/) - renamed to processing_tracker
        tracker_dir = Path(self.output_dirs["base"]) / radar / "analysis"
        tracker_dir.mkdir(parents=True, exist_ok=True)
        tracker_path = tracker_dir / f"{radar}_processing_tracker.db"
        self.tracker = FileProcessingTracker(tracker_path)
        logger.debug("Processing tracker: %s", tracker_path)

    def start(self, max_runtime: Optional[int] = None):
        """Start the pipeline and run until completion or user interrupt.

        This is a blocking call that starts the downloader and processor
        threads, then enters a monitoring loop. The loop logs status every 30 seconds
        and handles mode-specific exit conditions.

        **Realtime Mode:** Runs until you press Ctrl+C or max_runtime is exceeded.

        **Historical Mode:** Automatically exits when all files within the
        start_time/end_time range are queued, processed, and plotted.

        All output (console + file) logged to logs/{radar}_pipeline.log
        at the level specified in config["logging"]["level"].

        Parameters
        ----------
        max_runtime : int, optional
            Maximum runtime in minutes (realtime mode only).
            If None, runs until KeyboardInterrupt (Ctrl+C).
            Ignored in historical mode (uses file completion instead).

        Raises
        ------
        KeyboardInterrupt
            User pressed Ctrl+C. Pipeline stops gracefully.

        Notes
        -----
        To stop the pipeline gracefully, press Ctrl+C. Do NOT force-kill.
        The stop() method is called automatically to close threads and save results.

        Examples
        --------
        Run for 60 minutes in realtime mode::

            orch = PipelineOrchestrator(config)
            orch.start(max_runtime=60)

        Run historical mode until all files processed::

            orch = PipelineOrchestrator(config)  # mode="historical" in config
            orch.start()  # Runs until all files between start_time/end_time processed
        """
        self._setup_logging()

        # Initialize DataRepository
        radar = self.config.downloader.radar
        self.repository = DataRepository(
            run_id=self.run_id,
            base_dir=self.output_dirs["base"],
            radar=radar,
            config=self.config
        )

        self._start_time = time.time()
        self._max_duration = max_runtime * 60 if max_runtime else None

        logger.info(
            "Pipeline started | run=%s radar=%s mode=%s",
            self.run_id, radar, self.config.mode.upper()
        )

        # Start Downloader thread
        self.downloader = AwsNexradDownloader(
            config=self.config,
            output_dirs=self.output_dirs,
            result_queue=self.downloader_queue,
            file_tracker=self.tracker,
        )
        self.downloader.start()

        # Start Processor thread
        self.processor = RadarProcessor(
            input_queue=self.downloader_queue,
            config=self.config,
            output_dirs=self.output_dirs,
            file_tracker=self.tracker,
            repository=self.repository,
        )
        self.processor.start()

        mode = self.config.mode
        logger.debug("Pipeline running in %s mode. Press Ctrl+C to stop.", mode.upper())

        try:
            self._main_loop(mode)
        except KeyboardInterrupt:
            logger.info("\nShutdown signal received (Ctrl+C)")
            self._interrupted = True
        finally:
            self.stop()

    def _main_loop(self, mode: str):
        """Monitoring loop: check exit conditions and log status."""
        last_status_time = time.time()

        while True:
            # 0. Honour external stop() call (e.g. from CLI after SIGTERM/SIGINT)
            if self._stop_event:
                break

            # 1. Historical completion check (must run before downloader death check)
            if mode == "historical":
                if self._check_historical_complete():
                    break

            # 2. Check for thread failures or self-stops (e.g. ContractViolation)
            if self.processor.stopped():
                logger.critical("Processor has stopped (likely due to contract violation). Exiting.")
                break

            if not self.processor.is_alive():
                logger.critical("Processor thread died unexpectedly. Exiting.")
                break

            if not self.downloader.is_alive():
                if mode == "historical" and self.downloader.is_historical_complete():
                    logger.info("Downloader exited after historical completion.")
                    break
                logger.critical("Downloader thread died unexpectedly. Exiting.")
                break

            # 3. Mode-specific exit conditions
            if mode == "realtime" and self._max_duration:
                elapsed = time.time() - self._start_time
                if elapsed > self._max_duration:
                    logger.info("Max duration reached")
                    break

            # 4. Status logging (every 30s)
            if time.time() - last_status_time > 30:
                self._log_status()
                last_status_time = time.time()

            time.sleep(1)

    def _check_historical_complete(self) -> bool:
        """Check if historical mode is complete. Returns True to exit."""
        downloader_complete = self.downloader.is_historical_complete()

        if not downloader_complete:
            return False

        processed, expected = self.downloader.get_historical_progress()
        logger.info("Downloader complete: %d/%d files queued", processed, expected)

        # Stop downloader explicitly to signal thread termination
        self.downloader.stop()
        logger.info("Stopping downloader thread...")
        self.downloader.join(timeout=5)
        if self.downloader.is_alive():
            logger.warning("Downloader thread did not stop cleanly")

        # Wait for processor queue to drain
        self._drain_queue(self.downloader_queue, "processor")

        # Now stop processor thread
        logger.info("Stopping processor thread...")
        if self.processor and self.processor.is_alive():
            self.processor.stop()
            self.processor.join(timeout=10)
            if self.processor.is_alive():
                logger.warning("Processor thread did not stop cleanly")

        logger.info("Historical mode complete")
        return True

    def _drain_queue(self, q: queue.Queue, name: str, timeout: int = 300):
        """Wait for queue to drain with timeout."""
        wait_count = 0
        start_time = time.time()
        last_size = q.qsize()
        
        while q.qsize() > 0:
            current_size = q.qsize()
            if current_size == last_size:
                wait_count += 1
            else:
                wait_count = 0  # Reset if progress is being made
                last_size = current_size
            
            logger.info("Waiting for %s queue: %d remaining", name, current_size)
            time.sleep(1)
            
            # Check timeout both by iteration count and elapsed time
            if wait_count > timeout // 5 or (time.time() - start_time) > timeout:
                logger.warning("%s queue drain timeout (%d/%d seconds)", 
                              name, int(time.time() - start_time), timeout)
                break

    def stop(self):
        """Stop the pipeline gracefully and finalize all results.

        Called automatically when start() exits (either by user interrupt or
        mode-specific completion). Safe to call multiple times.

        **Operations:**

        1. Signals all worker threads to stop
        2. Waits up to 5 seconds for each thread to finish
        3. Saves accumulated cell statistics to SQLite database
        4. Generates final summary statistics (total cells, completion times)
        5. Closes database connections
        6. Logs pipeline runtime and file processing statistics

        Notes
        -----
        This method should complete within ~20 seconds. If a thread does not
        respond to the stop signal within its timeout, a warning is logged but
        the shutdown continues (graceful degradation).

        Files processed before the final save() will have their statistics
        in the SQLite database. The database is safe to query even while the
        pipeline is running (uses WAL mode).
        """
        if self._stop_event:
            return

        self._stop_event = True

        # Stop threads
        for name, thread in [("Downloader", self.downloader),
                              ("Processor", self.processor)]:
            if thread and thread.is_alive():
                thread.stop()
                thread.join(timeout=5)
                if thread.is_alive():
                    logger.warning("%s did not stop cleanly", name)

        # Save results
        if self.processor:
            self.processor.save_results()
            self.processor.close_database()

        # Finalize repository
        if self.repository:
            final_status = "cancelled" if self._interrupted else "completed"
            self.repository.finalize_run(final_status)
            if self._close_repository_on_stop:
                self.repository.close()

        # Summary
        elapsed = time.time() - self._start_time if self._start_time else 0
        if self.tracker:
            stats = self.tracker.get_statistics()
            total_cells = stats.get('total_cells', 0) or 0
            logger.info(
                "Pipeline stopped. Runtime: %.1fs | files=%d completed=%d cells=%d",
                elapsed,
                stats.get('total', 0),
                stats.get('completed', 0),
                total_cells,
            )
            self.tracker.close()
        else:
            logger.info("Pipeline stopped. Runtime: %.1fs", elapsed)

    def _log_status(self):
        """Log current pipeline status."""
        mode = self.config.mode
        hist_status = ""
        if mode == "historical" and self.downloader:
            processed, expected = self.downloader.get_historical_progress()
            hist_status = f" [{processed}/{expected}]"

        logger.info(
            "Status: D=%s P=%s Q=%d%s",
            "UP" if self.downloader and self.downloader.is_alive() else "DOWN",
            "UP" if self.processor and self.processor.is_alive() else "DOWN",
            self.downloader_queue.qsize(),
            hist_status
        )

    def close_repository(self) -> None:
        """Close repository connection if present."""
        if self.repository:
            self.repository.close()
