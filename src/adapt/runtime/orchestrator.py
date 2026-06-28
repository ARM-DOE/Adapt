# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Multi-threaded pipeline orchestration.

Coordinates downloader and processor threads with queue-based inter-thread
communication. Manages lifecycle, monitoring, and graceful shutdown.

Note: Plotting is handled by a separate PlotConsumer thread that polls
the DataRepository independently. This decoupling ensures processing
is not blocked by visualization and validates repository API integrity.
"""

import logging
import queue
import random
import sys
import time
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from adapt.contracts.execution_history import RunStart, RunSummary
from adapt.persistence import DataRepository
from adapt.runtime.console_status import ConsoleStatus
from adapt.runtime.file_tracker import FileProcessingTracker
from adapt.runtime.history_handler import HistoryLogHandler
from adapt.runtime.logging_setup import configure_logging
from adapt.runtime.observability import ObsSettings, build_observability
from adapt.runtime.processor import RadarProcessor
from adapt.runtime.provenance import capture_provenance, config_hash
from adapt.runtime.run_reporter import RunReporter
from adapt.runtime.sources import source_registry

if TYPE_CHECKING:
    from adapt.configuration.schemas.internal import InternalConfig
    from adapt.contracts.observability import Observability
    from adapt.contracts.source import ScanSource

__all__ = ["PipelineOrchestrator"]

logger = logging.getLogger(__name__)

# Span names that wrap modules rather than being modules themselves; excluded from
# the per-module summary breakdown (they share the module_duration_seconds histogram).
_WRAPPER_SPANS = frozenset({"pipeline", "scan"})

# Monitor-loop cadence; also the live status-line tick (spinner + elapsed) interval.
_STATUS_TICK_SECONDS = 0.5


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
        self.downloader_queue: queue.Queue[object] = queue.Queue(maxsize=max_queue_size)

        # Extract output_dirs from validated config
        assert config.output_dirs is not None
        self.output_dirs = {k: Path(v) for k, v in config.output_dirs.items()}

        # Threads (created in start())
        self.downloader: ScanSource | None = None
        self.processor: RadarProcessor | None = None

        # File tracking (initialized in _setup_logging)
        self.tracker: FileProcessingTracker | None = None

        # DataRepository (initialized in start()) - use run_id from config or generate
        self.run_id = config.run_id
        self.repository: DataRepository | None = None

        # Lifecycle state
        self._stop_event = False
        self._stop_requested = False  # external break request (separate from the stop() guard)
        self._interrupted = False  # Track user interrupt (Ctrl+C) vs normal completion
        self._start_time: float | None = None
        self._max_duration: float | None = None
        self._close_repository_on_stop = close_repository_on_stop

        # Telemetry (built in start()); kept here so stop() is safe before start().
        self._obs: Observability | None = None
        self._root_span: AbstractContextManager | None = None
        self._root_trace_id = ""
        self._history_handler: HistoryLogHandler | None = None
        self._reporter: RunReporter | None = None
        self._status: ConsoleStatus | None = None

    def _obs_settings(self) -> ObsSettings:
        """Translate the resolved logging/observability config into ObsSettings."""
        lc = self.config.logging
        return ObsSettings(
            enabled=lc.enabled,
            traces=lc.traces,
            metrics=lc.metrics,
            json_logs=lc.json_logs,
            console_logs=lc.console_logs,
            level=lc.level,
            console_level=lc.console_level,
            progress_every=lc.progress_every,
        )

    def _build_observability(self) -> "Observability":
        """Build the telemetry provider from the resolved config.

        Injects production clocks/rng here (the only place wall-clock reads are
        legal for telemetry). Trace ids are random per run.
        """
        return build_observability(
            self._obs_settings(),
            clock=time.perf_counter,
            wall_clock=lambda: datetime.now(UTC),
            rng=random.Random(),
        )

    def _enabled_module_names(self) -> tuple[str, ...]:
        """All enabled module names — in-pipeline AND post-persistence (phase-3).

        The header is built from this, so phase-3 enrichment modules (e.g.
        cell_volume_stats) are surfaced, not just the phase 0-2 pipeline.
        """
        if not self.processor:
            return ()
        mods = (*self.processor._pipeline_modules, *self.processor._post_modules)
        return tuple(m.name for m in mods)

    def _record_run_start(self, radar: str) -> None:
        """Open the execution-history run record and print the console run header."""
        prov = capture_provenance()
        modules = self._enabled_module_names()
        start = RunStart(
            run_id=self.run_id or "",
            pipeline=self.config.source,
            pipeline_version=prov.software_version,
            site=radar,
            dataset=radar,
            instrument="NEXRAD",
            mode=self.config.mode,
            start_time=datetime.now(UTC),
            configuration_hash=config_hash(self.config.model_dump_json()),
            configuration_file=str(
                self.repository.catalog.radar_dir / f"config_run_{self.run_id}.json"
            )
            if self.repository
            else "",
            provenance=prov,
            enabled_modules=modules,
        )
        assert self.repository is not None
        assert self._reporter is not None
        self.repository.history.start_run(start)
        self._reporter.header(start)

    def _finalize_history(self) -> None:
        """Print the run summary, then persist history.

        The console summary is built from in-memory telemetry + the drained handler
        counts and printed FIRST, so it never depends on a database write succeeding
        (a DB failure during shutdown is logged loudly but cannot hide the summary).
        """
        if self._obs is None or self.repository is None:
            return
        warnings: list = []
        errors: list = []
        if self._history_handler is not None:
            warnings, errors = self._history_handler.drain()

        summary = self._build_run_summary(
            "cancelled" if self._interrupted else "success",
            warnings=len(warnings),
            errors=len(errors),
        )
        if self._reporter is not None:
            self._reporter.summary(summary)

        # Persist after the user-facing summary is out. Failures here are reported
        # loudly but must not abort the orderly shutdown.
        run_id = self.run_id or ""
        try:
            if warnings:
                self.repository.history.record_warnings(run_id, warnings)
            if errors:
                self.repository.history.record_errors(run_id, errors)
            self.repository.history.finalize_run(summary)
        except Exception:
            logger.exception("Failed to persist execution history on shutdown")

    def _build_run_summary(self, status: str, *, warnings: int, errors: int) -> RunSummary:
        """Aggregate the end-of-run summary from in-memory telemetry metrics.

        warnings/errors are passed in (drained from the history handler) so the
        summary never reads the database — keeping it robust during shutdown.
        """
        assert self._obs is not None
        m = self._obs.metrics
        scan_times = m.histogram_values("scan_processing_time")
        totals = m.histogram_totals_by_label("module_duration_seconds", "stage")
        counts = m.histogram_counts_by_label("module_duration_seconds", "stage")
        # "pipeline" (root) and "scan" are wrapper spans, not modules — exclude them
        # from the per-module breakdown so the table shows only real stages.
        module_stats = tuple(
            (name, counts.get(name, 0), total)
            for name, total in sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
            if name not in _WRAPPER_SPANS
        )
        slowest = tuple((name, total) for name, _calls, total in module_stats[:3])
        duration = (time.time() - self._start_time) if self._start_time else 0.0
        return RunSummary(
            run_id=self.run_id or "",
            status=status,
            end_time=datetime.now(UTC),
            duration_seconds=duration,
            files_processed=int(m.counter_total("files_processed_total")),
            scans_processed=len(scan_times),
            objects_detected=int(m.counter_total("cells_detected_total")),
            warnings=warnings,
            errors=errors,
            average_scan_time=(sum(scan_times) / len(scan_times)) if scan_times else 0.0,
            maximum_scan_time=max(scan_times) if scan_times else 0.0,
            slowest_stages=slowest,
            module_stats=module_stats,
            failures=int(m.counter_total("errors_total")),
        )

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
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
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

    def _create_source(self):
        """Resolve and construct the ingress source named by ``config.source``.

        Sources are swappable plugins (download, local directory, …) registered in
        adapt.runtime.sources. All are constructed with the same signature.
        """
        source_cls = source_registry.get(self.config.source)
        return source_cls(
            config=self.config,
            output_dirs=self.output_dirs,
            result_queue=self.downloader_queue,
            file_tracker=self.tracker,
        )

    def start(self, max_runtime: int | None = None):
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
        assert self.run_id is not None
        self.repository = DataRepository(
            run_id=self.run_id,
            base_dir=self.output_dirs["base"],
            radar=radar,
            config=self.config,
        )

        # Build telemetry and open the root pipeline span. The root trace id is handed
        # to the processor thread (contextvars do not cross threads) so the whole run
        # shares one trace.
        self._obs = self._build_observability()
        self._root_span = self._obs.span(
            "pipeline", pipeline_id=self.run_id or "", dataset_id=radar
        )
        self._root_span.__enter__()
        self._root_trace_id = self._obs.current().trace_id

        # Live status line (spinner) — TTY only; self-disables when piped or quiet.
        settings = self._obs_settings()
        self._status = ConsoleStatus(
            sys.stderr,
            enabled=settings.console_logs and sys.stderr.isatty(),
            clock=time.monotonic,
        )

        # Structured logging (JSON file + quiet console) + capture warnings/errors.
        log_path = Path(self.output_dirs["logs"]) / f"pipeline_{radar}.log"
        configure_logging(settings, log_path, console_status=self._status)
        self._history_handler = HistoryLogHandler()
        logging.getLogger().addHandler(self._history_handler)
        self._reporter = RunReporter()

        self._start_time = time.time()
        self._max_duration = max_runtime * 60 if max_runtime else None

        logger.info(
            "Pipeline started | run=%s radar=%s mode=%s",
            self.run_id,
            radar,
            self.config.mode.upper(),
        )

        # Start the ingress source (resolved by name from config)
        self.downloader = self._create_source()
        self.downloader.start()

        # Start Processor thread
        self.processor = RadarProcessor(
            input_queue=self.downloader_queue,
            config=self.config,
            output_dirs=self.output_dirs,
            file_tracker=self.tracker,
            repository=self.repository,
            observability=self._obs,
            root_trace_id=self._root_trace_id,
            reporter=self._reporter,
        )
        self.processor.start()

        # Open the execution-history run record and print the one-shot console header.
        self._record_run_start(radar)

        mode = self.config.mode
        logger.debug("Pipeline running in %s mode. Press Ctrl+C to stop.", mode.upper())

        try:
            self._main_loop(mode)
        finally:
            self.stop()

    def _main_loop(self, mode: str):
        """Monitoring loop: check exit conditions and log status."""
        assert self.downloader is not None
        assert self.processor is not None
        assert self._start_time is not None
        last_status_time = time.time()

        while True:
            # 0. Honour an external stop request or a prior stop() (e.g. from the CLI
            #    after SIGTERM/SIGINT). request_stop() lets the orchestrator's OWN thread
            #    run finalize+summary, instead of a daemon that the process kills on exit.
            if self._stop_requested or self._stop_event:
                break

            # 1. Historical completion check (must run before downloader death check)
            if mode == "historical" and self._check_historical_complete():
                break

            # 2. Check for thread failures or self-stops (e.g. ContractViolation)
            if self.processor.stopped():
                logger.critical(
                    "Processor has stopped (likely due to contract violation). Exiting."
                )
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

            # 4. Status logging (every 30s, file only) + live status line (every tick)
            if time.time() - last_status_time > 30:
                self._log_status()
                last_status_time = time.time()

            if self._status is not None:
                self._status.set(self.processor.current_activity() or "waiting for next scan")
                self._status.tick()

            time.sleep(_STATUS_TICK_SECONDS)

        if self._status is not None:
            self._status.clear()  # leave the terminal clean before stop()/summary

    def _check_historical_complete(self) -> bool:
        """Check if historical mode is complete. Returns True to exit."""
        assert self.downloader is not None
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
                logger.warning(
                    "%s queue drain timeout (%d/%d seconds)",
                    name,
                    int(time.time() - start_time),
                    timeout,
                )
                break

    def request_stop(self) -> None:
        """Ask the monitoring loop to exit so the orchestrator's own thread finalizes.

        Called from the CLI on SIGINT/SIGTERM. Distinct from ``stop()`` so signalling
        the loop never trips ``stop()``'s "already finalized" guard — finalize +
        summary then run on the joined (non-daemon) orchestrator thread, not a daemon
        the process kills on exit.
        """
        self._stop_requested = True

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

        # Close the root pipeline span if it was opened in start().
        if self._root_span is not None:
            self._root_span.__exit__(None, None, None)
            self._root_span = None

        # Stop threads
        for name, thread in [
            ("Downloader", self.downloader),
            ("Processor", self.processor),
        ]:
            if thread and thread.is_alive():
                thread.stop()
                thread.join(timeout=5)
                if thread.is_alive():
                    logger.warning("%s did not stop cleanly", name)

        # Save results
        if self.processor:
            self.processor.save_results()
            self.processor.close_database()

        # Execution history: flush captured warnings/errors, finalize the run record,
        # and print the one-shot console summary (before the repository closes).
        self._finalize_history()

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
            total_cells = stats.get("total_cells", 0) or 0
            logger.info(
                "Pipeline stopped. Runtime: %.1fs | files=%d completed=%d cells=%d",
                elapsed,
                stats.get("total", 0),
                stats.get("completed", 0),
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
            hist_status,
        )

    def close_repository(self) -> None:
        """Close repository connection if present."""
        if self.repository:
            self.repository.close()
