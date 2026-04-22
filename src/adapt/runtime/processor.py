"""Radar data processor thread.

Reads NEXRAD file paths from the downloader queue and delegates all
scientific processing to NexradPipeline (the graph-based execution engine).
After each file the segmentation NetCDF is saved to the repository.

Responsibilities of this class (orchestration only):
- Queue management: pop filepath, mark task done
- File deduplication via FileProcessingTracker
- NetCDF persistence after graph run
- Stop/start lifecycle
"""

import logging
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import pandas as pd

from adapt.modules.base import ContractViolation
from adapt.persistence import DataRepository, ProductType
from adapt.persistence.writer import RepositoryWriter

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig

__all__ = ['RadarProcessor']

logger = logging.getLogger(__name__)


class RadarProcessor(threading.Thread):
    """Worker thread that processes NEXRAD files through the execution graph.

    Receives file paths from the downloader queue and runs them through
    ``NexradPipeline``, which executes the module DAG (ingest → detection →
    projection → analysis → tracking). Scientific module instances inside the pipeline
    persist across files so stateful modules (e.g. ProjectionModule frame
    history) work correctly.

    After the graph runs, this class saves the projected/segmented dataset
    to a NetCDF artifact in the repository for downstream consumers.

    Example usage (called by PipelineOrchestrator)::

        processor = RadarProcessor(
            input_queue=downloader_queue,
            config=config,
            output_dirs=dirs,
            file_tracker=tracker,
            repository=repo,
        )
        processor.start()
        ...
        processor.stop()
    """

    def __init__(
        self,
        input_queue: queue.Queue,
        config: "InternalConfig",
        output_dirs: dict,
        file_tracker=None,
        repository: Optional[DataRepository] = None,
        name: str = "RadarProcessor",
    ):
        super().__init__(daemon=True, name=name)

        self.input_queue  = input_queue
        self.config       = config
        self.output_dirs  = {k: Path(v) for k, v in output_dirs.items()}
        self.file_tracker = file_tracker
        self.repository   = repository
        self._stop_event  = threading.Event()
        self.output_lock  = threading.Lock()

        if not self.repository:
            raise ValueError(
                "DataRepository is required for RadarProcessor. "
                "Initialize it in the orchestrator before creating the processor."
            )

        # Build the graph-based pipeline once — module instances (and their
        # frame history) persist across process_file() calls.
        from adapt.execution.pipeline_builder import NexradPipeline
        self._pipeline = NexradPipeline(config, dict(output_dirs))

        # Frame pairing orchestration state
        # We maintain a rolling list of segmented datasets and only call
        # projection/analysis/tracking when we have 2 valid frames
        self._segmented_history = []  # List of (filepath, ds_2d, scan_time) tuples
        self._max_history = config.processor.max_history  # Should be 2
        self._max_time_gap_minutes = config.projector.max_time_interval_minutes
        self._last_skipped = False  # Set True when process_file skips an analyzed file

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def stop(self):
        """Signal the processor to stop after the current file finishes."""
        self._stop_event.set()

    def stopped(self) -> bool:
        """True if stop() has been called or a ContractViolation forced stop."""
        return self._stop_event.is_set()

    def run(self):
        """Main processor loop (runs in thread)."""
        logger.info("Processor started, waiting for files...")
        _skip_count = 0

        while not self.stopped():
            try:
                filepath = self.input_queue.get(timeout=1)
            except queue.Empty:
                if _skip_count:
                    logger.info("Skipped %d already-analyzed files", _skip_count)
                    _skip_count = 0
                continue

            try:
                skipped = self.process_file(filepath)
                if skipped is True and self._last_skipped:
                    _skip_count += 1
                else:
                    if _skip_count:
                        logger.info("Skipped %d already-analyzed files", _skip_count)
                        _skip_count = 0
            except Exception:
                logger.exception("Failed to process file: %s", filepath)
            finally:
                self.input_queue.task_done()

        if _skip_count:
            logger.info("Skipped %d already-analyzed files", _skip_count)
        logger.info("Processor stopped")

    # ── Per-file processing ───────────────────────────────────────────────────

    def process_file(self, filepath) -> bool:
        """Process NEXRAD file with frame pairing orchestration.

        New architecture:
        - File 1: Load → Detect → Wait (build history, no projection yet)
        - File 2: Load → Detect → Check pair → Projection → Analysis → Tracking

        Only calls projection/analysis/tracking when we have 2 segmented
        datasets with an acceptable time gap. This prevents crashes when
        modules expect projected_ds but only 1 file has been processed.

        Parameters
        ----------
        filepath : str or dict
            Path to the NEXRAD Level-II file. Dict format ``{"path": ...}``
            is accepted for backwards compatibility with the downloader queue.

        Returns
        -------
        bool
            True if the file was processed (or ready to pair), False on error.
        """
        queued_at = None
        if isinstance(filepath, dict):
            queued_at = filepath.get("queued_at")
            filepath = filepath["path"]

        file_id = Path(filepath).stem
        tracker = self.file_tracker

        if tracker and tracker.should_process(file_id, "analyzed") is False:
            self._last_skipped = True
            return True
        self._last_skipped = False

        queue_wait_s = (time.time() - queued_at) if queued_at else None
        logger.info("Processing: %s", Path(filepath).name)

        try:
            # ──────────────────────────────────────────────────────────────────
            # PHASE 1: Load + Detect (always run, even for first file)
            # ──────────────────────────────────────────────────────────────────
            context_initial = {
                "nexrad_file": filepath,
                "config": self.config,
                "output_dirs": self.output_dirs,
            }
            if self.repository:
                context_initial["repository"] = self.repository

            ds_2d, scan_time, ingest_s, detect_s = self._run_ingest_detection_only(context_initial)

            # ──────────────────────────────────────────────────────────────────
            # PHASE 2: Add to rolling history
            # ──────────────────────────────────────────────────────────────────
            self._segmented_history.append((filepath, ds_2d, scan_time))
            if len(self._segmented_history) > self._max_history:
                self._segmented_history.pop(0)

            # ──────────────────────────────────────────────────────────────────
            # PHASE 3: Check if ready for full processing
            # ──────────────────────────────────────────────────────────────────
            if len(self._segmented_history) < 2:
                logger.info(
                    "Segmented %s, waiting for pair | ingest=%.1fs detect=%.1fs",
                    Path(filepath).name, ingest_s, detect_s,
                )
                return True  # Success, but waiting for second file

            # ──────────────────────────────────────────────────────────────────
            # PHASE 4: Validate time gap between frames
            # ──────────────────────────────────────────────────────────────────
            time_gap_valid, time_gap_minutes = self._validate_time_gap()
            if not time_gap_valid:
                logger.warning(
                    "Time gap %.1f min > %.1f min, discarding oldest frame. "
                    "Waiting for next file with smaller gap.",
                    time_gap_minutes,
                    self._max_time_gap_minutes
                )
                # Keep newest frame in history, wait for next file
                return True  # Not an error, just waiting for better pair

            # ──────────────────────────────────────────────────────────────────
            # PHASE 5: Run full pipeline (projection → analysis → tracking)
            # ──────────────────────────────────────────────────────────────────
            logger.info(
                "Processing pair: %s + %s (gap: %.1f min)",
                Path(self._segmented_history[0][0]).name,
                Path(self._segmented_history[1][0]).name,
                time_gap_minutes
            )

            t_proj = time.perf_counter()
            result = self._run_full_pipeline(context_initial)
            project_s = time.perf_counter() - t_proj

            # ──────────────────────────────────────────────────────────────────
            # PHASE 6: Save outputs to repository
            # ──────────────────────────────────────────────────────────────────
            if self.repository and result:
                self._save_results(result, scan_time)

            # Log cell count + timing
            cell_stats = result.get("cell_stats")
            n_cells = len(cell_stats) if cell_stats is not None else 0
            logger.info(
                "Processed pair: %d cells | ingest=%.1fs detect=%.1fs project=%.1fs%s",
                n_cells, ingest_s, detect_s, project_s,
                f" queue=%.1fs" % queue_wait_s if queue_wait_s is not None else "",
            )

            # Mark both files as processed
            if tracker:
                timings = {
                    "ingest_seconds": ingest_s,
                    "detect_seconds": detect_s,
                    "project_seconds": project_s,
                }
                if queue_wait_s is not None:
                    timings["queue_wait_seconds"] = queue_wait_s
                for fp, _, _ in self._segmented_history:
                    fid = Path(fp).stem
                    tracker.mark_stage_complete(fid, "analyzed", num_cells=n_cells, timings=timings)

            return True

        except ContractViolation as e:
            logger.critical(
                "CRITICAL: Pipeline contract violated: %s. Stopping pipeline.", e
            )
            self.stop()
            if tracker:
                tracker.mark_stage_complete(
                    file_id, "analyzed", error=f"ContractViolation: {e}"
                )
            return False

        except Exception as e:
            logger.exception("Error processing %s", filepath)
            if tracker:
                tracker.mark_stage_complete(file_id, "analyzed", error=str(e))
            return False

    # ── NetCDF persistence ────────────────────────────────────────────────────

    def _save_analysis_netcdf(self, ds, filepath: str, scan_time) -> Optional[str]:
        """Write the analysis dataset to a NetCDF artifact in the repository."""
        try:
            radar         = self.config.downloader.radar
            filename_stem = Path(filepath).stem
            if scan_time is None:
                scan_time = datetime.now(timezone.utc)

            ds.attrs.update({
                "source":      str(filepath),
                "radar":       radar,
                "description": "Radar analysis with segmentation and projections",
            })

            artifact_id = self.repository.write_netcdf(
                ds=ds,
                product_type=ProductType.ANALYSIS_NC,
                scan_time=scan_time,
                producer="processor",
                parent_ids=[],
                metadata={"components": list(ds.data_vars.keys())},
                filename_stem=filename_stem,
            )
            components = list(ds.data_vars.keys())
            logger.info("Analysis saved: %s [%s]", artifact_id, ", ".join(components))
            return artifact_id

        except Exception as e:
            logger.warning("Could not save analysis NetCDF: %s", e)
            return None

    # ── Frame Pairing Orchestration Helpers ───────────────────────────────────

    def _run_ingest_detection_only(self, context: dict):
        """Run ONLY ingest + detection modules (segmentation).

        This runs the first part of the pipeline (ingest and detection) without
        calling projection/analysis/tracking. Used to build up the rolling
        history of segmented datasets.

        Returns
        -------
        ds_2d : xr.Dataset
            Segmented 2D dataset with cell_labels
        scan_time : datetime
            Scan timestamp
        ingest_seconds : float
            Wall time for the ingest (regridding) step
        detect_seconds : float
            Wall time for the detection (segmentation) step
        """
        # Import modules directly (not through pipeline graph)
        from adapt.modules.ingest.module import LoadModule
        from adapt.modules.detection.module import DetectModule

        # Instantiate if not cached (persist across calls)
        if not hasattr(self, '_ingest_module'):
            self._ingest_module = LoadModule()
            self._detection_module = DetectModule()

        # Run ingest module
        t0 = time.perf_counter()
        result = self._ingest_module.run(context)
        context.update(result)
        ingest_s = time.perf_counter() - t0

        # Persist radar location from actual data on first file (idempotent after that).
        if self.repository:
            grid_ds = context.get("grid_ds") or context.get("grid_ds_2d")
            if grid_ds is not None:
                lat = grid_ds.attrs.get("radar_latitude")
                lon = grid_ds.attrs.get("radar_longitude")
                if lat is not None and lon is not None:
                    self.repository.registry.ensure_radar_location(
                        self.config.downloader.radar, lat=float(lat), lon=float(lon)
                    )

        # Run detection module
        t1 = time.perf_counter()
        result = self._detection_module.run(context)
        context.update(result)
        detect_s = time.perf_counter() - t1

        # Extract outputs
        ds_2d = context.get("segmented_ds")
        scan_time = context.get("scan_time")

        return ds_2d, scan_time, ingest_s, detect_s

    def _validate_time_gap(self):
        """Check if time gap between frames is acceptable for optical flow.

        Returns
        -------
        valid : bool
            True if time gap is within max_time_interval_minutes
        gap_minutes : float
            Actual time gap in minutes
        """
        if len(self._segmented_history) < 2:
            return False, 0.0

        # Extract scan times from history tuples
        time1 = self._segmented_history[0][2]  # (filepath, ds_2d, scan_time)
        time2 = self._segmented_history[1][2]

        # Compute gap in minutes
        gap_minutes = (time2 - time1).total_seconds() / 60.0
        valid = abs(gap_minutes) <= self._max_time_gap_minutes

        return valid, gap_minutes

    def _run_full_pipeline(self, context: dict):
        """Run projection → analysis → tracking on validated frame pair.

        Reuses the context already populated by _run_ingest_detection_only
        (which contains grid_ds, segmented_ds, scan_time, etc.) to avoid
        re-running the expensive ingest step.

        Returns
        -------
        dict
            Updated context with projected_ds, cell_stats, tracked_cells, etc.
        """
        # Inject segmented history into ProjectionModule
        projection_module = self._get_projection_module()
        if projection_module:
            projection_module._dataset_history = [
                (fp, ds) for fp, ds, _ in self._segmented_history
            ]

        # Run only the stages after ingest+detection — they are already in context
        _skip = {"ingest", "detection"}
        for node in self._pipeline._nodes:
            if node.name not in _skip:
                result = node.module.run(context)
                context.update(result)

        return context

    def _get_projection_module(self):
        """Get ProjectionModule instance from pipeline graph."""
        try:
            for node in self._pipeline._nodes:
                if node.name == "projection":
                    return node.module
        except Exception:
            logger.warning("Could not find ProjectionModule in pipeline")
        return None

    def _save_results(self, result: dict, scan_time):
        """Save all pipeline outputs to the repository.

        Saves:
        - projected_ds as NetCDF artifact
        - cell_stats, cell_adjacency, tracked_cells, track_events,
          tracked_cell_adjacency as Parquet artifacts
        """
        if scan_time is not None and scan_time.tzinfo is None:
            scan_time = scan_time.replace(tzinfo=timezone.utc)

        # NetCDF: segmentation + projections + flow vectors
        projected_ds = result.get("projected_ds")
        if projected_ds is not None:
            filepath = self._segmented_history[-1][0]  # Most recent file
            self._save_analysis_netcdf(projected_ds, filepath, scan_time)

        # Parquet: analysis and tracking outputs
        writer = RepositoryWriter(self.repository)

        cell_stats      = result.get("cell_stats")
        cell_adjacency  = result.get("cell_adjacency")
        tracked_cells   = result.get("tracked_cells")
        track_events    = result.get("track_events")
        tracked_adj     = result.get("tracked_cell_adjacency")

        if cell_stats is not None and not cell_stats.empty:
            writer.write_analysis(df=cell_stats, scan_time=scan_time, producer="analysis")
        if cell_adjacency is not None and not cell_adjacency.empty:
            writer.write_analysis(df=cell_adjacency, scan_time=scan_time, producer="cell_adjacency")
        if tracked_cells is not None and not tracked_cells.empty:
            writer.write_analysis(df=tracked_cells, scan_time=scan_time, producer="tracking_cells")
        if track_events is not None and not track_events.empty:
            writer.write_analysis(df=track_events, scan_time=scan_time, producer="tracking_events")
        if tracked_adj is not None and not tracked_adj.empty:
            writer.write_analysis(df=tracked_adj, scan_time=scan_time, producer="tracking_adjacency")

    # ── Results API (called by orchestrator on shutdown) ──────────────────────

    def get_results(self) -> pd.DataFrame:
        """Cell stats are in the repository; use DataClient to query them."""
        return pd.DataFrame()

    def save_results(self, filepath: str = None):
        """No-op: processor writes results to repository in _save_results."""
        pass

    def close_database(self):
        """No-op: repository manages its own connection lifecycle."""
        pass
