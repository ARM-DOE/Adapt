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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import pandas as pd

from adapt.modules.base import ContractViolation
from adapt.persistence import DataRepository, ProductType

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
        logger.info("RadarProcessor initialized with NexradPipeline")

        # Frame pairing orchestration state
        # We maintain a rolling list of segmented datasets and only call
        # projection/analysis/tracking when we have 2 valid frames
        self._segmented_history = []  # List of (filepath, ds_2d, scan_time) tuples
        self._max_history = config.processor.max_history  # Should be 2
        self._max_time_gap_minutes = config.projector.max_time_interval_minutes

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

        while not self.stopped():
            try:
                filepath = self.input_queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                self.process_file(filepath)
            except Exception:
                logger.exception("Failed to process file: %s", filepath)
            finally:
                self.input_queue.task_done()

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
        if isinstance(filepath, dict):
            filepath = filepath["path"]

        file_id = Path(filepath).stem
        tracker = self.file_tracker

        if tracker and tracker.should_process(file_id, "analyzed") is False:
            logger.info("Skipping already analyzed: %s", Path(filepath).name)
            return True

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

            ds_2d, scan_time = self._run_ingest_detection_only(context_initial)

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
                    "Segmented %s, waiting for pair (have %d/2 frames)",
                    Path(filepath).name,
                    len(self._segmented_history)
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

            result = self._run_full_pipeline(context_initial)

            # ──────────────────────────────────────────────────────────────────
            # PHASE 6: Save outputs to repository
            # ──────────────────────────────────────────────────────────────────
            if self.repository and result:
                self._save_results(result, scan_time)

            # Log cell count
            cell_stats = result.get("cell_stats")
            n_cells = len(cell_stats) if cell_stats is not None else 0
            logger.info("Processed pair: %d cells detected", n_cells)

            # Mark both files as processed
            if tracker:
                for fp, _, _ in self._segmented_history:
                    fid = Path(fp).stem
                    tracker.mark_stage_complete(fid, "analyzed", num_cells=n_cells)

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
            artifact   = self.repository.get_artifact(artifact_id)
            nc_path    = Path(artifact["file_path"])
            components = list(ds.data_vars.keys())
            logger.info("Analysis saved: %s [%s]", nc_path.name, ", ".join(components))
            return str(nc_path)

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
        """
        # Import modules directly (not through pipeline graph)
        from adapt.modules.ingest.module import LoadModule
        from adapt.modules.detection.module import DetectModule

        # Instantiate if not cached (persist across calls)
        if not hasattr(self, '_ingest_module'):
            self._ingest_module = LoadModule()
            self._detection_module = DetectModule()

        # Run ingest module
        result = self._ingest_module.run(context)
        context.update(result)

        # Run detection module
        result = self._detection_module.run(context)
        context.update(result)

        # Extract outputs
        ds_2d = context.get("segmented_ds")
        scan_time = context.get("scan_time")

        return ds_2d, scan_time

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

        This is called only when we have 2 segmented datasets with an
        acceptable time gap. The projection module will use the pre-built
        history to compute optical flow.

        Returns
        -------
        dict
            Pipeline result containing projected_ds, cell_stats, tracked_cells, etc.
        """
        # Inject segmented history directly into ProjectionModule
        # (NexradPipeline.process_file creates a fresh context, so we can't pass it via context)
        projection_module = self._get_projection_module()
        if projection_module:
            # Set the history directly on the module instance
            projection_module._dataset_history = [
                (fp, ds) for fp, ds, _ in self._segmented_history
            ]
            logger.debug("Injected %d-frame history into ProjectionModule",
                        len(self._segmented_history))

        # Run full pipeline graph (uses most recent file as primary)
        filepath = self._segmented_history[-1][0]  # Most recent file
        result = self._pipeline.process_file(
            nexrad_file=filepath,
            repository=self.repository,
        )

        return result

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
        """Save pipeline outputs to repository.

        Saves:
        - projected_ds as NetCDF artifact
        - cell_stats already saved by AnalysisModule
        - tracked_cells/tracked_storms will be saved by TrackingModule
        """
        # Save projected_ds to NetCDF (contains segmentation + projections + flow)
        projected_ds = result.get("projected_ds")
        if projected_ds is not None:
            filepath = self._segmented_history[-1][0]  # Most recent file
            self._save_analysis_netcdf(projected_ds, filepath, scan_time)

        # Log results
        cell_stats = result.get("cell_stats")
        n_cells = len(cell_stats) if cell_stats is not None else 0
        logger.info("Frame pair processed: %d cells detected", n_cells)

    # ── Results API (called by orchestrator on shutdown) ──────────────────────

    def get_results(self) -> pd.DataFrame:
        """Return processed cell statistics.

        Cell stats are written to the repository by AnalysisModule on each
        file.  This method returns an empty DataFrame since the canonical
        data source is the repository itself (use DataClient to query it).
        """
        return pd.DataFrame()

    def save_results(self, filepath: str = None):
        """No-op: AnalysisModule writes results to the repository directly."""
        pass

    def close_database(self):
        """No-op: repository manages its own connection lifecycle."""
        pass
