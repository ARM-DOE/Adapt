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

from adapt.contracts import ContractViolation
from adapt.core import DataRepository, ProductType

if TYPE_CHECKING:
    from adapt.schemas import InternalConfig

__all__ = ['RadarProcessor']

logger = logging.getLogger(__name__)


class RadarProcessor(threading.Thread):
    """Worker thread that processes NEXRAD files through the execution graph.

    Receives file paths from the downloader queue and runs them through
    ``NexradPipeline``, which executes the module DAG (load → detect →
    projection → analysis). Scientific module instances inside the pipeline
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
        from adapt.controller.pipeline import NexradPipeline
        self._pipeline = NexradPipeline(config, dict(output_dirs))
        logger.info("RadarProcessor initialized with NexradPipeline")

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
        """Run the full processing graph for a single NEXRAD file.

        Delegates to ``NexradPipeline.process_file()``, then saves the
        projected/segmented dataset to a NetCDF artifact in the repository.
        AnalysisModule writes cell statistics to the repository automatically
        when ``repository`` is present in the graph context.

        Parameters
        ----------
        filepath : str or dict
            Path to the NEXRAD Level-II file. Dict format ``{"path": ...}``
            is accepted for backwards compatibility with the downloader queue.

        Returns
        -------
        bool
            True if the file was processed (or already done), False on error.
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
            result = self._pipeline.process_file(
                nexrad_file=filepath,
                repository=self.repository,
            )

            # Save the projected/segmented dataset to NetCDF for consumers
            projected_ds = result.get("projected_ds") or result.get("segmented_ds")
            scan_time    = result.get("scan_time")
            if projected_ds is not None:
                self._save_analysis_netcdf(projected_ds, filepath, scan_time)

            # Log cell count
            cell_stats = result.get("cell_stats")
            n_cells = len(cell_stats) if cell_stats is not None else 0
            logger.info(
                "Processed: %s  (%d cells)", Path(filepath).name, n_cells
            )

            if tracker:
                tracker.mark_stage_complete(
                    file_id, "analyzed", num_cells=n_cells
                )
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
