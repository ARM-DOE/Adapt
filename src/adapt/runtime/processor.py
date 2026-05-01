# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Radar data processor thread.

Reads NEXRAD file paths from the downloader queue and delegates all
scientific processing to two GraphExecutors built at startup:

- ``_single_executor``: ingest + detection (runs every file)
- ``_multi_executor``: projection + analysis + tracking (runs when 2-frame
  pair is ready)

Responsibilities of this class (orchestration only):
- Queue management: pop filepath, mark task done
- File deduplication via FileProcessingTracker
- Frame pairing: accumulate segmented history, validate time gap
- Context assembly: inject dataset_history before calling multi-executor
- NetCDF + Parquet persistence after graph run
- Stop/start lifecycle
"""

import logging
import queue
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from adapt.configuration.schemas.materialization import materialize_module_configs
from adapt.contracts import ContractViolation
from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor
from adapt.execution.module_registry import registry
from adapt.execution.pipeline_builder import _ensure_modules_registered
from adapt.persistence import DataRepository, ProductType
from adapt.persistence.track_store import TrackStore
from adapt.persistence.writer import RepositoryWriter

if TYPE_CHECKING:
    from adapt.configuration.schemas import InternalConfig

__all__ = ['RadarProcessor']

logger = logging.getLogger(__name__)


class RadarProcessor(threading.Thread):
    """Worker thread that processes NEXRAD files through two execution graphs.

    Receives file paths from the downloader queue. For each file:

    1. Runs the single-frame graph (ingest → detection) via ``_single_executor``.
    2. Accumulates segmented datasets in a rolling history.
    3. When a valid 2-frame pair is ready, runs the multi-frame graph
       (projection → analysis → tracking) via ``_multi_executor``,
       passing the frame history in context.

    Both executors enforce input/output contracts at every DAG edge via
    ``GraphExecutor``. The processor itself performs no validation.

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
        repository: DataRepository | None = None,
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

        # Build two execution graphs; module instances are shared (stateful
        # projector/tracker state persists across files via the module objects).
        _ensure_modules_registered()
        modules = registry.create_modules()

        single_modules = [m for m in modules if m.name in {"ingest", "detection"}]
        multi_modules  = [m for m in modules if m.name in {"projection", "analysis", "tracking"}]

        self._single_executor = GraphExecutor(GraphBuilder(single_modules).build())
        self._multi_executor  = GraphExecutor(GraphBuilder(multi_modules).build())

        self._module_configs = materialize_module_configs(config)

        logger.info(
            "RadarProcessor graphs: single=[%s] multi=[%s]",
            ", ".join(m.name for m in single_modules),
            ", ".join(m.name for m in multi_modules),
        )

        # Frame pairing orchestration state
        self._segmented_history = []  # list of (filepath, ds_2d, scan_time)
        self._max_history = config.processor.max_history
        self._max_time_gap_minutes = config.projector.max_time_interval_minutes
        self._last_skipped = False

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
        """Process a NEXRAD file with frame-pairing orchestration.

        Phase 1 — ingest + detection (every file):
            Runs single-frame executor. Contract-validated by GraphExecutor.

        Phase 2 — frame pairing:
            Accumulates segmented datasets. Waits until 2 frames are ready.

        Phase 3 — projection + analysis + tracking (when pair is ready):
            Injects dataset_history into context. Runs multi-frame executor.
            Contract-validated by GraphExecutor.

        Returns
        -------
        bool
            True if processed or deferred (waiting for pair), False on error.
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
            # ── Phase 1: ingest + detection ────────────────────────────────
            t0 = time.perf_counter()
            base_ctx = {
                "nexrad_file": filepath,
                "ingest_config": self._module_configs["ingest_config"],
                "detection_config": self._module_configs["detection_config"],
                "output_dirs": self.output_dirs,
            }
            if self.repository:
                base_ctx["repository"] = self.repository

            frame_ctx = self._single_executor.run(base_ctx)
            single_s  = time.perf_counter() - t0

            # Register radar location from first scan (idempotent after that)
            if self.repository:
                grid_ds = frame_ctx.get("grid_ds") or frame_ctx.get("grid_ds_2d")
                if grid_ds is not None:
                    lat = grid_ds.attrs.get("radar_latitude")
                    lon = grid_ds.attrs.get("radar_longitude")
                    if lat is not None and lon is not None:
                        self.repository.registry.ensure_radar_location(
                            self.config.downloader.radar, lat=float(lat), lon=float(lon)
                        )

            scan_time = frame_ctx.get("scan_time")

            # ── Phase 2: accumulate frame history ──────────────────────────
            self._segmented_history.append((filepath, frame_ctx["segmented_ds"], scan_time))
            if len(self._segmented_history) > self._max_history:
                self._segmented_history.pop(0)

            if len(self._segmented_history) < 2:
                logger.info(
                    "Segmented %s, waiting for pair | %.1fs",
                    Path(filepath).name, single_s,
                )
                return True

            # ── Phase 3: validate time gap ─────────────────────────────────
            time_gap_valid, time_gap_minutes = self._validate_time_gap()
            if not time_gap_valid:
                logger.warning(
                    "Time gap %.1f min > %.1f min, discarding oldest frame.",
                    time_gap_minutes, self._max_time_gap_minutes,
                )
                return True

            logger.info(
                "Processing pair: %s + %s (gap: %.1f min)",
                Path(self._segmented_history[0][0]).name,
                Path(self._segmented_history[1][0]).name,
                time_gap_minutes,
            )

            # ── Phase 4: projection + analysis + tracking ──────────────────
            t_proj = time.perf_counter()
            pair_ctx = {
                **frame_ctx,
                "projection_config": self._module_configs["projection_config"],
                "analysis_config": self._module_configs["analysis_config"],
                "tracking_config": self._module_configs["tracking_config"],
                "output_dirs": self.output_dirs,
                "dataset_history": [(fp, ds) for fp, ds, _ in self._segmented_history],
            }
            if self.repository:
                pair_ctx["repository"] = self.repository

            result = self._multi_executor.run(pair_ctx)
            project_s = time.perf_counter() - t_proj

            # ── Phase 5: persist results ───────────────────────────────────
            if self.repository and result:
                self._save_results(result, scan_time)

            cell_stats = result.get("cell_stats")
            n_cells = len(cell_stats) if cell_stats is not None else 0
            logger.info(
                "Processed pair: %d cells | %.1fs proj%s",
                n_cells, project_s,
                f" queue={queue_wait_s:.1f}s" if queue_wait_s is not None else "",
            )

            if tracker:
                timings = {"project_seconds": project_s}
                if queue_wait_s is not None:
                    timings["queue_wait_seconds"] = queue_wait_s
                for fp, _, _ in self._segmented_history:
                    fid = Path(fp).stem
                    tracker.mark_stage_complete(fid, "analyzed", num_cells=n_cells, timings=timings)

            return True

        except ContractViolation as e:
            logger.critical("CRITICAL: Pipeline contract violated: %s. Stopping pipeline.", e)
            self.stop()
            if tracker:
                tracker.mark_stage_complete(file_id, "analyzed", error=f"ContractViolation: {e}")
            return False

        except Exception as e:
            logger.exception("Error processing %s", filepath)
            if tracker:
                tracker.mark_stage_complete(file_id, "analyzed", error=str(e))
            return False

    # ── Frame pairing helpers ─────────────────────────────────────────────────

    def _validate_time_gap(self):
        """Return (valid, gap_minutes) for the two frames in history."""
        if len(self._segmented_history) < 2:
            return False, 0.0
        time1 = self._segmented_history[0][2]
        time2 = self._segmented_history[1][2]
        gap_minutes = (time2 - time1).total_seconds() / 60.0
        return abs(gap_minutes) <= self._max_time_gap_minutes, gap_minutes

    # ── Persistence helpers ───────────────────────────────────────────────────

    def _save_analysis_netcdf(self, ds, filepath: str, scan_time) -> str | None:
        """Write the analysis dataset to a NetCDF artifact in the repository."""
        try:
            radar         = self.config.downloader.radar
            filename_stem = Path(filepath).stem
            if scan_time is None:
                scan_time = datetime.now(UTC)

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

    def _save_results(self, result: dict, scan_time):
        """Save all pipeline outputs to the repository."""
        if scan_time is not None and scan_time.tzinfo is None:
            scan_time = scan_time.replace(tzinfo=UTC)

        projected_ds = result.get("projected_ds")
        if projected_ds is not None:
            filepath = self._segmented_history[-1][0]
            self._save_analysis_netcdf(projected_ds, filepath, scan_time)

        writer = RepositoryWriter(self.repository)

        cell_stats     = result.get("cell_stats")
        cell_adjacency = result.get("cell_adjacency")
        tracked_cells  = result.get("tracked_cells")
        cell_events    = result.get("cell_events")

        if cell_stats is not None and not cell_stats.empty:
            writer.write_analysis(df=cell_stats, scan_time=scan_time, producer="analysis")
        if cell_adjacency is not None and not cell_adjacency.empty:
            writer.write_analysis(df=cell_adjacency, scan_time=scan_time, producer="cell_adjacency")

        if tracked_cells is not None and not tracked_cells.empty:
            if cell_stats is None:
                raise ValueError("Missing required cell_stats for TrackStore persistence")
            if cell_adjacency is None:
                raise ValueError("Missing required cell_adjacency for TrackStore persistence")
            TrackStore(self.repository.catalog.db_path).write_scan(
                run_id=self.repository.run_id,
                scan_time=scan_time,
                cell_stats_df=cell_stats,
                tracked_cells_df=tracked_cells,
                cell_events_df=cell_events if cell_events is not None else pd.DataFrame(),
                cell_adjacency_df=cell_adjacency,
            )

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
