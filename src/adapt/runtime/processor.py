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

import numpy as np
import pandas as pd
import xarray as xr

from adapt.configuration.schemas.module_resolver import resolve_module_configs
from adapt.contracts import ContractViolation
from adapt.execution.graph.builder import GraphBuilder
from adapt.execution.graph.executor import GraphExecutor
from adapt.execution.module_registry import registry
from adapt.execution.pipeline_builder import _ensure_modules_registered, resolve_enabled_modules
from adapt.persistence import DataRepository, ProductType
from adapt.persistence.track_store import TrackStore
from adapt.persistence.writer import RepositoryWriter
from adapt.utils.cell_uid_lut import build_cell_uid_lut

if TYPE_CHECKING:
    from adapt.configuration.schemas.internal import InternalConfig

__all__ = ["RadarProcessor"]

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

        self.input_queue = input_queue
        self.config = config
        self.output_dirs = {k: Path(v) for k, v in output_dirs.items()}
        self.file_tracker = file_tracker
        self.repository = repository
        self._stop_event = threading.Event()
        self.output_lock = threading.Lock()

        if not self.repository:
            raise ValueError(
                "DataRepository is required for RadarProcessor. "
                "Initialize it in the orchestrator before creating the processor."
            )

        # Build one execution graph per required_history value.
        # Module instances are shared (stateful projector/tracker persists across files).
        _ensure_modules_registered(config.extensions)
        modules = registry.create_modules()
        modules = resolve_enabled_modules(
            modules,
            modules=config.modules,
            only=config.only_modules,
            exclude=config.exclude_modules,
        )
        logger.info("Enabled modules: [%s]", ", ".join(m.name for m in modules))

        in_pipeline = [m for m in modules if m.pipeline_phase != 3]
        post_persist = [m for m in modules if m.pipeline_phase == 3]

        history_groups: dict[int, list] = {}
        for m in in_pipeline:
            history_groups.setdefault(m.required_history, []).append(m)

        self._executors: dict[int, GraphExecutor] = {
            req: GraphExecutor(GraphBuilder(mods).build())
            for req, mods in sorted(history_groups.items())
        }

        self._post_modules = post_persist
        self._post_executor: GraphExecutor | None = (
            GraphExecutor(GraphBuilder(post_persist).build()) if post_persist else None
        )

        self._module_configs = resolve_module_configs(config)

        for req, mods in sorted(history_groups.items()):
            logger.info(
                "RadarProcessor required_history=%d: [%s]",
                req,
                ", ".join(m.name for m in mods),
            )
        if post_persist:
            logger.info(
                "RadarProcessor post-persistence: [%s]",
                ", ".join(m.name for m in post_persist),
            )

        # Rolling scan history (replaces per-phase segmented_history)
        self._scan_history: list[dict] = []
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

        Phase 1 — pipeline_phase=1 modules (every file):
            Per-file executor. Contract-validated by GraphExecutor.

        Frame pairing:
            Accumulates segmented datasets. Waits until 2 frames are ready.

        Phase 2 — pipeline_phase=2 modules (when pair is ready):
            Injects dataset_history into context. Per-pair executor.
            Contract-validated by GraphExecutor.

        Phase 3 — pipeline_phase=3 modules (after persistence, when pair ran):
            Post-persistence executor. Extensions read from the data store
            independently using the persistence reader/writer.
            Context: run_id, scan_time, catalog_path, repository.
            No-op if no phase-3 modules are registered.

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
            t0 = time.perf_counter()

            # ── Build base context with all module configs ─────────────────
            base_ctx: dict = {
                "nexrad_file": filepath,
                **self._module_configs,
                "output_dirs": self.output_dirs,
            }
            if self.repository:
                base_ctx["repository"] = self.repository

            # ── Rolling window: run all executor groups in history-size order
            # required_history=N means N scans total (N-1 prior + current).
            # Skip if fewer than N-1 prior scans are available.
            result: dict = {}
            for req_hist, executor in sorted(self._executors.items()):
                prior_needed = req_hist - 1
                if len(self._scan_history) < prior_needed:
                    logger.info(
                        "Waiting for history: need %d prior scans, have %d (%s)",
                        prior_needed,
                        len(self._scan_history),
                        Path(filepath).name,
                    )
                    continue

                if req_hist > 1:
                    # Validate time gap before running multi-scan modules
                    current_scan_time = result.get("scan_time")
                    time_gap_valid, time_gap_minutes = self._validate_time_gap(current_scan_time)
                    if not time_gap_valid:
                        logger.warning(
                            "Time gap %.1f min > %.1f min, skipping multi-scan modules.",
                            time_gap_minutes,
                            self._max_time_gap_minutes,
                        )
                        continue

                ctx = {**base_ctx, **result}
                if req_hist > 1:
                    # Build scan_history: (N-1) prior entries + current partial context
                    prior = self._scan_history[-prior_needed:] if prior_needed else []
                    ctx["scan_history"] = list(prior) + [{**base_ctx, **result}]
                group_result = executor.run(ctx)
                result.update(group_result)

            scan_time = result.get("scan_time") or base_ctx.get("scan_time")
            # Normalize once to tz-aware UTC so persistence (artifact registration),
            # the enrich 3D-grid read, and the enrich module all use one representation.
            scan_time = self._normalize_scan_time(scan_time)
            elapsed_s = time.perf_counter() - t0

            # Register radar location from first scan (idempotent after that)
            if self.repository:
                grid_ds = result.get("grid_ds") or result.get("grid_ds_2d")
                if grid_ds is not None:
                    lat = grid_ds.attrs.get("radar_latitude")
                    lon = grid_ds.attrs.get("radar_longitude")
                    if lat is not None and lon is not None:
                        self.repository.registry.ensure_radar_location(
                            self.config.downloader.radar, lat=float(lat), lon=float(lon)
                        )

            # ── Accumulate scan in rolling history ─────────────────────────
            self._scan_history.append({**base_ctx, **result})
            if len(self._scan_history) > self._max_history:
                self._scan_history.pop(0)

            # ── Persist results ────────────────────────────────────────────
            if self.repository and result:
                self._save_results(result, scan_time)

            # ── Post-persistence enrichment (pipeline_phase=3) ─────────────
            # Enrich modules index on (scan_time, cell_uid); they only run once
            # tracking has committed cell_uid for this scan.
            if self._post_executor is not None and self._should_run_enrichment(result):
                post_ctx = self._build_enrich_context(result, scan_time)
                ext_result = self._post_executor.run(post_ctx)
                self._save_enrichment_results(ext_result)

            cell_stats = result.get("cell_stats")
            n_cells = len(cell_stats) if cell_stats is not None else 0
            logger.info(
                "Processed: %d cells | %.1fs%s",
                n_cells,
                elapsed_s,
                f" queue={queue_wait_s:.1f}s" if queue_wait_s is not None else "",
            )

            if tracker:
                timings = {"project_seconds": elapsed_s}
                if queue_wait_s is not None:
                    timings["queue_wait_seconds"] = queue_wait_s
                tracker.mark_stage_complete(file_id, "analyzed", num_cells=n_cells, timings=timings)

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

    # ── Enrichment (post-persistence) helpers ─────────────────────────────────

    def _should_run_enrichment(self, result: dict) -> bool:
        """True when enrich modules may run: they require committed cell_uid."""
        if self._post_executor is None or not self.repository:
            return False
        tracked_cells = result.get("tracked_cells")
        return (
            tracked_cells is not None
            and not tracked_cells.empty
            and "cell_uid" in tracked_cells.columns
        )

    def _build_enrich_context(self, result: dict, scan_time) -> dict:
        """Assemble the post-persistence context, injecting stored artifacts on demand.

        Modules never touch storage. If any enrich module declares ``grid_ds_3d``
        as an input, the processor reads the registered 3D gridded NetCDF for this
        scan and injects it. Other declared storage-backed inputs follow the same
        pattern as they are added.
        """
        assert self.repository is not None
        ctx = {
            **result,
            "run_id": self.repository.run_id,
            "scan_time": scan_time,
            "catalog_path": self.repository.catalog.db_path,
            "repository": self.repository,
        }
        if any("grid_ds_3d" in m.inputs for m in self._post_modules):
            grid_3d = self._read_grid_3d(scan_time)
            if grid_3d is not None:
                ctx["grid_ds_3d"] = grid_3d
        return ctx

    def _read_grid_3d(self, scan_time):
        """Read the registered 3D gridded NetCDF for this scan, or None if absent."""
        assert self.repository is not None
        # The artifact is registered with a tz-aware (UTC) scan_time; normalize the
        # query the same way so the catalog's isoformat comparison matches.
        scan_time = self._normalize_scan_time(scan_time)
        artifacts = self.repository.query(
            product_type=ProductType.GRIDDED_NC, time_range=(scan_time, scan_time)
        )
        if not artifacts:
            return None
        return self.repository.open_dataset(artifacts[0]["artifact_id"])

    def _save_enrichment_results(self, ext_result: dict) -> None:
        """Write each enrich module's declared output table from its returned DataFrame."""
        from adapt.persistence.module_output import ModuleOutputWriter

        assert self.repository is not None
        for module in self._post_modules:
            spec = module.output_table
            if spec is None or not module.outputs:
                continue
            df = ext_result.get(module.outputs[0])
            if df is None or getattr(df, "empty", True):
                continue
            ModuleOutputWriter(self.repository.catalog.db_path, spec).write(df)

    # ── Frame pairing helpers ─────────────────────────────────────────────────

    def _validate_time_gap(self, current_scan_time=None):
        """Return (valid, gap_minutes) between the most recent history entry and current scan."""
        if not self._scan_history:
            return False, 0.0
        time1 = self._scan_history[-1].get("scan_time")
        time2 = current_scan_time
        if time1 is None or time2 is None:
            return False, 0.0
        gap_minutes = (time2 - time1).total_seconds() / 60.0
        return abs(gap_minutes) <= self._max_time_gap_minutes, gap_minutes

    # ── Persistence helpers ───────────────────────────────────────────────────

    @staticmethod
    def _normalize_scan_time(scan_time):
        """Return a tz-aware (UTC) scan_time so catalog isoformat comparisons match."""
        if scan_time is not None and scan_time.tzinfo is None:
            return scan_time.replace(tzinfo=UTC)
        return scan_time

    def _save_analysis_netcdf(self, ds, filepath: str, scan_time) -> str | None:
        """Write the analysis dataset to a NetCDF artifact in the repository."""
        assert self.repository is not None
        try:
            radar = self.config.downloader.radar
            filename_stem = Path(filepath).stem
            if scan_time is None:
                scan_time = datetime.now(UTC)

            ds.attrs.update(
                {
                    "source": str(filepath),
                    "radar": radar,
                    "description": "Radar analysis with segmentation and projections",
                }
            )

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

    @staticmethod
    def _attach_cell_uid_lut(projected_ds: xr.Dataset, tracked_cells: pd.DataFrame) -> xr.Dataset:
        """Add a 1D ``cell_uid`` variable mapping each cell_label index to its uid.

        Indexed by ``cell_label`` (0 = background -> "NONE", 1..n = cells), so the
        2D segmentation's local labels can be resolved to global uids from inside
        the analysis NetCDF. Returns ``projected_ds`` unchanged when there are no
        tracked cells.
        """
        if tracked_cells is None or tracked_cells.empty:
            return projected_ds
        max_label = int(projected_ds["cell_labels"].max())
        label_to_uid = dict(
            zip(tracked_cells["cell_label"].astype(int), tracked_cells["cell_uid"], strict=True)
        )
        lut = build_cell_uid_lut(label_to_uid, max_label)
        projected_ds["cell_uid"] = xr.DataArray(
            lut,
            dims=["cell_label"],
            coords={"cell_label": np.arange(max_label + 1)},
            attrs={
                "description": "cell_label index -> global cell_uid; index 0 = NONE (background)"
            },
        )
        return projected_ds

    def _save_results(self, result: dict, scan_time):
        """Save all pipeline outputs to the repository."""
        assert self.repository is not None
        scan_time = self._normalize_scan_time(scan_time)

        # Register the loader-written 3D gridded NetCDF as a queryable artifact so
        # enrich modules can open it by scan_time. The loader wrote the file; the
        # processor (which owns I/O) registers it in the catalog.
        grid_nc_path = result.get("grid_nc_path")
        if grid_nc_path and Path(grid_nc_path).exists():
            self.repository.register_artifact(
                product_type=ProductType.GRIDDED_NC,
                file_path=grid_nc_path,
                scan_time=scan_time,
                producer="ingest",
            )

        cell_stats = result.get("cell_stats")
        cell_adjacency = result.get("cell_adjacency")
        tracked_cells = result.get("tracked_cells")
        cell_events = result.get("cell_events")

        projected_ds = result.get("projected_ds")
        if projected_ds is not None:
            projected_ds = self._attach_cell_uid_lut(projected_ds, tracked_cells)
            filepath = self._scan_history[-1].get("nexrad_file", "") if self._scan_history else ""
            self._save_analysis_netcdf(projected_ds, filepath, scan_time)

        writer = RepositoryWriter(self.repository)

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
                cell_events_df=(cell_events if cell_events is not None else pd.DataFrame()),
                cell_adjacency_df=cell_adjacency,
            )

    # ── Results API (called by orchestrator on shutdown) ──────────────────────

    def get_results(self) -> pd.DataFrame:
        """Cell stats are in the repository; use DataClient to query them."""
        return pd.DataFrame()

    def save_results(self, filepath: str | None = None):
        """No-op: processor writes results to repository in _save_results."""
        pass

    def close_database(self):
        """No-op: repository manages its own connection lifecycle."""
        pass
