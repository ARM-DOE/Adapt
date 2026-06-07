# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""RepositoryClient — read-only access to an ADAPT repository.

Discovers data through the two-tier database system:
- Root-level registry (adapt_registry.db): runs and radars.
- Radar-level catalogs (catalog.db): items, scans, tracks, annotations.

Example usage::

    from adapt.api import RepositoryClient, FilterSpec

    client = RepositoryClient("/data/radar_output")

    for run in client.runs():
        print(run.run_id, run.status)

    tracks_df = client.tracks(run_id)
    severe = client.select(run_id, FilterSpec(max_refl_min_dbz=55.0))

    bundle = client.scan_bundle(scan_time, radar="KDIX")

    # Raw SQL escape hatch (DuckDB over Parquet)
    df = client.query("SELECT * FROM analysis2d WHERE refl_max > 40")
"""

from __future__ import annotations

import contextlib
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

import pandas as pd
import xarray as xr

from adapt.api.domain import Run, Scan, ScanBundle, Track
from adapt.api.selection import FilterSpec
from adapt.persistence.catalog import RadarCatalog
from adapt.persistence.registry import RepositoryRegistry
from adapt.persistence.track_store import TrackStore

__all__ = ["RepositoryClient"]

logger = logging.getLogger(__name__)


class RepositoryClient:
    """Read-only interface for an ADAPT repository.

    Thread-safe for notebook usage.
    Discovers all data through catalog databases — no filesystem inspection.

    Parameters
    ----------
    repository_root : str or Path
        Root directory of the ADAPT repository.
    """

    def __init__(self, repository_root: str | Path) -> None:
        self.root_dir = Path(repository_root).resolve()
        if not self.root_dir.exists():
            raise FileNotFoundError(f"Repository not found: {self.root_dir}")

        self._registry = RepositoryRegistry.get_instance(self.root_dir)
        self._duckdb_conn: duckdb.DuckDBPyConnection | None = None
        self._radar_catalogs: dict[str, RadarCatalog] = {}

        logger.info("RepositoryClient initialised at %s", self.root_dir)

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _catalog(self, radar: str) -> RadarCatalog:
        if radar not in self._radar_catalogs:
            radar_dir = self.root_dir / radar
            if not radar_dir.exists():
                raise FileNotFoundError(f"Radar directory not found: {radar_dir}")
            self._radar_catalogs[radar] = RadarCatalog(radar_dir)
        return self._radar_catalogs[radar]

    def _track_store(self, radar: str) -> TrackStore:
        catalog = self._catalog(radar)
        return TrackStore(catalog.db_path)

    def _duckdb(self):
        if self._duckdb_conn is None:
            import duckdb

            self._duckdb_conn = duckdb.connect(":memory:")
        return self._duckdb_conn

    def _resolve_radar(self, radar: str | None) -> str:
        if radar:
            return radar
        available = self.radars()
        if not available:
            raise ValueError("No radars found in repository")
        return available[0]

    @staticmethod
    def _parse_dt(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.min

    # =========================================================================
    # Discovery
    # =========================================================================

    def radars(self) -> list[str]:
        """Return all registered radar IDs."""
        df = self._registry.list_radars()
        return df["radar"].tolist() if not df.empty else []

    def runs(self, radar: str | None = None) -> list[Run]:
        """Return all runs as domain objects, optionally filtered by radar."""
        df = self._registry.list_runs(radar=radar)
        if df.empty:
            return []
        result = []
        for _, row in df.iterrows():
            result.append(
                Run(
                    run_id=row["run_id"],
                    radar_id=row.get("radar", radar or ""),
                    start_time=self._parse_dt(row.get("start_time")),
                    end_time=self._parse_dt(row["end_time"]) if row.get("end_time") else None,
                    status=str(row.get("status", "unknown")),
                    mode=str(row.get("mode", "unknown")),
                )
            )
        return result

    def run(self, run_id: str) -> Run:
        """Return metadata for a specific run."""
        df = self._registry.list_runs()
        row_df = df[df["run_id"] == run_id]
        if row_df.empty:
            raise ValueError(f"Run '{run_id}' not found")
        row = row_df.iloc[0]
        return Run(
            run_id=run_id,
            radar_id=str(row.get("radar", "")),
            start_time=self._parse_dt(row.get("start_time")),
            end_time=self._parse_dt(row["end_time"]) if row.get("end_time") else None,
            status=str(row.get("status", "unknown")),
            mode=str(row.get("mode", "unknown")),
        )

    # =========================================================================
    # Track access
    # =========================================================================

    def tracks(self, run_id: str, radar: str | None = None) -> pd.DataFrame:
        """Return the full cell_tracks lifecycle-summary table for a run."""
        radar = self._resolve_radar(radar)
        return self._track_store(radar).get_cell_tracks(run_id)

    def track(self, run_id: str, cell_uid: str, radar: str | None = None) -> Track:
        """Return the lifecycle summary for a single track as a domain object."""
        radar = self._resolve_radar(radar)
        df = self._track_store(radar).get_cell_tracks(run_id)
        row_df = df[df["cell_uid"] == cell_uid]
        if row_df.empty:
            raise ValueError(f"Track '{cell_uid}' not found in run '{run_id}'")
        row = row_df.iloc[0]
        first = self._parse_dt(row.get("first_seen_time"))
        last = self._parse_dt(row.get("last_seen_time"))
        lifetime_s = max((last - first).total_seconds(), 0.0)
        return Track(
            run_id=run_id,
            cell_uid=cell_uid,
            first_seen=first,
            last_seen=last,
            n_scans=int(row.get("n_scans", 0)),
            lifetime_s=lifetime_s,
            origin_type=str(row.get("origin_type", "UNKNOWN")),
            termination_type=str(row.get("termination_type", "UNKNOWN")),
            max_area_km2=float(row.get("max_area_sqkm", 0.0)),
            max_reflectivity_dbz=float(row.get("max_reflectivity", 0.0)),
        )

    def track_history(self, run_id: str, cell_uid: str, radar: str | None = None) -> pd.DataFrame:
        """Return all scan rows for one track, ordered by scan_time."""
        radar = self._resolve_radar(radar)
        return self._track_store(radar).get_track_history(run_id, cell_uid)

    def track_events(self, run_id: str, cell_uid: str, radar: str | None = None) -> pd.DataFrame:
        """Return lineage events for one track."""
        radar = self._resolve_radar(radar)
        return self._track_store(radar).get_cell_events(run_id, cell_uid)

    # =========================================================================
    # Selection
    # =========================================================================

    def select(self, run_id: str, criteria: FilterSpec, radar: str | None = None) -> pd.DataFrame:
        """Return the subset of cell_tracks satisfying *criteria*.

        Parameters
        ----------
        run_id:
            Run to filter.
        criteria:
            Immutable :class:`FilterSpec`.
        radar:
            Radar ID; resolved automatically if omitted.

        Returns
        -------
        DataFrame
            Same schema as :meth:`tracks`, filtered to matching rows.
        """
        if criteria.is_empty():
            return self.tracks(run_id, radar=radar)

        radar = self._resolve_radar(radar)
        store = self._track_store(radar)
        conn = store._connect()

        where_clause, params = criteria.to_sql_where()

        # Build base: run_id filter + criteria
        if where_clause:
            # where_clause starts with "WHERE ..."
            inner = where_clause[len("WHERE ") :]
            sql = f"SELECT * FROM cell_tracks WHERE run_id = ? AND {inner}"
        else:
            sql = "SELECT * FROM cell_tracks WHERE run_id = ?"

        all_params: list = [run_id, *params]

        # required_tags: JOIN against annotations table
        if criteria.required_tags:
            for tag in sorted(criteria.required_tags):
                sql += (
                    " AND EXISTS (SELECT 1 FROM annotations a"
                    " WHERE a.run_id = cell_tracks.run_id"
                    " AND a.cell_uid = cell_tracks.cell_uid"
                    " AND a.tag = ?)"
                )
                all_params.append(tag)

        with store._lock:
            rows = conn.execute(sql, all_params).fetchall()

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    # =========================================================================
    # Scan access
    # =========================================================================

    def scans(
        self,
        radar: str,
        run_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 200,
    ) -> list[Scan]:
        """Return scan metadata records for a radar."""
        catalog = self._catalog(radar)
        try:
            df = catalog.list_scans(
                run_id=run_id,
                start_time=start,
                end_time=end,
                limit=limit,
            )
        except Exception:
            df = self._scans_from_items(radar, run_id, start, end, limit)

        result = []
        for _, row in df.iterrows():
            result.append(
                Scan(
                    scan_time=self._parse_dt(row.get("scan_time")),
                    radar_id=radar,
                    run_id=str(row.get("run_id", run_id or "")),
                    n_cells=int(row.get("num_cells", 0)),
                    max_reflectivity=float(row.get("max_reflectivity", 0.0)),
                    has_tracks=bool(row.get("has_tracks", False)),
                )
            )
        return result

    def _scans_from_items(
        self,
        radar: str,
        run_id: str | None,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> pd.DataFrame:
        catalog = self._catalog(radar)
        conn = catalog._get_connection()
        conditions = ["item_type = 'segmentation2d'", "status = 'complete'"]
        params: list = []
        if run_id:
            conditions.append("run_id = ?")
            params.append(run_id)
        if start:
            conditions.append("scan_time >= ?")
            params.append(start.isoformat())
        if end:
            conditions.append("scan_time <= ?")
            params.append(end.isoformat())
        params.append(limit)
        where = " AND ".join(conditions)
        with catalog._lock:
            rows = conn.execute(
                f"SELECT DISTINCT scan_time, run_id FROM items WHERE {where} "
                f"ORDER BY scan_time DESC LIMIT ?",
                params,
            ).fetchall()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    def scan_bundle(self, scan_time: datetime | str, radar: str | None = None) -> ScanBundle:
        """Return all data products for a single scan."""
        radar = self._resolve_radar(radar)
        scan_time_dt = self._parse_dt(scan_time)

        catalog = self._catalog(radar)
        scan_record = None
        with contextlib.suppress(Exception):
            scan_record = catalog.get_scan(scan_time_dt)

        if scan_record:
            return self._bundle_from_scan_record(scan_record, radar, scan_time_dt)
        return self._bundle_from_items(radar, scan_time_dt)

    def _bundle_from_scan_record(
        self, scan_record: dict, radar: str, scan_time: datetime
    ) -> ScanBundle:
        meta = Scan(
            scan_time=scan_time,
            radar_id=radar,
            run_id=str(scan_record.get("run_id", "")),
            n_cells=int(scan_record.get("num_cells", 0)),
            max_reflectivity=float(scan_record.get("max_reflectivity", 0.0)),
            has_tracks=bool(scan_record.get("has_tracks", False)),
        )
        seg = self._load_item_file(radar, scan_record.get("segmentation2d_item_id"))
        cells = self._load_item_file(radar, scan_record.get("analysis2d_item_id"))

        tracks: list[Track] = []
        run_id = scan_record.get("run_id")
        if run_id:
            store = self._track_store(radar)
            scan_cells = store.get_cells_by_scan(run_id, scan_time)
            for uid in scan_cells["cell_uid"].dropna().unique() if not scan_cells.empty else []:
                with contextlib.suppress(Exception):
                    tracks.append(self.track(run_id, str(uid), radar=radar))

        return ScanBundle(scan=meta, segmentation=seg, cells=cells, tracks=tracks)

    def _bundle_from_items(self, radar: str, scan_time: datetime) -> ScanBundle:
        catalog = self._catalog(radar)
        conn = catalog._get_connection()
        scan_time_str = scan_time.isoformat()

        def _nearest(item_type: str) -> dict | None:
            with catalog._lock:
                row = conn.execute(
                    "SELECT * FROM items WHERE item_type = ? AND status = 'complete' "
                    "ORDER BY ABS(julianday(scan_time) - julianday(?)) LIMIT 1",
                    (item_type, scan_time_str),
                ).fetchone()
            return dict(row) if row else None

        seg_item = _nearest("segmentation2d")
        analysis_item = _nearest("analysis2d")

        seg = None
        if seg_item:
            path = self.root_dir / radar / seg_item["file_path"]
            if path.exists():
                seg = xr.open_dataset(path)

        cells = None
        if analysis_item:
            path = self.root_dir / radar / analysis_item["file_path"]
            if path.exists():
                cells = pd.read_parquet(path, engine="pyarrow")

        run_id = seg_item.get("run_id", "") if seg_item else ""
        meta = Scan(
            scan_time=scan_time,
            radar_id=radar,
            run_id=run_id,
            n_cells=len(cells) if cells is not None else 0,
            max_reflectivity=0.0,
            has_tracks=False,
        )
        return ScanBundle(scan=meta, segmentation=seg, cells=cells, tracks=[])

    def _load_item_file(self, radar: str, item_id: str | None) -> Any:
        if not item_id:
            return None
        catalog = self._catalog(radar)
        conn = catalog._get_connection()
        with catalog._lock:
            row = conn.execute("SELECT * FROM items WHERE item_id = ?", (item_id,)).fetchone()
        if not row:
            return None
        path = self.root_dir / radar / dict(row)["file_path"]
        if not path.exists():
            return None
        if path.suffix == ".parquet":
            return pd.read_parquet(path, engine="pyarrow")
        if path.suffix in {".nc", ".nc4", ".netcdf"}:
            return xr.open_dataset(path)
        return None

    # =========================================================================
    # Annotations
    # =========================================================================

    def annotate(
        self,
        run_id: str,
        cell_uid: str,
        radar: str | None = None,
        tag: str | None = None,
        note: str | None = None,
    ) -> None:
        """Persist a tag or note against a track in the radar catalog."""
        if tag is None and note is None:
            raise ValueError("Provide at least one of tag or note")

        radar = self._resolve_radar(radar)
        catalog = self._catalog(radar)
        conn = catalog._get_connection()

        with catalog._lock:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS annotations ("
                "  run_id TEXT NOT NULL, cell_uid TEXT NOT NULL,"
                "  tag TEXT, note TEXT,"
                "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,"
                "  PRIMARY KEY (run_id, cell_uid, tag)"
                ")"
            )
            conn.execute(
                "INSERT OR REPLACE INTO annotations (run_id, cell_uid, tag, note)"
                " VALUES (?, ?, ?, ?)",
                (run_id, cell_uid, tag, note),
            )
            conn.commit()

    def annotations(self, run_id: str, radar: str | None = None) -> pd.DataFrame:
        """Return all annotations for a run as a DataFrame."""
        radar = self._resolve_radar(radar)
        catalog = self._catalog(radar)
        conn = catalog._get_connection()
        with catalog._lock:
            try:
                rows = conn.execute(
                    "SELECT * FROM annotations WHERE run_id = ?", (run_id,)
                ).fetchall()
            except Exception:
                return pd.DataFrame(columns=["run_id", "cell_uid", "tag", "note", "created_at"])
        if not rows:
            return pd.DataFrame(columns=["run_id", "cell_uid", "tag", "note", "created_at"])
        return pd.DataFrame([dict(r) for r in rows])

    # =========================================================================
    # Raw SQL escape hatch
    # =========================================================================

    def query(self, sql: str, radar: str | None = None) -> pd.DataFrame:
        """Execute a SELECT query over Parquet tables via DuckDB."""
        if not sql.strip().upper().startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed")

        radar = self._resolve_radar(radar)
        conn = self._duckdb()
        catalog = self._catalog(radar)

        item_types = self._registry.list_item_types()
        for item_type in item_types:
            info = self._registry.get_item_type_info(item_type)
            if not info or info.get("storage_format") != "parquet":
                continue
            items = catalog.query_items(item_type=item_type, status="complete")
            if items.empty:
                continue
            paths = [str(self.root_dir / radar / r["file_path"]) for _, r in items.iterrows()]
            if paths:
                try:
                    conn.execute(f"DROP VIEW IF EXISTS {item_type}")
                    conn.execute(f"CREATE VIEW {item_type} AS SELECT * FROM read_parquet({paths})")
                except Exception as exc:
                    logger.warning("Could not create view for %s: %s", item_type, exc)

        return conn.execute(sql).fetchdf()

    # =========================================================================
    # Status
    # =========================================================================

    def is_pipeline_running(self, radar: str | None = None) -> bool:
        """Return True if a pipeline process appears to be active."""
        pid_file = Path.home() / ".adapt" / "pipeline.pid"
        if pid_file.exists():
            try:
                import os

                pid = int(pid_file.read_text().strip())
                os.kill(pid, 0)
                return True
            except (ValueError, OSError, ProcessLookupError):
                pass

        try:
            radar = self._resolve_radar(radar)
            runs_df = self._registry.list_runs(radar=radar)
            if not runs_df.empty and (runs_df["status"] == "running").any():
                return True
            if not runs_df.empty:
                catalog = self._catalog(radar)
                progress = catalog.get_progress(runs_df.iloc[0]["run_id"])
                if progress and progress.get("last_updated"):
                    last = datetime.fromisoformat(progress["last_updated"].replace("Z", "+00:00"))
                    return (datetime.now(UTC) - last).total_seconds() < 60
        except Exception as exc:
            logger.debug("Error checking pipeline status: %s", exc)

        return False

    def pipeline_progress(
        self, radar: str | None = None, run_id: str | None = None
    ) -> dict[str, Any]:
        """Return detailed pipeline progress."""
        try:
            radar = self._resolve_radar(radar)
            runs_df = self._registry.list_runs(radar=radar)
            if runs_df.empty:
                return {"is_running": False, "error": "No runs found"}
            if not run_id:
                run_id = runs_df.iloc[0]["run_id"]
            catalog = self._catalog(radar)
            progress = catalog.get_progress(run_id) or {}
            return {
                "is_running": self.is_pipeline_running(radar=radar),
                "run_id": run_id,
                "radar": radar,
                **progress,
            }
        except Exception as exc:
            return {"is_running": False, "error": str(exc)}

    def repository_info(self) -> dict[str, Any]:
        """Return summary information about the repository."""
        radar_list = self.radars()
        return {
            "path": str(self.root_dir),
            "is_initialized": (self.root_dir / "adapt_registry.db").exists(),
            "num_radars": len(radar_list),
            "radars": radar_list,
            "num_runs": len(self.runs()),
        }

    # =========================================================================
    # Streaming (notebook convenience)
    # =========================================================================

    def stream(self, sql: str, poll_interval: int = 5, radar: str | None = None):
        """Yield new DataFrame batches as the pipeline produces data (generator)."""
        last_seen_time = None
        while True:
            try:
                wrapped = (
                    f"SELECT * FROM ({sql}) WHERE scan_time > '{last_seen_time}'"
                    f" ORDER BY scan_time ASC"
                    if last_seen_time
                    else f"SELECT * FROM ({sql}) ORDER BY scan_time ASC LIMIT 1"
                )
                result = self.query(wrapped, radar=radar)
                if not result.empty:
                    if "scan_time" in result.columns:
                        last_seen_time = result["scan_time"].max()
                    yield result
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                break
            except Exception as exc:
                logger.error("Stream error: %s", exc)
                time.sleep(poll_interval)

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def close(self) -> None:
        """Close all database connections."""
        if self._duckdb_conn:
            self._duckdb_conn.close()
            self._duckdb_conn = None
        for catalog in self._radar_catalogs.values():
            catalog.close()
        self._radar_catalogs.clear()
        logger.info("RepositoryClient connections closed")
