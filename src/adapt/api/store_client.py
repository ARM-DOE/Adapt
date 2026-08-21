# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""StoreClient — the read API over an Adapt data store.

The ONE queryable surface for every consumer (dashboard, TSE, notebooks,
scripts): registry-level discovery (collections, runs), catalog-level scan
timelines and rasters, and products-level science tables. New modules need no
client changes — their tables are discovered through ``tables()`` and read
through ``table()``; identity keys (``run_id``, ``scan_id``, ``cell_uid``) join
everything.

Read-only by construction: opening a client never creates directories,
databases, or rows (``annotate`` is the single, documented write exception).
File-descriptor discipline: exactly one SQLite connection per database, opened
lazily and closed by :meth:`close`; raster reads load the dataset into memory
and close the NetCDF file before returning.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr

from adapt.api.domain import Collection, Run, Scan, ScanBundle, ScanRaster, ScanRef, Track
from adapt.api.selection import FilterSpec
from adapt.persistence.errors import StoreError
from adapt.persistence.store import Store
from adapt.utils.time import from_scan_iso, to_scan_iso

__all__ = ["StoreClient", "TrackGraph"]

_FILTER_OPS = {"eq": "=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}


def _parse_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(frozen=True)
class TrackGraph:
    """The connected split/merge component around one cell."""

    run_id: str
    cell_uids: tuple[str, ...]
    tracks: tuple[Track, ...]
    events: pd.DataFrame


class _CollectionHandle:
    """Lazily opened read connections for one collection's two databases."""

    def __init__(self, collection_id: str, path: Path) -> None:
        self.collection_id = collection_id
        self.path = path
        self.objects_dir = path / "objects"
        self._catalog: sqlite3.Connection | None = None
        self._products: sqlite3.Connection | None = None

    def catalog(self) -> sqlite3.Connection:
        if self._catalog is None:
            self._catalog = _connect(self.path / "catalog.db")
        return self._catalog

    def products(self) -> sqlite3.Connection:
        if self._products is None:
            self._products = _connect(self.path / "products.db")
        return self._products

    def close(self) -> None:
        for conn in (self._catalog, self._products):
            if conn is not None:
                conn.close()
        self._catalog = None
        self._products = None


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


class StoreClient:
    """Read API over one Adapt store root."""

    def __init__(self, root: str | Path) -> None:
        self.root = Store.open(root).root  # loud on uninitialized/legacy roots
        self._registry: sqlite3.Connection | None = None
        self._handles: dict[str, _CollectionHandle] = {}

    # -- lifecycle -------------------------------------------------------------

    def close(self) -> None:
        """Release every connection; the client is reusable after close."""
        if self._registry is not None:
            self._registry.close()
            self._registry = None
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()

    def __enter__(self) -> StoreClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _registry_conn(self) -> sqlite3.Connection:
        if self._registry is None:
            self._registry = _connect(self.root / "registry.db")
        return self._registry

    def _handle(self, collection_id: str) -> _CollectionHandle:
        handle = self._handles.get(collection_id)
        if handle is None:
            path = self.root / "collections" / collection_id
            if not (path / "catalog.db").exists():
                raise StoreError(f"Unknown collection '{collection_id}' in store {self.root}")
            handle = _CollectionHandle(collection_id, path)
            self._handles[collection_id] = handle
        return handle

    # -- discovery ---------------------------------------------------------------

    def collections(self) -> list[Collection]:
        """Every registered collection."""
        rows = (
            self._registry_conn()
            .execute("SELECT * FROM collections ORDER BY collection_id")
            .fetchall()
        )
        return [
            Collection(
                collection_id=row["collection_id"],
                source_kind=row["source_kind"],
                location_lat=row["location_lat"],
                location_lon=row["location_lon"],
            )
            for row in rows
        ]

    def runs(self, collection: str | None = None) -> list[Run]:
        """Runs, newest first (a documented ordering contract)."""
        sql = "SELECT * FROM runs"
        params: list = []
        if collection is not None:
            sql += " WHERE collection_id = ?"
            params.append(collection)
        sql += " ORDER BY started_at DESC, rowid DESC"
        rows = self._registry_conn().execute(sql, params).fetchall()
        return [self._run_from_row(row) for row in rows]

    def latest_run(self, collection: str | None = None) -> Run | None:
        """The most recently started run, or None on an empty store."""
        runs = self.runs(collection)
        return runs[0] if runs else None

    def run(self, run_id: str) -> Run:
        row = (
            self._registry_conn()
            .execute("SELECT * FROM runs WHERE run_id = ?", (run_id,))
            .fetchone()
        )
        if row is None:
            raise StoreError(f"Run '{run_id}' does not exist")
        return self._run_from_row(row)

    def run_config(self, run_id: str) -> dict:
        """The resolved configuration this run was executed with.

        Parsed from the run provenance record (registry runs.config_json) —
        the single authoritative record of per-run settings such as
        ``global_.tracking_field`` and ``reader.field_map``. Keys use the
        InternalConfig field spelling (``global_``, not ``global``).
        """
        row = (
            self._registry_conn()
            .execute("SELECT config_json FROM runs WHERE run_id = ?", (run_id,))
            .fetchone()
        )
        if row is None:
            raise StoreError(f"Run '{run_id}' does not exist")
        raw = row["config_json"]
        if not raw:
            raise StoreError(f"Run '{run_id}' carries no config provenance")
        return json.loads(raw)

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> Run:
        started = _parse_iso(row["started_at"])
        assert started is not None  # NOT NULL column
        return Run(
            run_id=row["run_id"],
            collection_id=row["collection_id"],
            status=row["status"],
            started_at=started,
            ended_at=_parse_iso(row["ended_at"]),
            config_hash=row["config_hash"],
            pipeline_version=row["pipeline_version"],
        )

    # -- scans -------------------------------------------------------------------

    def scans(
        self,
        collection: str,
        run_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
    ) -> list[Scan]:
        """Complete scans, time-ordered; ``run_id=None`` unions across runs.

        Cross-run rows always carry their ``run_id`` — deduplication across
        reprocessed scans is the caller's decision.
        """
        handle = self._handle(collection)
        sql = "SELECT * FROM scans WHERE status = 'complete'"
        params: list = []
        if run_id is not None:
            sql += " AND run_id = ?"
            params.append(run_id)
        if start is not None:
            sql += " AND scan_time >= ?"
            params.append(to_scan_iso(start))
        if end is not None:
            sql += " AND scan_time <= ?"
            params.append(to_scan_iso(end))
        sql += " ORDER BY scan_time, run_id"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(int(limit))
        rows = handle.catalog().execute(sql, params).fetchall()
        return [
            Scan(
                scan_id=row["scan_id"],
                run_id=row["run_id"],
                collection_id=collection,
                scan_time=from_scan_iso(row["scan_time"]),
                source_file_name=row["source_file_name"],
                status=row["status"],
                start_time=_parse_iso(row["start_time"]),
                end_time=_parse_iso(row["end_time"]),
            )
            for row in rows
        ]

    def scan_timeline(self, collection: str, run_id: str) -> list[ScanRef]:
        """The run's ordered, complete-only scan timeline (the dashboard seam)."""
        return [
            ScanRef(run_id=s.run_id, scan_id=s.scan_id, scan_time=s.scan_time)
            for s in self.scans(collection, run_id=run_id)
        ]

    def scans_since(
        self, collection: str, run_id: str, after: ScanRef | None = None
    ) -> list[ScanRef]:
        """Live-follow: complete scans newer than the watermark (all when None)."""
        timeline = self.scan_timeline(collection, run_id)
        if after is None:
            return timeline
        return [ref for ref in timeline if ref.scan_time > after.scan_time]

    def _scan(self, collection: str, run_id: str, scan_id: str) -> Scan:
        row = (
            self._handle(collection)
            .catalog()
            .execute("SELECT * FROM scans WHERE run_id = ? AND scan_id = ?", (run_id, scan_id))
            .fetchone()
        )
        if row is None:
            raise StoreError(f"Scan '{scan_id}' is not registered for run '{run_id}'")
        return Scan(
            scan_id=row["scan_id"],
            run_id=row["run_id"],
            collection_id=collection,
            scan_time=from_scan_iso(row["scan_time"]),
            source_file_name=row["source_file_name"],
            status=row["status"],
            start_time=_parse_iso(row["start_time"]),
            end_time=_parse_iso(row["end_time"]),
        )

    # -- rasters and bundles -------------------------------------------------------

    def open_scan_raster(
        self, collection: str, run_id: str, scan_id: str, product: str = "segmentation2d"
    ) -> ScanRaster:
        """One scan's raster product, fully loaded in memory (no retained handle)."""
        handle = self._handle(collection)
        row = (
            handle.catalog()
            .execute(
                "SELECT * FROM artifacts WHERE run_id = ? AND scan_id = ? AND artifact_type = ? "
                "ORDER BY created_at DESC, artifact_id DESC LIMIT 1",
                (run_id, scan_id, product),
            )
            .fetchone()
        )
        if row is None:
            raise StoreError(
                f"Scan '{scan_id}' has no {product} artifact in run '{run_id}' "
                f"of collection '{collection}'"
            )
        dataset = xr.load_dataset(handle.objects_dir / row["object_name"])
        ref = ScanRef(
            run_id=run_id, scan_id=scan_id, scan_time=from_scan_iso(row["observation_time"])
        )
        return ScanRaster(dataset, ref, product)

    def scan_bundle(self, run_id: str, scan_id: str, collection: str) -> ScanBundle:
        """Everything for one COMPLETE scan, resolved through catalog links."""
        scan = self._scan(collection, run_id, scan_id)
        if scan.status != "complete":
            raise StoreError(
                f"Scan '{scan_id}' of run '{run_id}' is not complete (status '{scan.status}')"
            )
        raster = self.open_scan_raster(collection, run_id, scan_id)
        cells = self.cells_at_scan(run_id, scan_id, collection)
        tracks = [
            self._track_from_row(row)
            for uid in (cells["cell_uid"].tolist() if not cells.empty else [])
            for row in self._handle(collection)
            .products()
            .execute("SELECT * FROM cell_tracks WHERE run_id = ? AND cell_uid = ?", (run_id, uid))
            .fetchall()
        ]
        return ScanBundle(scan=scan, segmentation=raster.dataset, cells=cells, tracks=tracks)

    # -- cells and tracks ------------------------------------------------------------

    def cells_at_scan(self, run_id: str, scan_id: str, collection: str) -> pd.DataFrame:
        """All tracked-cell rows for one scan; empty before tracking ever ran."""
        return self._tracking_frame(
            collection,
            "SELECT * FROM cells_by_scan WHERE run_id = ? AND scan_id = ?",
            (run_id, scan_id),
            table="cells_by_scan",
        )

    def _tracking_frame(
        self, collection: str, sql: str, params: tuple, *, table: str
    ) -> pd.DataFrame:
        """A tracking-table read that is empty (not an error) before first freeze."""
        handle = self._handle(collection)
        exists = (
            handle.products()
            .execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,))
            .fetchone()
        )
        if exists is None:
            return pd.DataFrame()
        return pd.read_sql_query(sql, handle.products(), params=list(params))

    @staticmethod
    def _track_from_row(row: sqlite3.Row) -> Track:
        first_seen = from_scan_iso(row["first_seen_time"])
        last_seen = from_scan_iso(row["last_seen_time"])
        return Track(
            run_id=row["run_id"],
            cell_uid=row["cell_uid"],
            first_seen=first_seen,
            last_seen=last_seen,
            n_scans=row["n_scans"],
            lifetime_s=(last_seen - first_seen).total_seconds(),
            origin_type=row["origin_type"],
            termination_type=row["termination_type"],
            max_area_km2=row["max_area_sqkm"] or 0.0,
            max_reflectivity_dbz=row["max_reflectivity"] or 0.0,
        )

    # -- generic table reads -------------------------------------------------------

    def tables(self, collection: str) -> pd.DataFrame:
        """Every product table's frozen declaration (name, owner, granularity, keys)."""
        return pd.read_sql_query(
            "SELECT table_name, owner_module, granularity, primary_key, index_columns, "
            " columns_json, frozen_at FROM table_schemas ORDER BY table_name",
            self._handle(collection).products(),
        )

    def table(
        self,
        name: str,
        collection: str,
        run_id: str | None = None,
        filters: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Rows from one product table; every filter column is validated.

        Filter values: scalar (equality), list/tuple (IN), or
        ``{"op": "eq"|"gt"|"ge"|"lt"|"le"|"in", "value": ...}``.
        """
        handle = self._handle(collection)
        columns = self._table_columns(handle, name)

        clauses: list[str] = []
        params: list = []
        merged: dict[str, Any] = dict(filters or {})
        if run_id is not None:
            merged["run_id"] = run_id
        for column, condition in merged.items():
            if column not in columns:
                raise StoreError(f"Table '{name}' has no column '{column}'")
            clause, clause_params = _compile_condition(column, condition)
            clauses.append(clause)
            params.extend(clause_params)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return pd.read_sql_query(f"SELECT * FROM {name}{where}", handle.products(), params=params)

    def tracks(self, collection: str, run_id: str | None = None) -> pd.DataFrame:
        """The track population; ``run_id=None`` unions across runs (rows carry it)."""
        if run_id is None:
            return self._tracking_frame(
                collection,
                "SELECT * FROM cell_tracks ORDER BY run_id, first_seen_time",
                (),
                table="cell_tracks",
            )
        return self._tracking_frame(
            collection,
            "SELECT * FROM cell_tracks WHERE run_id = ? ORDER BY first_seen_time",
            (run_id,),
            table="cell_tracks",
        )

    def track(self, run_id: str, cell_uid: str, collection: str) -> Track:
        row = (
            self._handle(collection)
            .products()
            .execute(
                "SELECT * FROM cell_tracks WHERE run_id = ? AND cell_uid = ?", (run_id, cell_uid)
            )
            .fetchone()
        )
        if row is None:
            raise StoreError(f"Track '{cell_uid}' does not exist in run '{run_id}'")
        return self._track_from_row(row)

    def cells(self, run_id: str, collection: str) -> pd.DataFrame:
        """One run's full per-scan cell table, time-ordered; empty before tracking."""
        return self._tracking_frame(
            collection,
            "SELECT * FROM cells_by_scan WHERE run_id = ? ORDER BY scan_time",
            (run_id,),
            table="cells_by_scan",
        )

    def track_history(self, run_id: str, cell_uid: str, collection: str) -> pd.DataFrame:
        """One cell's per-scan rows across every scan it existed in, time-ordered."""
        return self._tracking_frame(
            collection,
            "SELECT * FROM cells_by_scan WHERE run_id = ? AND cell_uid = ? ORDER BY scan_time",
            (run_id, cell_uid),
            table="cells_by_scan",
        )

    def track_events(
        self, run_id: str, cell_uid: str | None = None, collection: str | None = None
    ) -> pd.DataFrame:
        """Lineage events for a run; with ``cell_uid``, edges touching that cell."""
        if collection is None:
            raise StoreError("track_events requires the collection")
        if cell_uid is None:
            return self._tracking_frame(
                collection,
                "SELECT * FROM cell_events WHERE run_id = ? ORDER BY event_id",
                (run_id,),
                table="cell_events",
            )
        return self._tracking_frame(
            collection,
            "SELECT * FROM cell_events WHERE run_id = ? "
            "AND (source_cell_uid = ? OR target_cell_uid = ?) ORDER BY event_id",
            (run_id, cell_uid, cell_uid),
            table="cell_events",
        )

    def track_graph(self, run_id: str, cell_uid: str, collection: str) -> TrackGraph:
        """The full split/merge connected component around one cell.

        Walks ``cell_events`` edges (splits, merges, continuations) in both
        directions until closure, then returns every member cell's track and
        every edge inside the component.
        """
        handle = self._handle(collection)
        rows = (
            handle.products()
            .execute(
                """
            WITH RECURSIVE component(uid) AS (
                VALUES (?)
                UNION
                SELECT CASE WHEN e.source_cell_uid = c.uid
                            THEN e.target_cell_uid ELSE e.source_cell_uid END
                FROM cell_events e
                JOIN component c
                  ON (e.source_cell_uid = c.uid OR e.target_cell_uid = c.uid)
                WHERE e.run_id = ?
            )
            SELECT uid FROM component WHERE uid IS NOT NULL ORDER BY uid
            """,
                (cell_uid, run_id),
            )
            .fetchall()
        )
        uids = tuple(row["uid"] for row in rows)
        placeholders = ", ".join("?" * len(uids))
        tracks = tuple(
            self._track_from_row(row)
            for row in handle.products()
            .execute(
                f"SELECT * FROM cell_tracks WHERE run_id = ? AND cell_uid IN ({placeholders}) "
                "ORDER BY first_seen_time",
                (run_id, *uids),
            )
            .fetchall()
        )
        events = pd.read_sql_query(
            f"SELECT * FROM cell_events WHERE run_id = ? "
            f"AND (source_cell_uid IN ({placeholders}) OR target_cell_uid IN ({placeholders})) "
            "ORDER BY event_id",
            handle.products(),
            params=[run_id, *uids, *uids],
        )
        return TrackGraph(run_id=run_id, cell_uids=uids, tracks=tracks, events=events)

    # -- population selection and annotations -----------------------------------------

    def select(self, run_id: str, criteria: FilterSpec, collection: str) -> pd.DataFrame:
        """Tracks of one run matching the FilterSpec (ranges, IN-lists, tags)."""
        clause, params = criteria.to_sql_where()
        conditions = [clause.removeprefix("WHERE ")] if clause else []
        if criteria.required_tags:
            for tag in sorted(criteria.required_tags):
                conditions.append(
                    "EXISTS (SELECT 1 FROM annotations a WHERE a.run_id = cell_tracks.run_id "
                    "AND a.cell_uid = cell_tracks.cell_uid AND a.tag = ?)"
                )
                params.append(tag)
        conditions.append("run_id = ?")
        params.append(run_id)
        return self._tracking_frame(
            collection,
            f"SELECT * FROM cell_tracks WHERE {' AND '.join(conditions)} ORDER BY first_seen_time",
            tuple(params),
            table="cell_tracks",
        )

    def annotate(
        self,
        run_id: str,
        cell_uid: str,
        tag: str,
        note: str | None = None,
        collection: str | None = None,
    ) -> None:
        """Attach a user tag to a cell — the client's single write exception."""
        if collection is None:
            raise StoreError("annotate requires the collection")
        conn = self._handle(collection).products()
        conn.execute(
            "INSERT INTO annotations (run_id, cell_uid, tag, note, created_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(run_id, cell_uid, tag) DO UPDATE SET note = excluded.note",
            (run_id, cell_uid, tag, note, datetime.now(UTC).isoformat()),
        )
        conn.commit()

    def annotations(self, run_id: str, collection: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM annotations WHERE run_id = ? ORDER BY cell_uid, tag",
            self._handle(collection).products(),
            params=[run_id],
        )

    # -- read-only SQL ---------------------------------------------------------------

    def sql(self, query: str, collection: str) -> pd.DataFrame:
        """Arbitrary read-only SQL over products.db (catalog attached as ``catalog``).

        The scientists' escape hatch: full SELECT generality over every module's
        tables plus the ``catalog.*`` discovery tables, on a read-only
        connection opened per call — writes are rejected by SQLite itself.
        """
        handle = self._handle(collection)
        conn = sqlite3.connect(
            f"file:{handle.path / 'products.db'}?mode=ro", uri=True, check_same_thread=False
        )
        try:
            conn.execute(
                "ATTACH DATABASE ? AS catalog",
                (f"file:{handle.path / 'catalog.db'}?mode=ro",),
            )
            return pd.read_sql_query(query, conn)
        finally:
            conn.close()

    # -- status ------------------------------------------------------------------------

    def artifacts(
        self,
        collection: str,
        run_id: str | None = None,
        artifact_type: str | None = None,
    ) -> list[dict]:
        """Artifact metadata rows (no paths are needed to read them — see rasters)."""
        clauses, params = [], []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if artifact_type is not None:
            clauses.append("artifact_type = ?")
            params.append(artifact_type)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = (
            self._handle(collection)
            .catalog()
            .execute(
                f"SELECT * FROM artifacts{where} ORDER BY observation_time, artifact_id", params
            )
            .fetchall()
        )
        return [dict(row) for row in rows]

    def is_pipeline_running(self, collection: str | None = None) -> bool:
        """True when any run (optionally of one collection) is in status running."""
        return any(run.status == "running" for run in self.runs(collection))

    def pipeline_progress(self, collection: str) -> dict:
        """The latest run's lifecycle row plus its complete-scan count."""
        latest = self.latest_run(collection)
        if latest is None:
            raise StoreError(f"No runs recorded for collection '{collection}'")
        return {
            "run_id": latest.run_id,
            "status": latest.status,
            "started_at": latest.started_at,
            "ended_at": latest.ended_at,
            "complete_scans": len(self.scans(collection, run_id=latest.run_id)),
        }

    def repository_info(self) -> dict:
        """Store-level summary: root, collections, run count."""
        return {
            "root": str(self.root),
            "collections": [c.collection_id for c in self.collections()],
            "runs": len(self.runs()),
        }

    def _table_columns(self, handle: _CollectionHandle, name: str) -> set[str]:
        known = (
            handle.products()
            .execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,))
            .fetchone()
        )
        if known is None:
            raise StoreError(
                f"Table '{name}' does not exist in collection "
                f"'{handle.collection_id}' (see tables())"
            )
        return {row["name"] for row in handle.products().execute(f"PRAGMA table_info({name})")}


def _compile_condition(column: str, condition: Any) -> tuple[str, list]:
    """One validated filter condition -> (SQL clause, params)."""
    if isinstance(condition, Mapping):
        op = condition.get("op")
        value = condition.get("value")
    elif isinstance(condition, (list, tuple, set, frozenset)):
        op, value = "in", condition
    else:
        op, value = "eq", condition

    if op == "in":
        if not isinstance(value, (list, tuple, set, frozenset)):
            raise ValueError(f"Filter op 'in' for column '{column}' needs a list of values")
        values = sorted(value) if isinstance(value, (set, frozenset)) else list(value)
        placeholders = ", ".join("?" * len(values))
        return f"{column} IN ({placeholders})", values
    if op not in _FILTER_OPS:
        raise ValueError(
            f"Unknown filter op '{op}' for column '{column}' "
            f"(expected one of {', '.join((*_FILTER_OPS, 'in'))})"
        )
    if isinstance(value, datetime):
        value = to_scan_iso(value)
    return f"{column} {_FILTER_OPS[op]} ?", [value]
