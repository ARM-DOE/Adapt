# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Store-level registry: collections and the single run lifecycle.

Backs ``{root}/registry.db`` (schema ``store_registry.sql``). This is the ONE
run lifecycle in the store — module executions and run events are recorded
against it, and per-collection discovery lives in the collection catalog.

Thread-safe singleton per store root (WAL); ``close_all()`` releases the
process-wide cache deterministically (FD-leak lesson from the old registry).
"""

import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from adapt.persistence.sqlite_store import SqliteStore
from adapt.persistence.store import REGISTRY_FILENAME, StoreError, _reject_legacy_root

__all__ = ["RunStart", "StoreRegistry"]

_TERMINAL_STATUSES = ("completed", "cancelled", "failed")

_instance_cache: dict[str, "StoreRegistry"] = {}
_cache_lock = threading.Lock()


@dataclass(frozen=True)
class RunStart:
    """Everything the registry records when a run begins."""

    run_id: str
    collection_id: str
    config_hash: str
    config_json: str
    pipeline_version: str
    environment_json: str


class StoreRegistry(SqliteStore):
    """Registry over ``{root}/registry.db``; requires an initialized store."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        db_path = self.root / REGISTRY_FILENAME
        _reject_legacy_root(self.root)
        if not db_path.exists():
            raise StoreError(
                f"No Adapt store at {self.root}: {REGISTRY_FILENAME} not found. "
                "Run 'adapt init' first."
            )
        super().__init__(db_path, "store_registry.sql")

    @classmethod
    def get_instance(cls, root: str | Path) -> "StoreRegistry":
        """Return the cached registry for ``root``, creating it on first use."""
        key = str(Path(root).resolve())
        with _cache_lock:
            if key not in _instance_cache:
                _instance_cache[key] = cls(root)
            return _instance_cache[key]

    @classmethod
    def close_all(cls) -> None:
        """Close every cached registry connection and empty the cache."""
        with _cache_lock:
            for registry in _instance_cache.values():
                registry.close()
            _instance_cache.clear()

    # -- collections ---------------------------------------------------------

    def register_collection(
        self,
        collection_id: str,
        *,
        source_kind: str,
        lat: float | None = None,
        lon: float | None = None,
        metadata_json: str | None = None,
    ) -> None:
        """Register a collection; registering an existing id is a no-op."""
        now = datetime.now(UTC).isoformat()
        conn = self._get_connection()
        with self._lock:
            conn.execute(
                "INSERT OR IGNORE INTO collections "
                "(collection_id, source_kind, location_lat, location_lon, "
                " metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (collection_id, source_kind, lat, lon, metadata_json, now),
            )
            conn.commit()

    def list_collections(self) -> list[dict]:
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute("SELECT * FROM collections ORDER BY collection_id").fetchall()
        return [dict(row) for row in rows]

    def get_collection(self, collection_id: str) -> dict:
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT * FROM collections WHERE collection_id = ?", (collection_id,)
            ).fetchone()
        if row is None:
            raise StoreError(f"Collection '{collection_id}' is not registered")
        return dict(row)

    def ensure_collection_location(self, collection_id: str, *, lat: float, lon: float) -> None:
        """Record the collection location once; an existing location is kept.

        Deterministic: called with the location from pipeline inputs (the first
        gridded dataset), never from external lookup tables.
        """
        conn = self._get_connection()
        with self._lock:
            row = conn.execute(
                "SELECT location_lat, location_lon FROM collections WHERE collection_id = ?",
                (collection_id,),
            ).fetchone()
            if row is None:
                raise StoreError(f"Collection '{collection_id}' is not registered")
            if row["location_lat"] is not None and row["location_lon"] is not None:
                return
            conn.execute(
                "UPDATE collections SET location_lat = ?, location_lon = ? WHERE collection_id = ?",
                (float(lat), float(lon), collection_id),
            )
            conn.commit()

    # -- run lifecycle -------------------------------------------------------

    def begin_run(self, start: RunStart) -> None:
        """Record a new run as ``running``; unknown collection or reused run_id raise."""
        now = datetime.now(UTC).isoformat()
        conn = self._get_connection()
        with self._lock:
            known = conn.execute(
                "SELECT 1 FROM collections WHERE collection_id = ?", (start.collection_id,)
            ).fetchone()
            if known is None:
                raise StoreError(
                    f"Cannot begin run '{start.run_id}': collection "
                    f"'{start.collection_id}' is not registered"
                )
            try:
                conn.execute(
                    "INSERT INTO runs (run_id, collection_id, status, config_hash, "
                    " config_json, pipeline_version, environment_json, started_at) "
                    "VALUES (?, ?, 'running', ?, ?, ?, ?, ?)",
                    (
                        start.run_id,
                        start.collection_id,
                        start.config_hash,
                        start.config_json,
                        start.pipeline_version,
                        start.environment_json,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise StoreError(f"Run '{start.run_id}' already exists") from exc
            conn.commit()

    def resume_run(self, run_id: str) -> None:
        """Re-open an existing run (``--run-id`` continuation): running, no end time."""
        conn = self._get_connection()
        with self._lock:
            updated = conn.execute(
                "UPDATE runs SET status = 'running', ended_at = NULL WHERE run_id = ?",
                (run_id,),
            )
            if updated.rowcount == 0:
                raise StoreError(f"Run '{run_id}' does not exist")
            conn.commit()

    def finalize_run(
        self,
        run_id: str,
        status: str,
        *,
        scans_processed: int | None = None,
        scans_failed: int | None = None,
    ) -> None:
        """Move a running run to a terminal status; double-finalize raises."""
        if status not in _TERMINAL_STATUSES:
            raise ValueError(
                f"'{status}' is not a terminal run status (expected one of "
                f"{', '.join(_TERMINAL_STATUSES)})"
            )
        now = datetime.now(UTC).isoformat()
        conn = self._get_connection()
        with self._lock:
            row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise StoreError(f"Run '{run_id}' does not exist")
            if row["status"] != "running":
                raise StoreError(f"Run '{run_id}' is already finalized (status '{row['status']}')")
            conn.execute(
                "UPDATE runs SET status = ?, ended_at = ?, "
                " scans_processed = COALESCE(?, scans_processed), "
                " scans_failed = COALESCE(?, scans_failed) "
                "WHERE run_id = ?",
                (status, now, scans_processed, scans_failed, run_id),
            )
            conn.commit()

    def record_module_execution(
        self, run_id: str, module: str, *, status: str, duration_seconds: float, detail_json: str
    ) -> None:
        """Upsert one module's execution record (re-runs replace their own row)."""
        conn = self._get_connection()
        with self._lock:
            conn.execute(
                "INSERT INTO run_modules (run_id, module, status, duration_seconds, detail_json) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(run_id, module) DO UPDATE SET status=excluded.status, "
                " duration_seconds=excluded.duration_seconds, detail_json=excluded.detail_json",
                (run_id, module, status, duration_seconds, detail_json),
            )
            conn.commit()

    def run_modules(self, run_id: str) -> list[dict]:
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM run_modules WHERE run_id = ? ORDER BY module", (run_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def record_run_event(
        self, run_id: str, *, severity: str, module: str, message: str, context_json: str
    ) -> None:
        """Append one warning/error event for a run."""
        conn = self._get_connection()
        with self._lock:
            conn.execute(
                "INSERT INTO run_events (run_id, severity, module, message, context_json, "
                " created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, severity, module, message, context_json, datetime.now(UTC).isoformat()),
            )
            conn.commit()

    def run_events(self, run_id: str) -> list[dict]:
        conn = self._get_connection()
        with self._lock:
            rows = conn.execute(
                "SELECT * FROM run_events WHERE run_id = ? ORDER BY event_id", (run_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_run(self, collection_id: str | None = None) -> dict | None:
        """The most recently started run (optionally within one collection)."""
        conn = self._get_connection()
        with self._lock:
            if collection_id is None:
                row = conn.execute(
                    "SELECT * FROM runs ORDER BY started_at DESC, rowid DESC LIMIT 1"
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM runs WHERE collection_id = ? "
                    "ORDER BY started_at DESC, rowid DESC LIMIT 1",
                    (collection_id,),
                ).fetchone()
        return dict(row) if row else None

    def get_run(self, run_id: str) -> dict:
        conn = self._get_connection()
        with self._lock:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            raise StoreError(f"Run '{run_id}' does not exist")
        return dict(row)
