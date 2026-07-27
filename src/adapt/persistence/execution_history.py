# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Execution History — a permanent, queryable scientific metadata product.

Records every run (run_history), each module execution (module_history), and
structured warnings/errors into the radar catalog database, so questions like
"which runs failed?", "which module got slower?", "what config produced these
outputs?" are answered with SQL, never by parsing log files.

Owns its own connection + tables (idempotent ``CREATE TABLE IF NOT EXISTS``),
like FileProcessingTracker. Consumes only ``contracts`` DTOs — never the
observability implementation.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

from adapt.contracts.execution_history import ErrorEvent, RunStart, RunSummary, WarningEvent
from adapt.contracts.observability import SpanRecord

__all__ = ["ExecutionHistory"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS run_history (
    run_id TEXT PRIMARY KEY,
    pipeline TEXT NOT NULL, pipeline_version TEXT, git_commit TEXT,
    hostname TEXT, username TEXT,
    start_time TEXT NOT NULL, end_time TEXT, duration_seconds REAL,
    configuration_hash TEXT, configuration_file TEXT,
    dataset TEXT, site TEXT, instrument TEXT,
    files_processed INTEGER DEFAULT 0, scans_processed INTEGER DEFAULT 0,
    objects_detected INTEGER DEFAULT 0,
    warnings INTEGER DEFAULT 0, errors INTEGER DEFAULT 0,
    status TEXT NOT NULL,
    average_scan_time REAL, maximum_scan_time REAL,
    slowest_stage TEXT, slowest_stage_duration REAL,
    software_version TEXT, python_version TEXT, platform TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS module_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL, scan_id TEXT, module TEXT NOT NULL,
    duration_seconds REAL NOT NULL, status TEXT NOT NULL, error TEXT,
    trace_id TEXT, span_id TEXT, recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS warning_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL, scan_id TEXT, module TEXT,
    category TEXT, message TEXT NOT NULL, logger TEXT, timestamp TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS error_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL, scan_id TEXT, module TEXT,
    exception_type TEXT, message TEXT NOT NULL, traceback TEXT,
    logger TEXT, timestamp TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_module_history_run ON module_history(run_id);
CREATE INDEX IF NOT EXISTS idx_module_history_module ON module_history(module);
CREATE INDEX IF NOT EXISTS idx_error_history_run ON error_history(run_id);
CREATE INDEX IF NOT EXISTS idx_warning_history_run ON warning_history(run_id);
"""


class ExecutionHistory:
    """Writer/reader for the four execution-history tables in the catalog db."""

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ── writes ────────────────────────────────────────────────────────────────
    def start_run(self, start: RunStart) -> None:
        now = datetime.now(UTC).isoformat()
        p = start.provenance
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO run_history (
                    run_id, pipeline, pipeline_version, git_commit, hostname, username,
                    start_time, configuration_hash, configuration_file,
                    dataset, site, instrument, status,
                    software_version, python_version, platform, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    start.run_id,
                    start.pipeline,
                    start.pipeline_version,
                    p.git_commit,
                    p.hostname,
                    p.username,
                    start.start_time.isoformat(),
                    start.configuration_hash,
                    start.configuration_file,
                    start.dataset,
                    start.site,
                    start.instrument,
                    "running",
                    p.software_version,
                    p.python_version,
                    p.platform,
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def record_modules(
        self, run_id: str, scan_id: str, spans: list[SpanRecord], recorded_at: datetime
    ) -> None:
        ts = recorded_at.isoformat()
        rows = [
            (
                run_id,
                scan_id,
                s.name,
                s.duration_s,
                "error" if s.error else "ok",
                s.error or None,
                s.trace_id,
                s.span_id,
                ts,
            )
            for s in spans
        ]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO module_history
                (run_id, scan_id, module, duration_seconds, status, error,
                 trace_id, span_id, recorded_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )
            self._conn.commit()

    def record_warnings(self, run_id: str, events: list[WarningEvent]) -> None:
        rows = [
            (run_id, e.scan_id, e.module, e.category, e.message, e.logger, e.timestamp.isoformat())
            for e in events
        ]
        with self._lock:
            self._conn.executemany(
                "INSERT INTO warning_history "
                "(run_id, scan_id, module, category, message, logger, timestamp) "
                "VALUES (?,?,?,?,?,?,?)",
                rows,
            )
            self._conn.commit()

    def record_errors(self, run_id: str, events: list[ErrorEvent]) -> None:
        rows = [
            (
                run_id,
                e.scan_id,
                e.module,
                e.exception_type,
                e.message,
                e.traceback,
                e.logger,
                e.timestamp.isoformat(),
            )
            for e in events
        ]
        with self._lock:
            self._conn.executemany(
                "INSERT INTO error_history "
                "(run_id, scan_id, module, exception_type, message, traceback, logger, timestamp) "
                "VALUES (?,?,?,?,?,?,?,?)",
                rows,
            )
            self._conn.commit()

    def finalize_run(self, summary: RunSummary) -> None:
        slowest, slowest_dur = summary.slowest_stages[0] if summary.slowest_stages else (None, None)
        with self._lock:
            self._conn.execute(
                """
                UPDATE run_history SET
                    status=?, end_time=?, duration_seconds=?,
                    files_processed=?, scans_processed=?, objects_detected=?,
                    warnings=?, errors=?,
                    average_scan_time=?, maximum_scan_time=?,
                    slowest_stage=?, slowest_stage_duration=?, updated_at=?
                WHERE run_id=?
                """,
                (
                    summary.status,
                    summary.end_time.isoformat(),
                    summary.duration_seconds,
                    summary.files_processed,
                    summary.scans_processed,
                    summary.objects_detected,
                    summary.warnings,
                    summary.errors,
                    summary.average_scan_time,
                    summary.maximum_scan_time,
                    slowest,
                    slowest_dur,
                    datetime.now(UTC).isoformat(),
                    summary.run_id,
                ),
            )
            self._conn.commit()

    def export_run_report(self, run_id: str, path: Path | str) -> None:
        runs = self.query_runs()
        run = next((r for r in runs if r["run_id"] == run_id), None)
        report = {
            "run": run,
            "modules": self.query_modules(run_id=run_id),
            "warnings": self.query_warnings(run_id=run_id),
            "errors": self.query_errors(run_id=run_id),
        }
        Path(path).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    # ── reads ─────────────────────────────────────────────────────────────────
    def query_runs(self, *, site: str | None = None, status: str | None = None) -> list[dict]:
        sql = "SELECT * FROM run_history"
        clauses, params = [], []
        if site is not None:
            clauses.append("site = ?")
            params.append(site)
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY start_time DESC"
        return self._fetch(sql, params)

    def query_modules(self, *, run_id: str | None = None, module: str | None = None) -> list[dict]:
        sql = "SELECT * FROM module_history"
        clauses, params = [], []
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if module is not None:
            clauses.append("module = ?")
            params.append(module)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        return self._fetch(sql, params)

    def query_warnings(self, *, run_id: str | None = None) -> list[dict]:
        return self._fetch_by_run("warning_history", run_id)

    def query_errors(self, *, run_id: str | None = None) -> list[dict]:
        return self._fetch_by_run("error_history", run_id)

    def failure_rate_by_module(self) -> dict[str, float]:
        rows = self._fetch(
            "SELECT module, "
            "AVG(CASE WHEN status = 'error' THEN 1.0 ELSE 0.0 END) AS rate "
            "FROM module_history GROUP BY module",
            [],
        )
        return {r["module"]: r["rate"] for r in rows}

    def close(self) -> None:
        self._conn.close()

    # ── helpers ───────────────────────────────────────────────────────────────
    def _fetch(self, sql: str, params: list) -> list[dict]:
        with self._lock:
            return [dict(row) for row in self._conn.execute(sql, params).fetchall()]

    def _fetch_by_run(self, table: str, run_id: str | None) -> list[dict]:
        if run_id is None:
            return self._fetch(f"SELECT * FROM {table}", [])
        return self._fetch(f"SELECT * FROM {table} WHERE run_id = ?", [run_id])
